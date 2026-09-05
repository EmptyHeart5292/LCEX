//! 每交易对一个异步 task:消费指令 → 引擎 → 生产事件。
//! 引擎状态被 task 独占,满足"单交易对单线程"约束。
//!
//! 恢复语义:
//! - 启动时从指令 log 开头重放,重建内存订单簿;
//! - 已提交 offset 之前的指令只重建、不重复投递事件;
//! - 事件全部产出成功后再提交该条指令 offset(先产出再提交)。

use std::sync::Arc;
use std::time::{Duration, Instant};

use cex_engine::Engine;
use cex_protocol::Event;
use rdkafka::client::DefaultClientContext;
use rdkafka::config::ClientConfig;
use rdkafka::consumer::{BaseConsumer, CommitMode, Consumer};
use rdkafka::message::Message;
use rdkafka::producer::{FutureProducer, FutureRecord};
use rdkafka::topic_partition_list::{Offset, TopicPartitionList};
use rdkafka::util::{Timeout, TokioRuntime};
use tokio::sync::watch;
use tracing::{debug, error, info, warn};

use crate::config::RunnerConfig;
use crate::{codec, routing};

type KafkaProducer = FutureProducer<DefaultClientContext, TokioRuntime>;

enum PollOut {
    Empty,
    Err(String),
    Msg {
        topic: String,
        partition: i32,
        offset: i64,
        payload: Option<Vec<u8>>,
    },
}

/// 已提交的 next offset 之前的指令只用于重建订单簿,不得再次投递事件。
pub fn is_replay(offset: i64, committed_next: i64) -> bool {
    offset < committed_next
}

/// Kafka 未提交 / Invalid 视为 0:从第一条指令开始允许投递。
pub fn committed_next_from_offset(offset: Offset) -> i64 {
    match offset {
        Offset::Offset(o) if o >= 0 => o,
        _ => 0,
    }
}

pub async fn run_symbol(
    cfg: Arc<RunnerConfig>,
    symbol: String,
    mut shutdown: watch::Receiver<bool>,
) {
    let in_topic = routing::input_topic(&cfg.topic_prefix, &symbol);
    let group_id = format!("cex-matching-{}", symbol.to_lowercase());

    let consumer: BaseConsumer = ClientConfig::new()
        .set("bootstrap.servers", &cfg.brokers)
        .set("group.id", &group_id)
        .set("enable.auto.commit", "false")
        .set("enable.auto.offset.store", "false")
        .set("auto.offset.reset", "earliest")
        .create()
        .expect("consumer config is valid");
    consumer
        .subscribe(&[in_topic.as_str()])
        .expect("subscribe topic");

    wait_assignment(&consumer);
    let committed_next = read_committed_next(&consumer, &in_topic);
    seek_beginning(&consumer);
    info!(
        symbol = %symbol,
        topic = %in_topic,
        committed_next,
        "matching worker started (replay from 0, publish from committed_next)"
    );

    let producer: KafkaProducer = ClientConfig::new()
        .set("bootstrap.servers", &cfg.brokers)
        .create()
        .expect("producer config is valid");

    let consumer = Arc::new(consumer);
    let mut engine = Engine::new();

    loop {
        tokio::select! {
            _ = shutdown.changed() => {
                info!(symbol = %symbol, resting_orders = engine.resting_orders(), last_seq = engine.last_seq(), "worker shutting down");
                break;
            }
            polled = poll_once(Arc::clone(&consumer)) => {
                match polled {
                    PollOut::Empty => {}
                    PollOut::Err(e) => {
                        warn!(symbol = %symbol, error = %e, "consume error, retrying");
                        tokio::time::sleep(Duration::from_millis(100)).await;
                    }
                    PollOut::Msg { topic, partition, offset, payload } => {
                        handle_command(
                            &consumer,
                            &producer,
                            &mut engine,
                            &cfg,
                            &symbol,
                            committed_next,
                            topic,
                            partition,
                            offset,
                            payload,
                        )
                        .await;
                    }
                }
            }
        }
    }
}

fn wait_assignment(consumer: &BaseConsumer) {
    let deadline = Instant::now() + Duration::from_secs(15);
    while Instant::now() < deadline {
        let _ = consumer.poll(Duration::from_millis(100));
        if let Ok(tpl) = consumer.assignment() {
            if tpl.count() > 0 {
                return;
            }
        }
    }
    panic!("timeout waiting for partition assignment");
}

fn read_committed_next(consumer: &BaseConsumer, topic: &str) -> i64 {
    match consumer.committed(Duration::from_secs(5)) {
        Ok(tpl) => {
            for e in tpl.elements() {
                if e.topic() == topic {
                    return committed_next_from_offset(e.offset());
                }
            }
            0
        }
        Err(e) => {
            warn!(error = %e, "committed offsets unavailable, publishing from 0");
            0
        }
    }
}

fn seek_beginning(consumer: &BaseConsumer) {
    let parts: Vec<(String, i32)> = match consumer.assignment() {
        Ok(tpl) => tpl
            .elements()
            .iter()
            .map(|e| (e.topic().to_owned(), e.partition()))
            .collect(),
        Err(_) => return,
    };
    for (topic, partition) in parts {
        if let Err(e) = consumer.seek(&topic, partition, Offset::Beginning, Duration::from_secs(5))
        {
            warn!(topic = %topic, partition, error = %e, "seek to beginning failed");
        }
    }
}

async fn poll_once(consumer: Arc<BaseConsumer>) -> PollOut {
    tokio::task::spawn_blocking(move || match consumer.poll(Duration::from_millis(200)) {
        None => PollOut::Empty,
        Some(Err(e)) => PollOut::Err(e.to_string()),
        Some(Ok(m)) => PollOut::Msg {
            topic: m.topic().to_owned(),
            partition: m.partition(),
            offset: m.offset(),
            payload: m.payload().map(|p| p.to_vec()),
        },
    })
    .await
    .unwrap_or_else(|e| PollOut::Err(e.to_string()))
}

async fn handle_command(
    consumer: &BaseConsumer,
    producer: &KafkaProducer,
    engine: &mut Engine,
    cfg: &RunnerConfig,
    symbol: &str,
    committed_next: i64,
    topic: String,
    partition: i32,
    offset: i64,
    payload: Option<Vec<u8>>,
) {
    let replay = is_replay(offset, committed_next);
    let Some(payload) = payload else {
        warn!(symbol = %symbol, offset, "empty payload skipped");
        if !replay {
            commit_offset(consumer, &topic, partition, offset);
        }
        return;
    };
    match codec::decode_command(&payload) {
        Ok(cmd) => {
            let events = engine.process(cmd);
            if replay {
                debug!(symbol = %symbol, offset, events = events.len(), "replayed command, not publishing");
                return;
            }
            publish_all(producer, cfg, symbol, &events).await;
            commit_offset(consumer, &topic, partition, offset);
            debug!(symbol = %symbol, offset, events = events.len(), resting = engine.resting_orders(), "processed");
        }
        Err(e) => {
            error!(symbol = %symbol, offset, error = %e, "malformed command skipped");
            if !replay {
                commit_offset(consumer, &topic, partition, offset);
            }
        }
    }
}

async fn publish_all(producer: &KafkaProducer, cfg: &RunnerConfig, symbol: &str, events: &[Event]) {
    for (topic, out) in routing::route_events(&cfg.topic_prefix, symbol, events) {
        loop {
            match producer
                .send(
                    FutureRecord::to(&topic).payload(out.as_str()).key(symbol),
                    Timeout::Never,
                )
                .await
            {
                Ok(_) => break,
                Err((e, _)) => {
                    error!(symbol = %symbol, topic = %topic, error = %e, "produce failed, retrying");
                    tokio::time::sleep(Duration::from_millis(200)).await;
                }
            }
        }
    }
}

fn commit_offset(consumer: &BaseConsumer, topic: &str, partition: i32, offset: i64) {
    let mut tpl = TopicPartitionList::new();
    if let Err(e) = tpl.add_partition_offset(topic, partition, Offset::Offset(offset + 1)) {
        error!(error = %e, "commit tpl invalid");
        return;
    }
    if let Err(e) = consumer.commit(&tpl, CommitMode::Sync) {
        error!(topic, partition, offset, error = %e, "commit failed");
    }
}
