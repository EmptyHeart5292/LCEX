//! 每交易对一个异步 task:消费指令 → 引擎 → 生产事件。
//! 引擎状态被 task 独占,满足"单交易对单线程"约束。

use std::sync::Arc;

use cex_engine::Engine;
use rdkafka::client::DefaultClientContext;
use rdkafka::config::ClientConfig;
use rdkafka::consumer::{Consumer, DefaultConsumerContext, StreamConsumer};
use rdkafka::message::Message;
use rdkafka::producer::{FutureProducer, FutureRecord};
use rdkafka::util::{Timeout, TokioRuntime};
use tokio::sync::watch;
use tracing::{debug, error, info, warn};

use crate::config::RunnerConfig;
use crate::{codec, routing};

/// rdkafka 0.29 的运行时泛型默认为 `()`,必须显式绑定 tokio
type KafkaConsumer = StreamConsumer<DefaultConsumerContext, TokioRuntime>;
type KafkaProducer = FutureProducer<DefaultClientContext, TokioRuntime>;

pub async fn run_symbol(
    cfg: Arc<RunnerConfig>,
    symbol: String,
    mut shutdown: watch::Receiver<bool>,
) {
    let in_topic = routing::input_topic(&cfg.topic_prefix, &symbol);

    // 单分区 topic + 固定 group:runner 实例间按 symbol 静态分片
    let consumer: KafkaConsumer = ClientConfig::new()
        .set("bootstrap.servers", &cfg.brokers)
        .set("group.id", format!("cex-matching-{}", symbol.to_lowercase()))
        .set("enable.auto.commit", "true")
        .set("auto.commit.interval.ms", "200")
        .set("auto.offset.reset", "earliest")
        .create()
        .expect("consumer config is valid");
    consumer
        .subscribe(&[in_topic.as_str()])
        .expect("subscribe topic");

    let producer: KafkaProducer = ClientConfig::new()
        .set("bootstrap.servers", &cfg.brokers)
        .create()
        .expect("producer config is valid");

    let mut engine = Engine::new();
    info!(symbol = %symbol, topic = %in_topic, "matching worker started");

    loop {
        tokio::select! {
            _ = shutdown.changed() => {
                info!(symbol = %symbol, resting_orders = engine.resting_orders(), last_seq = engine.last_seq(), "worker shutting down");
                break;
            }
            msg = consumer.recv() => {
                match msg {
                    Ok(m) => {
                        let Some(payload) = m.payload() else {
                            warn!(symbol = %symbol, "empty payload skipped");
                            continue;
                        };
                        match codec::decode_command(payload) {
                            Ok(cmd) => {
                                let events = engine.process(cmd);
                                for (topic, out) in routing::route_events(&cfg.topic_prefix, &symbol, &events) {
                                    if let Err((e, _)) = producer
                                        .send(
                                            FutureRecord::to(&topic).payload(out.as_str()).key(symbol.as_str()),
                                            Timeout::Never,
                                        )
                                        .await
                                    {
                                        error!(symbol = %symbol, topic = %topic, error = %e, "produce failed");
                                    }
                                }
                                debug!(symbol = %symbol, events = events.len(), resting = engine.resting_orders(), "processed");
                            }
                            Err(e) => {
                                // 毒消息:跳过并继续(订单服务以受理为准,不阻塞整个交易对)
                                error!(symbol = %symbol, error = %e, "malformed command skipped");
                            }
                        }
                    }
                    Err(e) => {
                        warn!(symbol = %symbol, error = %e, "consume error, retrying");
                        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
                    }
                }
            }
        }
    }
}
