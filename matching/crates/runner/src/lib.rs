//! 撮合 runner:把 [`cex_engine::Engine`] 接进 Kafka 事件总线。
//!
//! 拓扑(matching/README.md):
//! - 输入:`cex.orders.in.{symbol}`(每交易对单分区,runner 内单 task 消费,与引擎单线程约束一致)
//! - 输出:成交 → `cex.trades.{symbol}`,订单状态 → `cex.order-events.{symbol}`
//! - 消息格式见 `cex_protocol`(JSON,判别字段在外层)
//!
//! 语义:at-least-once + 下游幂等(成交按 trade_id、订单事件按 order_id+seq 去重)。

pub mod codec;
pub mod config;
pub mod routing;
pub mod worker;

pub use config::RunnerConfig;

use cex_engine::Engine;
use cex_protocol::Command;

/// 纯处理管线(无 Kafka 依赖,单测直接覆盖):
/// 指令 → 引擎 → 事件 → (topic, JSON payload) 路由结果。
pub fn process_command(
    engine: &mut Engine,
    cmd: Command,
    prefix: &str,
    symbol: &str,
) -> Vec<(String, String)> {
    let events = engine.process(cmd);
    routing::route_events(prefix, symbol, &events)
}

#[cfg(test)]
mod tests {
    use super::*;
    use cex_engine::Engine;
    use cex_protocol::{
        Event, EventKind, OrderType, PlaceCommand, Px, Qty, Side, TimeInForce, SCALE,
    };
    use config::parse_symbols;

    fn place(id: u64, user: u64, side: Side, price: Px, qty: Qty) -> Command {
        Command::Place(PlaceCommand {
            order_id: id,
            user_id: user,
            side,
            order_type: OrderType::Limit,
            tif: TimeInForce::Gtc,
            stp: Default::default(),
            post_only: false,
            price: Some(price),
            qty,
        })
    }

    #[test]
    fn symbols_parsing_trims_and_skips_empty() {
        assert_eq!(
            parse_symbols(" BTC-USDT, eth-usdt ,,SOL-USDT"),
            vec!["BTC-USDT", "eth-usdt", "SOL-USDT"]
        );
        assert!(parse_symbols(" , ").is_empty());
    }

    #[test]
    fn topic_naming_is_lowercased() {
        assert_eq!(routing::input_topic("cex", "BTC-USDT"), "cex.orders.in.btc-usdt");
        assert_eq!(routing::events_topic("cex", "BTC-USDT"), "cex.events.btc-usdt");
    }

    #[test]
    fn command_decode_round_trip_and_garbage_rejected() {
        let cmd = place(1, 7, Side::Bid, 100 * SCALE, SCALE);
        let bytes = serde_json::to_vec(&cmd).expect("serialize");
        assert_eq!(codec::decode_command(&bytes).expect("decode"), cmd);
        assert!(codec::decode_command(b"not json").is_err());
        assert!(codec::decode_command(b"").is_err());
    }

    #[test]
    fn engine_events_route_to_correct_topics() {
        let mut engine = Engine::new();

        // 挂单:只有 order_update
        let routes = process_command(&mut engine, place(1, 7, Side::Ask, 100 * SCALE, SCALE), "cex", "BTC-USDT");
        assert_eq!(routes.len(), 1);

        // 吃单:1 条 trade + maker/taker 两条 order_update
        let routes = process_command(&mut engine, place(2, 8, Side::Bid, 100 * SCALE, SCALE), "cex", "BTC-USDT");
        assert_eq!(routes.len(), 3);
        let mut last_seq = 1; // 第一条指令产生了 seq 1
        let mut trade_count = 0;
        for (topic, payload) in &routes {
            let ev: Event = serde_json::from_str(payload).expect("payload is valid event JSON");
            last_seq += 1;
            assert_eq!(ev.seq, last_seq, "seq 跨指令连续");
            assert_eq!(topic, "cex.events.btc-usdt", "单一 events topic");
            match &ev.kind {
                EventKind::Trade(_) => trade_count += 1,
                EventKind::OrderUpdate(_) => {}
            }
        }
        assert_eq!(trade_count, 1);
        assert_eq!(engine.last_seq(), 4, "1(挂单)+ 3(吃单)个事件");
    }
}
