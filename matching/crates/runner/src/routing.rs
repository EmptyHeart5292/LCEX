//! 事件路由:撮合输出进入每交易对单一 topic,保证下游看到的事件顺序
//! 与引擎输出完全一致(成交先于终态 order_update)——
//! 这是清算正确性(先结算后解冻)的前提,禁止拆分多 topic。

use cex_protocol::Event;

use crate::codec;

pub fn input_topic(prefix: &str, symbol: &str) -> String {
    format!("{prefix}.orders.in.{}", symbol.to_lowercase())
}

pub fn events_topic(prefix: &str, symbol: &str) -> String {
    format!("{prefix}.events.{}", symbol.to_lowercase())
}

/// 事件 → (topic, payload);全部进单一 events topic,kind 由消费端过滤
pub fn route_events(prefix: &str, symbol: &str, events: &[Event]) -> Vec<(String, String)> {
    let topic = events_topic(prefix, symbol);
    events
        .iter()
        .map(|ev| (topic.clone(), codec::encode_event(ev)))
        .collect()
}
