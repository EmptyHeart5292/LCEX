//! 事件路由:按事件类型决定输出 topic。
//!
//! - `trade` → `cex.trades.{symbol}`(下游:清算、行情、推送)
//! - `order_update` → `cex.order-events.{symbol}`(下游:订单服务、行情、推送)

use cex_protocol::{Event, EventKind};

use crate::codec;

pub fn input_topic(prefix: &str, symbol: &str) -> String {
    format!("{prefix}.orders.in.{}", symbol.to_lowercase())
}

pub fn trades_topic(prefix: &str, symbol: &str) -> String {
    format!("{prefix}.trades.{}", symbol.to_lowercase())
}

pub fn order_events_topic(prefix: &str, symbol: &str) -> String {
    format!("{prefix}.order-events.{}", symbol.to_lowercase())
}

pub fn route_events(prefix: &str, symbol: &str, events: &[Event]) -> Vec<(String, String)> {
    events
        .iter()
        .map(|ev| {
            let topic = match ev.kind {
                EventKind::Trade(_) => trades_topic(prefix, symbol),
                EventKind::OrderUpdate(_) => order_events_topic(prefix, symbol),
            };
            (topic, codec::encode_event(ev))
        })
        .collect()
}
