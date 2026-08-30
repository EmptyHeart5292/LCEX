//! 消息编解码:Command/Event 与 Kafka payload(JSON)互转。

use cex_protocol::{Command, Event};

pub fn decode_command(payload: &[u8]) -> Result<Command, serde_json::Error> {
    serde_json::from_slice(payload)
}

/// 这些类型不含浮点与非字符串键 map,序列化不会失败
pub fn encode_event(event: &Event) -> String {
    serde_json::to_string(event).expect("event serialization is infallible")
}
