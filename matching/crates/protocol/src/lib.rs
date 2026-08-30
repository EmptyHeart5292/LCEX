//! 撮合引擎的输入/输出事件协议定义。
//!
//! 价格与数量一律为 u64 定点整数(缩放 `SCALE` = 1e8),协议层禁止浮点。
//! serde JSON 编码与 Kafka 消息格式一一对应,判别字段固定在外层:
//! - 输入信封 `Command`:`{"type":"place","order_id":...}` / `{"type":"cancel","order_id":...}`
//! - 输出事件 `Event`:`{"seq":N,"kind":"trade","data":{...}}` / `{"seq":N,"kind":"order_update","data":{...}}`

use serde::{Deserialize, Serialize};

/// 定点缩放:价格与数量的最小单位为 1e-8
pub const SCALE: u64 = 100_000_000;

/// 定点价格(× 1e8)
pub type Px = u64;
/// 定点数量(× 1e8)
pub type Qty = u64;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Side {
    Bid,
    Ask,
}

impl Side {
    pub fn opposite(self) -> Side {
        match self {
            Side::Bid => Side::Ask,
            Side::Ask => Side::Bid,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum OrderType {
    Limit,
    Market,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TimeInForce {
    Gtc,
    Ioc,
    Fok,
}

/// 自成交防范模式。
/// MVP 范围:`None`(允许自成交)与 `CancelTaker`(遇自身挂单撤销 taker,已成交部分保留);
/// `CancelMaker` / `CancelBoth` 后续按需扩展。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum StpMode {
    #[default]
    None,
    CancelTaker,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PlaceCommand {
    /// 交易所侧全局唯一订单 ID(由 order 服务分配)
    pub order_id: u64,
    pub user_id: u64,
    pub side: Side,
    pub order_type: OrderType,
    pub tif: TimeInForce,
    pub stp: StpMode,
    /// Post-Only:只要会吃单即拒绝(限价单专用)
    pub post_only: bool,
    /// 市价单必须为 None
    pub price: Option<Px>,
    pub qty: Qty,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CancelCommand {
    pub order_id: u64,
    pub user_id: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum Command {
    Place(PlaceCommand),
    Cancel(CancelCommand),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum OrderStatus {
    /// 已挂入订单簿
    Open,
    /// 部分成交且剩余量仍挂簿
    PartiallyFilled,
    /// 全部成交
    Filled,
    /// 剩余量撤销(含部分成交后撤销;filled_qty 区分)
    Canceled,
    /// 未进入撮合即被拒绝(参数非法 / Post-Only 越价 / FOK 流动性不足)
    Rejected,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Trade {
    pub trade_id: u64,
    pub maker_order_id: u64,
    pub taker_order_id: u64,
    pub maker_user_id: u64,
    pub taker_user_id: u64,
    /// Taker 方向
    pub side: Side,
    pub price: Px,
    pub qty: Qty,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct OrderUpdate {
    pub order_id: u64,
    pub user_id: u64,
    pub side: Side,
    /// 市价单为 None
    pub price: Option<Px>,
    pub status: OrderStatus,
    pub filled_qty: Qty,
    pub qty: Qty,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Event {
    /// 输出事件全局序号:单交易对内单调递增,从 1 开始,无空洞
    pub seq: u64,
    /// flatten:JSON 信封为 {"seq":N,"kind":"...","data":{...}}
    #[serde(flatten)]
    pub kind: EventKind,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", content = "data", rename_all = "snake_case")]
pub enum EventKind {
    Trade(Trade),
    OrderUpdate(OrderUpdate),
}

#[cfg(test)]
mod tests {
    use super::*;
    use OrderType::*;

    fn place() -> PlaceCommand {
        PlaceCommand {
            order_id: 42,
            user_id: 7,
            side: Side::Bid,
            order_type: Limit,
            tif: TimeInForce::Fok,
            stp: StpMode::CancelTaker,
            post_only: false,
            price: Some(50000 * SCALE),
            qty: 123456789,
        }
    }

    #[test]
    fn command_json_round_trip() {
        let cmds = vec![
            Command::Place(place()),
            Command::Cancel(CancelCommand {
                order_id: 42,
                user_id: 7,
            }),
            Command::Place(PlaceCommand {
                order_type: Market,
                price: None,
                ..place()
            }),
        ];
        for cmd in cmds {
            let json = serde_json::to_string(&cmd).expect("serialize");
            let back: Command = serde_json::from_str(&json).expect("deserialize");
            assert_eq!(back, cmd, "round trip failed for {json}");
        }
    }

    #[test]
    fn event_json_round_trip() {
        let events = vec![
            Event {
                seq: 1,
                kind: EventKind::Trade(Trade {
                    trade_id: 9,
                    maker_order_id: 1,
                    taker_order_id: 2,
                    maker_user_id: 5,
                    taker_user_id: 6,
                    side: Side::Ask,
                    price: SCALE,
                    qty: SCALE,
                }),
            },
            Event {
                seq: 2,
                kind: EventKind::OrderUpdate(OrderUpdate {
                    order_id: 1,
                    user_id: 5,
                    side: Side::Bid,
                    price: Some(SCALE),
                    status: OrderStatus::PartiallyFilled,
                    filled_qty: SCALE,
                    qty: 2 * SCALE,
                }),
            },
        ];
        for ev in events {
            let json = serde_json::to_string(&ev).expect("serialize");
            let back: Event = serde_json::from_str(&json).expect("deserialize");
            assert_eq!(back, ev, "round trip failed for {json}");
        }
    }

    #[test]
    fn event_json_shape_is_stable() {
        let ev = Event {
            seq: 3,
            kind: EventKind::OrderUpdate(OrderUpdate {
                order_id: 1,
                user_id: 5,
                side: Side::Ask,
                price: None,
                status: OrderStatus::Rejected,
                filled_qty: 0,
                qty: SCALE,
            }),
        };
        let json = serde_json::to_string(&ev).expect("serialize");
        assert_eq!(
            json,
            r#"{"seq":3,"kind":"order_update","data":{"order_id":1,"user_id":5,"side":"ask","price":null,"status":"rejected","filled_qty":0,"qty":100000000}}"#
        );
    }
}
