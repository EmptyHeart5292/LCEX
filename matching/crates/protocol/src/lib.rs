//! 撮合引擎的输入/输出事件协议定义。
//!
//! 价格与数量一律为 u64 定点整数(缩放 `SCALE` = 1e8),协议层禁止浮点。
//! 本 crate 刻意保持零依赖、纯数据:与 Kafka 之间的 JSON 序列化由
//! runner 层(Phase 0 后期引入)负责,保证撮合核心可在任意环境离线编译。

/// 定点缩放:价格与数量的最小单位为 1e-8
pub const SCALE: u64 = 100_000_000;

/// 定点价格(× 1e8)
pub type Px = u64;
/// 定点数量(× 1e8)
pub type Qty = u64;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OrderType {
    Limit,
    Market,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TimeInForce {
    Gtc,
    Ioc,
    Fok,
}

/// 自成交防范模式。
/// MVP 范围:`None`(允许自成交)与 `CancelTaker`(遇自身挂单撤销 taker,已成交部分保留);
/// `CancelMaker` / `CancelBoth` 后续按需扩展。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum StpMode {
    #[default]
    None,
    CancelTaker,
}

#[derive(Debug, Clone)]
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

#[derive(Debug, Clone)]
pub struct CancelCommand {
    pub order_id: u64,
    pub user_id: u64,
}

#[derive(Debug, Clone)]
pub enum Command {
    Place(PlaceCommand),
    Cancel(CancelCommand),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
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

#[derive(Debug, Clone, PartialEq, Eq)]
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

#[derive(Debug, Clone, PartialEq, Eq)]
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

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Event {
    /// 输出事件全局序号:单交易对内单调递增,从 1 开始,无空洞
    pub seq: u64,
    pub kind: EventKind,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum EventKind {
    Trade(Trade),
    OrderUpdate(OrderUpdate),
}
