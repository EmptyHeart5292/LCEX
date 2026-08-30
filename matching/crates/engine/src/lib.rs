//! 撮合核心:价格-时间优先订单簿与确定性撮合循环。
//!
//! 约束(matching/README.md):每个交易对一个单线程实例,订单簿全内存,
//! 无跨线程共享锁;输入输出皆为 [`cex_protocol`] 事件,可从事件日志确定性重放。
//!
//! 订单类型:限价(GTC/IOC/FOK/Post-Only)、市价(剩余量撤销,天然 IOC)。
//! 自成交防范:MVP 支持 None / CancelTaker。

use std::collections::{BTreeMap, HashMap, VecDeque};

use cex_protocol::{
    CancelCommand, Command, Event, EventKind, OrderStatus, OrderType, OrderUpdate, PlaceCommand,
    Px, Qty, Side, StpMode, TimeInForce, Trade,
};

#[derive(Debug, Clone)]
struct Order {
    id: u64,
    user_id: u64,
    price: Px,
    qty: Qty,
    remaining: Qty,
}

#[derive(Debug, Default)]
struct Level {
    orders: VecDeque<Order>,
}

/// 一个交易对一个引擎实例;单线程运行,状态不跨线程共享。
#[derive(Debug, Default)]
pub struct Engine {
    bids: BTreeMap<Px, Level>,
    asks: BTreeMap<Px, Level>,
    /// order_id -> (side, price):撤单与统计的定位索引
    index: HashMap<u64, (Side, Px)>,
    next_trade_id: u64,
    last_seq: u64,
}

impl Engine {
    pub fn new() -> Self {
        Self::default()
    }

    /// 处理一条指令,返回本次产生的全部事件(seq 全局单调)。
    pub fn process(&mut self, cmd: Command) -> Vec<Event> {
        let mut out = Vec::new();
        match cmd {
            Command::Place(place) => self.place(place, &mut out),
            Command::Cancel(cancel) => self.cancel(cancel, &mut out),
        }
        out
    }

    pub fn best_bid(&self) -> Option<Px> {
        self.bids.keys().next_back().copied()
    }

    pub fn best_ask(&self) -> Option<Px> {
        self.asks.keys().next().copied()
    }

    /// 当前挂簿订单数
    pub fn resting_orders(&self) -> usize {
        self.index.len()
    }

    /// 最后一个输出事件的 seq(重放/checkpoint 用)
    pub fn last_seq(&self) -> u64 {
        self.last_seq
    }

    // ---------- 撮合 ----------

    fn place(&mut self, p: PlaceCommand, out: &mut Vec<Event>) {
        if p.qty == 0 {
            return self.reject(&p, out);
        }
        let limit_price = match p.order_type {
            OrderType::Limit => match p.price {
                Some(px) if px > 0 => px,
                _ => return self.reject(&p, out),
            },
            // 市价单不吃价格字段;吃完对手盘后剩余量撤销(天然 IOC)
            OrderType::Market => {
                if p.price.is_some() || p.post_only {
                    return self.reject(&p, out);
                }
                match p.side {
                    Side::Bid => Px::MAX,
                    Side::Ask => 0,
                }
            }
        };

        // Post-Only:只要会吃单即拒绝
        if p.post_only && self.would_cross(p.side, limit_price) {
            return self.reject(&p, out);
        }
        // FOK 预检:按 STP 语义计算可成交量,不足则整单拒绝
        if p.tif == TimeInForce::Fok
            && self.fillable_qty(p.side, limit_price, p.user_id, p.stp) < p.qty
        {
            return self.reject(&p, out);
        }

        let mut filled: Qty = 0;
        let mut remaining: Qty = p.qty;
        let mut stopped_by_stp = false;

        while remaining > 0 {
            // 借读阶段:对手盘最优价与队首信息
            let (maker_price, head_user, head_remaining) = match self.best_of(p.side) {
                None => break,
                Some((px, level)) => {
                    let head = level.orders.front().expect("stored levels are non-empty");
                    let crosses = match p.side {
                        Side::Bid => px <= limit_price,
                        Side::Ask => px >= limit_price,
                    };
                    if !crosses {
                        break;
                    }
                    (px, head.user_id, head.remaining)
                }
            };

            if p.stp == StpMode::CancelTaker && head_user == p.user_id {
                stopped_by_stp = true;
                break;
            }

            let q = remaining.min(head_remaining);

            // 可变阶段:成交并推进队首
            let (maker_id, maker_user, maker_total, maker_remaining) = {
                let book = match p.side {
                    Side::Bid => &mut self.asks,
                    Side::Ask => &mut self.bids,
                };
                let level = book.get_mut(&maker_price).expect("level existed at peek");
                let maker = level.orders.front_mut().expect("non-empty level");
                maker.remaining -= q;
                let done = maker.remaining == 0;
                let r = (maker.id, maker.user_id, maker.qty, maker.remaining);
                if done {
                    level.orders.pop_front();
                }
                r
            };
            if maker_remaining == 0 {
                let book = match p.side {
                    Side::Bid => &mut self.asks,
                    Side::Ask => &mut self.bids,
                };
                let empty = book
                    .get(&maker_price)
                    .expect("level existed")
                    .orders
                    .is_empty();
                if empty {
                    book.remove(&maker_price);
                }
                self.index.remove(&maker_id);
            }

            self.next_trade_id += 1;
            let trade_id = self.next_trade_id;
            out.push(Event {
                seq: self.next_seq(),
                kind: EventKind::Trade(Trade {
                    trade_id,
                    maker_order_id: maker_id,
                    taker_order_id: p.order_id,
                    maker_user_id: maker_user,
                    taker_user_id: p.user_id,
                    side: p.side,
                    price: maker_price,
                    qty: q,
                }),
            });
            out.push(Event {
                seq: self.next_seq(),
                kind: EventKind::OrderUpdate(OrderUpdate {
                    order_id: maker_id,
                    user_id: maker_user,
                    side: p.side.opposite(),
                    price: Some(maker_price),
                    status: if maker_remaining == 0 {
                        OrderStatus::Filled
                    } else {
                        OrderStatus::PartiallyFilled
                    },
                    filled_qty: maker_total - maker_remaining,
                    qty: maker_total,
                }),
            });

            remaining -= q;
            filled += q;
        }

        // Taker 最终状态
        let status = if stopped_by_stp {
            // STP:撤销 taker,已成交部分保留
            OrderStatus::Canceled
        } else if remaining == 0 {
            OrderStatus::Filled
        } else if p.order_type == OrderType::Limit && p.tif == TimeInForce::Gtc {
            let order = Order {
                id: p.order_id,
                user_id: p.user_id,
                price: limit_price,
                qty: p.qty,
                remaining,
            };
            let book = match p.side {
                Side::Bid => &mut self.bids,
                Side::Ask => &mut self.asks,
            };
            book.entry(limit_price).or_default().orders.push_back(order);
            self.index.insert(p.order_id, (p.side, limit_price));
            if filled > 0 {
                OrderStatus::PartiallyFilled
            } else {
                OrderStatus::Open
            }
        } else {
            // IOC / FOK(防御,预检已保证)/ 市价剩余量 → 撤销
            OrderStatus::Canceled
        };

        out.push(Event {
            seq: self.next_seq(),
            kind: EventKind::OrderUpdate(OrderUpdate {
                order_id: p.order_id,
                user_id: p.user_id,
                side: p.side,
                price: match p.order_type {
                    OrderType::Limit => Some(limit_price),
                    OrderType::Market => None,
                },
                status,
                filled_qty: filled,
                qty: p.qty,
            }),
        });
    }

    fn cancel(&mut self, c: CancelCommand, out: &mut Vec<Event>) {
        let Some((side, price)) = self.index.get(&c.order_id).copied() else {
            return; // 未知订单:静默忽略,order 服务负责状态校验
        };
        let order = {
            let book = match side {
                Side::Bid => &mut self.bids,
                Side::Ask => &mut self.asks,
            };
            let level = book.get_mut(&price).expect("indexed level exists");
            let pos = level
                .orders
                .iter()
                .position(|o| o.id == c.order_id)
                .expect("indexed order exists");
            let order = level.orders.remove(pos).expect("position valid");
            if level.orders.is_empty() {
                book.remove(&price);
            }
            order
        };
        self.index.remove(&c.order_id);
        out.push(Event {
            seq: self.next_seq(),
            kind: EventKind::OrderUpdate(OrderUpdate {
                order_id: order.id,
                user_id: order.user_id,
                side,
                price: Some(order.price),
                status: OrderStatus::Canceled,
                filled_qty: order.qty - order.remaining,
                qty: order.qty,
            }),
        });
    }

    fn reject(&mut self, p: &PlaceCommand, out: &mut Vec<Event>) {
        out.push(Event {
            seq: self.next_seq(),
            kind: EventKind::OrderUpdate(OrderUpdate {
                order_id: p.order_id,
                user_id: p.user_id,
                side: p.side,
                price: p.price,
                status: OrderStatus::Rejected,
                filled_qty: 0,
                qty: p.qty,
            }),
        });
    }

    // ---------- 盘口工具 ----------

    /// 对手盘(taker 视角)最优价与对应价位;Bid → asks 升序首项,Ask → bids 降序末项
    fn best_of(&self, taker_side: Side) -> Option<(Px, &Level)> {
        match taker_side {
            Side::Bid => self.asks.iter().next().map(|(px, l)| (*px, l)),
            Side::Ask => self.bids.iter().next_back().map(|(px, l)| (*px, l)),
        }
    }

    fn would_cross(&self, taker_side: Side, limit_price: Px) -> bool {
        match self.best_of(taker_side) {
            None => false,
            Some((px, _)) => match taker_side {
                Side::Bid => px <= limit_price,
                Side::Ask => px >= limit_price,
            },
        }
    }

    /// 按 STP 语义计算 taker 可成交的对手盘总量(FOK 预检):
    /// CancelTaker 模式下遇到自身挂单即止——触发 STP 会让 taker 撤销,后续流动性无意义。
    fn fillable_qty(&self, taker_side: Side, limit_price: Px, taker_user: u64, stp: StpMode) -> Qty {
        let mut total: Qty = 0;
        let iter: Box<dyn Iterator<Item = (&Px, &Level)>> = match taker_side {
            // taker 买:吃掉价格 ≤ limit 的卖单,按价格从优到劣(升序)
            Side::Bid => Box::new(self.asks.range(..=limit_price)),
            // taker 卖:吃掉价格 ≥ limit 的买单,按价格从优到劣(降序)
            Side::Ask => Box::new(self.bids.range(limit_price..).rev()),
        };
        for (_px, level) in iter {
            for o in &level.orders {
                if stp == StpMode::CancelTaker && o.user_id == taker_user {
                    return total;
                }
                total += o.remaining;
            }
        }
        total
    }

    fn next_seq(&mut self) -> u64 {
        self.last_seq += 1;
        self.last_seq
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use cex_protocol::{EventKind, OrderStatus, Px, Qty, SCALE};

    const U1: u64 = 1;
    const U2: u64 = 2;

    fn limit(id: u64, user: u64, side: Side, price: Px, qty: Qty) -> Command {
        Command::Place(PlaceCommand {
            order_id: id,
            user_id: user,
            side,
            order_type: OrderType::Limit,
            tif: TimeInForce::Gtc,
            stp: StpMode::None,
            post_only: false,
            price: Some(price),
            qty,
        })
    }

    fn ioc(id: u64, user: u64, side: Side, price: Px, qty: Qty) -> Command {
        match limit(id, user, side, price, qty) {
            Command::Place(mut p) => {
                p.tif = TimeInForce::Ioc;
                Command::Place(p)
            }
            _ => unreachable!(),
        }
    }

    fn fok(id: u64, user: u64, side: Side, price: Px, qty: Qty) -> Command {
        match limit(id, user, side, price, qty) {
            Command::Place(mut p) => {
                p.tif = TimeInForce::Fok;
                Command::Place(p)
            }
            _ => unreachable!(),
        }
    }

    fn post_only(id: u64, user: u64, side: Side, price: Px, qty: Qty) -> Command {
        match limit(id, user, side, price, qty) {
            Command::Place(mut p) => {
                p.post_only = true;
                Command::Place(p)
            }
            _ => unreachable!(),
        }
    }

    fn market(id: u64, user: u64, side: Side, qty: Qty) -> Command {
        Command::Place(PlaceCommand {
            order_id: id,
            user_id: user,
            side,
            order_type: OrderType::Market,
            tif: TimeInForce::Gtc,
            stp: StpMode::None,
            post_only: false,
            price: None,
            qty,
        })
    }

    fn cancel(id: u64, user: u64) -> Command {
        Command::Cancel(CancelCommand {
            order_id: id,
            user_id: user,
        })
    }

    /// 取出最后一个 taker 订单状态更新
    fn last_taker_update(events: &[Event]) -> &OrderUpdate {
        events
            .iter()
            .rev()
            .find_map(|e| match &e.kind {
                EventKind::OrderUpdate(u) => Some(u),
                _ => None,
            })
            .expect("taker update exists")
    }

    fn trades(events: &[Event]) -> Vec<Trade> {
        events
            .iter()
            .filter_map(|e| match &e.kind {
                EventKind::Trade(t) => Some(t.clone()),
                _ => None,
            })
            .collect()
    }

    fn updates(events: &[Event]) -> Vec<OrderUpdate> {
        events
            .iter()
            .filter_map(|e| match &e.kind {
                EventKind::OrderUpdate(u) => Some(u.clone()),
                _ => None,
            })
            .collect()
    }

    #[test]
    fn resting_orders_set_bbo() {
        let mut e = Engine::new();
        let ev1 = e.process(limit(1, U1, Side::Bid, 100 * SCALE, SCALE));
        let ev2 = e.process(limit(2, U2, Side::Ask, 101 * SCALE, SCALE));
        assert_eq!(last_taker_update(&ev1).status, OrderStatus::Open);
        assert_eq!(last_taker_update(&ev2).status, OrderStatus::Open);
        assert_eq!(e.best_bid(), Some(100 * SCALE));
        assert_eq!(e.best_ask(), Some(101 * SCALE));
        assert_eq!(e.resting_orders(), 2);
        assert!(trades(&ev1).is_empty() && trades(&ev2).is_empty());
    }

    #[test]
    fn basic_cross_at_same_price() {
        let mut e = Engine::new();
        e.process(limit(1, U1, Side::Ask, 100 * SCALE, SCALE));
        let ev = e.process(limit(2, U2, Side::Bid, 100 * SCALE, SCALE));
        let ts = trades(&ev);
        assert_eq!(ts.len(), 1);
        assert_eq!(ts[0].price, 100 * SCALE);
        assert_eq!(ts[0].qty, SCALE);
        assert_eq!(ts[0].maker_order_id, 1);
        assert_eq!(ts[0].taker_order_id, 2);
        assert_eq!(ts[0].side, Side::Bid);
        assert_eq!(last_taker_update(&ev).status, OrderStatus::Filled);
        assert_eq!(e.resting_orders(), 0);
        assert_eq!(e.best_bid(), None);
        assert_eq!(e.best_ask(), None);
    }

    #[test]
    fn price_time_priority_fifo() {
        let mut e = Engine::new();
        e.process(limit(1, U1, Side::Ask, 100 * SCALE, SCALE));
        e.process(limit(2, U2, Side::Ask, 100 * SCALE, SCALE));
        let ev = e.process(limit(3, U1, Side::Bid, 100 * SCALE, SCALE));
        let ts = trades(&ev);
        assert_eq!(ts.len(), 1);
        assert_eq!(ts[0].maker_order_id, 1, "先挂先成交");
        assert_eq!(e.best_ask(), Some(100 * SCALE));
        assert_eq!(e.resting_orders(), 1);
    }

    #[test]
    fn multi_level_sweep_uses_best_prices_first() {
        let mut e = Engine::new();
        e.process(limit(1, U1, Side::Ask, 99 * SCALE, SCALE));
        e.process(limit(2, U2, Side::Ask, 100 * SCALE, 2 * SCALE));
        let ev = e.process(limit(3, U1, Side::Bid, 100 * SCALE, 3 * SCALE));
        let ts = trades(&ev);
        assert_eq!(ts.len(), 2);
        assert_eq!((ts[0].price, ts[0].qty), (99 * SCALE, SCALE));
        assert_eq!((ts[1].price, ts[1].qty), (100 * SCALE, 2 * SCALE));
        assert_eq!(last_taker_update(&ev).status, OrderStatus::Filled);
    }

    #[test]
    fn partial_fill_leaves_maker_resting() {
        let mut e = Engine::new();
        e.process(limit(1, U1, Side::Ask, 100 * SCALE, 5 * SCALE));
        let ev = e.process(limit(2, U2, Side::Bid, 100 * SCALE, 2 * SCALE));
        let us = updates(&ev);
        // maker 部分成交
        assert_eq!(us[0].order_id, 1);
        assert_eq!(us[0].status, OrderStatus::PartiallyFilled);
        assert_eq!(us[0].filled_qty, 2 * SCALE);
        // taker 全部成交
        assert_eq!(us[1].status, OrderStatus::Filled);
        assert_eq!(e.best_ask(), Some(100 * SCALE));
        assert_eq!(e.resting_orders(), 1);
    }

    #[test]
    fn gtc_taker_partially_fills_then_rests() {
        let mut e = Engine::new();
        e.process(limit(1, U1, Side::Ask, 100 * SCALE, 2 * SCALE));
        let ev = e.process(limit(2, U2, Side::Bid, 100 * SCALE, 5 * SCALE));
        assert_eq!(last_taker_update(&ev).status, OrderStatus::PartiallyFilled);
        assert_eq!(last_taker_update(&ev).filled_qty, 2 * SCALE);
        assert_eq!(e.best_bid(), Some(100 * SCALE));
        assert_eq!(e.resting_orders(), 1);
    }

    #[test]
    fn ioc_cancels_unfilled_remainder() {
        let mut e = Engine::new();
        e.process(limit(1, U1, Side::Ask, 100 * SCALE, 2 * SCALE));
        let ev = e.process(ioc(2, U2, Side::Bid, 100 * SCALE, 5 * SCALE));
        let u = last_taker_update(&ev);
        assert_eq!(u.status, OrderStatus::Canceled);
        assert_eq!(u.filled_qty, 2 * SCALE);
        assert_eq!(e.resting_orders(), 0, "IOC 剩余量不得挂簿");
    }

    #[test]
    fn fok_all_or_reject() {
        let mut e = Engine::new();
        e.process(limit(1, U1, Side::Ask, 100 * SCALE, 3 * SCALE));
        // 流动性不足 → 整单拒绝,无成交
        let ev_fail = e.process(fok(2, U2, Side::Bid, 100 * SCALE, 5 * SCALE));
        assert_eq!(last_taker_update(&ev_fail).status, OrderStatus::Rejected);
        assert!(trades(&ev_fail).is_empty());
        assert_eq!(e.resting_orders(), 1, "拒绝单不影响盘口");
        // 流动性充足 → 全部成交
        let ev_ok = e.process(fok(3, U2, Side::Bid, 100 * SCALE, 3 * SCALE));
        assert_eq!(last_taker_update(&ev_ok).status, OrderStatus::Filled);
        assert_eq!(trades(&ev_ok).len(), 1);
    }

    #[test]
    fn fok_counts_only_crossing_liquidity() {
        let mut e = Engine::new();
        e.process(limit(1, U1, Side::Ask, 101 * SCALE, 5 * SCALE));
        // 限价 100 吃不到 101 的卖单 → 拒绝
        let ev = e.process(fok(2, U2, Side::Bid, 100 * SCALE, 5 * SCALE));
        assert_eq!(last_taker_update(&ev).status, OrderStatus::Rejected);
    }

    #[test]
    fn post_only_rejects_on_cross() {
        let mut e = Engine::new();
        e.process(limit(1, U1, Side::Ask, 100 * SCALE, SCALE));
        // 会吃单 → 拒绝
        let ev_rej = e.process(post_only(2, U2, Side::Bid, 101 * SCALE, SCALE));
        assert_eq!(last_taker_update(&ev_rej).status, OrderStatus::Rejected);
        assert!(trades(&ev_rej).is_empty());
        // 不越价 → 正常挂簿
        let ev_ok = e.process(post_only(3, U2, Side::Bid, 99 * SCALE, SCALE));
        assert_eq!(last_taker_update(&ev_ok).status, OrderStatus::Open);
    }

    #[test]
    fn market_order_fills_and_cancels_remainder() {
        let mut e = Engine::new();
        e.process(limit(1, U1, Side::Ask, 100 * SCALE, 2 * SCALE));
        let ev = e.process(market(2, U2, Side::Bid, 5 * SCALE));
        let u = last_taker_update(&ev);
        assert_eq!(u.status, OrderStatus::Canceled);
        assert_eq!(u.filled_qty, 2 * SCALE);
        assert_eq!(u.price, None, "市价单更新不携带价格");
        assert_eq!(e.resting_orders(), 0);
        // 空盘口市价单 → 零成交撤销
        let ev_empty = e.process(market(3, U2, Side::Bid, SCALE));
        let u2 = last_taker_update(&ev_empty);
        assert_eq!(u2.status, OrderStatus::Canceled);
        assert_eq!(u2.filled_qty, 0);
    }

    #[test]
    fn market_bid_sweeps_multiple_levels() {
        let mut e = Engine::new();
        e.process(limit(1, U1, Side::Ask, 100 * SCALE, SCALE));
        e.process(limit(2, U1, Side::Ask, 101 * SCALE, SCALE));
        let ev = e.process(market(3, U2, Side::Bid, 2 * SCALE));
        let ts = trades(&ev);
        assert_eq!(ts.len(), 2);
        assert_eq!(ts[0].price, 100 * SCALE);
        assert_eq!(ts[1].price, 101 * SCALE);
        assert_eq!(last_taker_update(&ev).status, OrderStatus::Filled);
    }

    #[test]
    fn stp_cancel_taker_prevents_self_trade() {
        let mut e = Engine::new();
        e.process(limit(1, U1, Side::Ask, 100 * SCALE, SCALE));
        let mut p = match limit(2, U1, Side::Bid, 100 * SCALE, SCALE) {
            Command::Place(p) => p,
            _ => unreachable!(),
        };
        p.stp = StpMode::CancelTaker;
        let ev = e.process(Command::Place(p));
        assert!(trades(&ev).is_empty(), "自成交被禁止");
        assert_eq!(last_taker_update(&ev).status, OrderStatus::Canceled);
        assert_eq!(e.resting_orders(), 1, "原挂单不受影响");
    }

    #[test]
    fn cancel_resting_order() {
        let mut e = Engine::new();
        e.process(limit(1, U1, Side::Bid, 100 * SCALE, SCALE));
        let ev = e.process(cancel(1, U1));
        let u = last_taker_update(&ev);
        assert_eq!(u.status, OrderStatus::Canceled);
        assert_eq!(u.filled_qty, 0);
        assert_eq!(e.resting_orders(), 0);
        assert_eq!(e.best_bid(), None);
        // 撤销不存在的订单 → 无事件
        assert!(e.process(cancel(999, U1)).is_empty());
    }

    #[test]
    fn seq_is_strictly_monotonic() {
        let mut e = Engine::new();
        let mut last = 0u64;
        for cmd in [
            limit(1, U1, Side::Bid, 100 * SCALE, SCALE),
            limit(2, U2, Side::Ask, 100 * SCALE, 2 * SCALE),
            limit(3, U2, Side::Ask, 101 * SCALE, SCALE),
            cancel(3, U2),
        ] {
            for ev in e.process(cmd) {
                assert_eq!(ev.seq, last + 1, "seq 必须连续无空洞");
                last = ev.seq;
            }
        }
        assert_eq!(e.last_seq(), last);
    }

    #[test]
    fn replay_is_deterministic() {
        let cmds = vec![
            limit(1, U1, Side::Bid, 100 * SCALE, SCALE),
            limit(2, U2, Side::Bid, 100 * SCALE, 2 * SCALE),
            limit(3, U1, Side::Ask, 101 * SCALE, SCALE),
            ioc(4, U2, Side::Bid, 102 * SCALE, 3 * SCALE),
            fok(5, U1, Side::Bid, 100 * SCALE, 2 * SCALE),
            cancel(1, U1),
            limit(6, U2, Side::Ask, 99 * SCALE, SCALE),
        ];
        let mut a = Engine::new();
        let mut events_a = Vec::new();
        for c in &cmds {
            events_a.extend(a.process(c.clone()));
        }
        let mut b = Engine::new();
        let mut events_b = Vec::new();
        for c in &cmds {
            events_b.extend(b.process(c.clone()));
        }
        assert_eq!(events_a, events_b);
        assert_eq!(a.best_bid(), b.best_bid());
        assert_eq!(a.best_ask(), b.best_ask());
        assert_eq!(a.resting_orders(), b.resting_orders());
        assert_eq!(a.last_seq(), b.last_seq());
    }

    #[test]
    fn rejects_invalid_commands() {
        let mut e = Engine::new();
        // 数量为 0
        let ev0 = e.process(limit(1, U1, Side::Bid, 100 * SCALE, 0));
        assert_eq!(last_taker_update(&ev0).status, OrderStatus::Rejected);
        // 限价单缺价格
        let ev_np = e.process(Command::Place(PlaceCommand {
            order_id: 2,
            user_id: U1,
            side: Side::Bid,
            order_type: OrderType::Limit,
            tif: TimeInForce::Gtc,
            stp: StpMode::None,
            post_only: false,
            price: None,
            qty: SCALE,
        }));
        assert_eq!(last_taker_update(&ev_np).status, OrderStatus::Rejected);
        // 市价单带价格 / Post-Only
        let ev_mp = e.process(Command::Place(PlaceCommand {
            order_id: 3,
            user_id: U1,
            side: Side::Bid,
            order_type: OrderType::Market,
            tif: TimeInForce::Gtc,
            stp: StpMode::None,
            post_only: false,
            price: Some(100 * SCALE),
            qty: SCALE,
        }));
        assert_eq!(last_taker_update(&ev_mp).status, OrderStatus::Rejected);
        // 价格为 0
        let ev_zp = e.process(limit(4, U1, Side::Bid, 0, SCALE));
        assert_eq!(last_taker_update(&ev_zp).status, OrderStatus::Rejected);
        assert_eq!(e.resting_orders(), 0);
    }

    #[test]
    #[ignore = "性能冒烟:手动运行 cargo test -- --ignored --nocapture"]
    fn throughput_smoke() {
        let mut e = Engine::new();
        let n = 10_000u64;
        let t0 = std::time::Instant::now();
        for i in 0..n {
            e.process(limit(i + 1, U1, Side::Ask, 100 * SCALE + i, SCALE));
        }
        for i in 0..n {
            e.process(market(n + 1 + i, U2, Side::Bid, SCALE));
        }
        let dt = t0.elapsed();
        println!(
            "{} 条指令耗时 {:?}({:.0} cmd/s)",
            2 * n,
            dt,
            (2 * n) as f64 / dt.as_secs_f64()
        );
        assert_eq!(e.resting_orders(), 0);
    }
}
