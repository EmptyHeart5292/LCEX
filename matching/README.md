# matching — 撮合引擎(Rust)

性能与正确性核心,独立于 Go 业务服务。决策见 ADR-001。

## 设计约束

- 每个交易对一个单线程引擎实例,订单簿全内存,无跨线程共享锁;
- 输入/输出全部为 Kafka 事件,事件溯源:从 log 重放可完整恢复订单簿;
- 输出事件带全局单调递增 sequence,下游据此校验连续性;
- 崩溃恢复 = 从最后 checkpoint 之后重放事件;
- 自成交防范(STP);订单类型:MVP 支持 GTC / IOC / FOK / Post-Only / 市价单。

## Workspace

- `crates/engine`:订单簿与撮合核心
- `crates/protocol`:输入/输出事件协议定义(与 Kafka 序列化格式一一对应)

## Topic 规划(拟定)

- 输入:`cex.orders.in.{symbol}`
- 输出:`cex.trades.{symbol}`、`cex.order-events.{symbol}`

状态:骨架已建,核心实现见 docs/roadmap.md(Phase 0 PoC → Phase 1 交付)。
