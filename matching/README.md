# matching — 撮合引擎(Rust)

性能与正确性核心,独立于 Go 业务服务。决策见 ADR-001。

## 设计约束

- 每个交易对一个单线程引擎实例,订单簿全内存,无跨线程共享锁;
- 输入/输出全部为 Kafka 事件,事件溯源:从 log 重放可完整恢复订单簿;
- 输出事件带全局单调递增 sequence,下游据此校验连续性;
- 崩溃恢复 = 从最后 checkpoint 之后重放事件;
- 自成交防范(STP);订单类型:MVP 支持 GTC / IOC / FOK / Post-Only / 市价单。

## Workspace

- `crates/engine`:订单簿与撮合核心(已实现,见下)
- `crates/protocol`:输入/输出事件协议定义(纯类型、零依赖;JSON 序列化随 runner 层引入)

## 当前状态(v0.1.0)

- 已实现:限价(GTC/IOC/FOK/Post-Only)、市价(剩余量撤销)、价格-时间优先、
  部分成交、STP(None / CancelTaker)、撤单、全局单调 seq、确定性重放;
- 测试:17 个单元测试全绿(`cargo test`),含重放确定性测试;
- 性能冒烟(release):单线程约 324 万条指令/秒(2 万条指令 6.2ms);
- 待做(Phase 0 后期):Kafka runner(输入输出接线)、checkpoint/重放落盘、
  STP 扩展(CancelMaker / CancelBoth)、深度快照导出。
