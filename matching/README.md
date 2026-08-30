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
- `crates/protocol`:输入/输出事件协议定义(serde JSON;定点整数,禁浮点)
- `crates/runner`:Kafka 接线进程 `cex-runner`

## Topic 与消息格式

- 输入:`cex.orders.in.{symbol}` —— `{"type":"place",...}` / `{"type":"cancel",...}`
- 输出:`cex.events.{symbol}` —— `{"seq":N,"kind":"trade"|"order_update","data":{...}}`
- symbol 为小写:`cex.orders.in.btc-usdt`;消息格式定义见 `crates/protocol/src/lib.rs`
- **每交易对输入/输出均单分区**;输出单一 topic 保证下游事件顺序与引擎一致
  (成交先于终态 order_update,清算"先结算后解冻"依赖此顺序),实例间按 symbol 静态分片

## 运行

```bash
docker compose -f infra/docker-compose.yml up -d kafka   # 本地 broker
CEX_KAFKA_BROKERS=localhost:9092 cargo run -p cex-runner  # 默认 BTC/ETH/SOL-USDT
```

环境变量:`CEX_KAFKA_BROKERS` / `CEX_SYMBOLS`(逗号分隔)/ `CEX_TOPIC_PREFIX`。

## 当前状态(v0.2.0)

- 已实现:限价(GTC/IOC/FOK/Post-Only)、市价(剩余量撤销)、价格-时间优先、
  部分成交、STP(None / CancelTaker)、撤单、全局单调 seq、确定性重放;
  JSON 编解码(扁平信封)、事件路由、Kafka 消费/生产 worker(SIGINT/SIGTERM 优雅退出);
- 测试:24 个单元测试全绿;性能冒烟(release)单线程约 324 万条指令/秒;
- 待做(v0.3.0):与 order 服务联调的真实 broker E2E、checkpoint/重放落盘、
  STP 扩展(CancelMaker / CancelBoth)、深度快照导出。
