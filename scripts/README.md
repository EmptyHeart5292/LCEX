# scripts

开发与运维脚本。

- `e2e-matching.sh`:撮合链路端到端验收 —— 真实 Kafka + cex-runner,
  投递 4 条测试指令并断言输出(事件数/价格数量/状态机/seq 连续性)。
  前置:kafka 容器已启动 + release 二进制已构建,直接运行即可。

Phase 1 起补充:

- `bootstrap-kafka-topics.sh`:创建生产环境 topic(替代 compose 中的自动建 topic)
- `db-migrate.sh`:数据库迁移
- `replay-check.sh`:账本流水重放校验(见 services/clearing/README.md 不变式)

- `e2e-mm.sh`:Phase 2 起步 —— mock 入账 + 做市按指数双边挂单,断言盘口夹住指数。
  前置:postgres/kafka/redis + cex-runner release。
- `e2e-wallet.sh`:资金闭环 mock 链 —— 地址/确认入账/提现扣费/幂等。
