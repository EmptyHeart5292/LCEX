# services — 后端微服务(Go)

除撮合引擎(Rust,见 `../matching`)外,全部业务服务统一使用 Go。
跨服务异步事件一律经 Kafka;同步调用使用 gRPC。接口契约见 `packages/api-spec`。

| 服务 | 职责 | 存储 / 依赖 |
|---|---|---|
| api-gateway | REST/WS 接入、鉴权、限频、推送 | Redis、Kafka |
| account | 用户、2FA、API Key、资金密码 | PostgreSQL、Redis |
| order | 下单校验、资金冻结、订单生命周期 | PostgreSQL、Kafka |
| clearing | 清算、复式记账、手续费 | PostgreSQL、Kafka |
| market | 深度/ticker/K线、历史行情 | Redis、ClickHouse、Kafka |
| price-index | 外部行情聚合、指数价 | Kafka、Redis |
| market-maker | 内部做市、库存对冲(语言决策见 ADR-007) | Kafka、order 服务、外部所私有 API |
| wallet | 托管对接、充值提现、归集、对账 | PostgreSQL、托管商 API |
| risk | 限额、名单、偏移监控、熔断 | Redis、PostgreSQL |

共享库(规划):`packages/exchange-connector` —— 外部交易所连接器,price-index 与 market-maker 复用。

后续按需新增:notify(邮件/短信通知)、report(报表)。
