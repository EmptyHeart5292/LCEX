# risk

风控服务:同步规则 + 异步监控。

## 职责

- 同步规则(被 order / wallet 同步调用):下单与提现限额、黑白名单(用户、提现地址)、API Key 权限校验;
- 价格偏移监控:本所成交价 vs price-index 指数价偏离超阈值 → 告警 / 熔断;
- 异常行为(基础版):刷量、对敲、异常登录;
- 熔断开关:提现暂停、交易对暂停、做市暂停(Admin 可操作);
- 制裁地址筛查:预留接口(Chainalysis / TRM,见 ADR-005);
- market-maker 的做市参数监控与 kill switch 落地。

## 依赖

Redis(计数、名单缓存)、PostgreSQL(规则、事件)、Kafka(全量事件订阅)、price-index(指数价)。
