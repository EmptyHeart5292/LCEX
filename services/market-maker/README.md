# market-maker

内部做市服务:为新平台盘口提供流动性并锚定外部价格。语言决策见 ADR-007(Go)。

## 策略

- 以 price-index 指数价为锚,双边挂单(买卖各 N 档),价差/单量按交易对配置;
- 刷新触发:价格移动超阈值、时间超时、部分成交后重挂(cancel/replace);
- 库存管理:各币种库存上限;偏离目标库存时调整报价偏移,并通过外部交易所私有 API 对冲保持中性;
- 每个交易对一个独立的单 goroutine 策略循环,状态不跨 goroutine 共享。

## 风险红线

- 与指数价最大偏移限制(超限撤单)、最大挂单名义额限制;
- 对冲通道故障时自动停止报价;
- 全局 kill switch 与单交易对暂停开关(接 risk / Admin)。

## 硬性约束

- 全部做市单走 order → matching → clearing 正常链路,不绕过风控与账本,不提供旁路。

## 依赖

Kafka(指数价、本所行情)、order 服务(下单,与普通用户同链路)、`packages/exchange-connector`(外部所私有 API 对冲)。
