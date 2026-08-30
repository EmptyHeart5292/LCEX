# order

订单服务:交易入口的资金守门人。

## 职责

- 下单:参数与交易规则校验(精度、最小名义额,配置见 `packages/api-spec/markets.yaml`)→ 复式记账冻结资金 → 订单事件写 Kafka;
- 撤单:校验状态 → 解冻剩余资金 → 取消事件写 Kafka;
- 订单查询:当前委托、历史委托、成交明细;
- clientOrderId 幂等去重;
- 订单状态机:NEW → PARTIALLY_FILLED → FILLED / CANCELED / REJECTED。

## 依赖

PostgreSQL(订单)、Kafka(投递撮合)、clearing 提供的账本接口(冻结/解冻/划转)、risk(同步规则)。
