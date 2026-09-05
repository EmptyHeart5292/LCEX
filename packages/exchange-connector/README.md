# exchange-connector

外部交易所连接器,供 `price-index` 与 `market-maker` 复用(ADR-006、ADR-007)。

## 当前

- `Hedger` 接口 + `Mock` 实现:做市库存偏离时记录对冲意图,不打真实所。
- `Healthy()==false` 时做市停报价。
- 五所公共行情仍在 `services/price-index`;私有 HMAC 下单后续迁入本包。

## 约束

价格/数量定点 ×1e8;对冲不得绕过本所 order 账本(本所挂单仍走 order 服务)。
