# exchange-connector(规划,Go 库)

外部交易所连接器:Binance / OKX / Bybit / MEXC / Bitget 的统一接入层。
由 `services/price-index` 与 `services/market-maker` 复用(见 ADR-006、ADR-007)。

## 范围

- 公共:WS ticker / trade / depth 订阅(指数价、对冲参考);
- 私有:下单/撤单、余额与成交回报(对冲通道),HMAC 签名;
- 通用设施:心跳与自动重连、服务器时间同步、限频适配。

## 约束

- 价格/数量一律使用定点整数(价格 × 1e8 等),杜绝浮点;
- 每所一个 adapter 实现,注册制接入;
- 连接状态与延迟指标统一暴露给 Prometheus。
