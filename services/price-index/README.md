# price-index

外部行情聚合:对齐 Binance / OKX / Bybit / MEXC / Bitget 价格的基础设施。

## 职责

- 订阅五所公共 WS 行情(ticker/trade),经 `packages/exchange-connector` 统一格式;
- 加权计算指数价:权重按交易对配置;自动剔除偏离超阈值、心跳超时的异常源;
- 输出:Kafka 事件 + Redis 热点,供 market-maker / market / risk 读取;
- 二期用途:合约的标记价格与强平参考价。

## 依赖

外部交易所公共 WS(无需鉴权)、`packages/exchange-connector`、Kafka、Redis。
