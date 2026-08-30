# market

行情服务:撮合事件的统一下游。

## 职责

- 维护实时深度(增量 + 定期快照)、ticker、最近成交;
- K线聚合:1m/5m/15m/1h/4h/1d,历史落 ClickHouse;
- 对外提供查询 REST,并供 api-gateway WS 广播;
- 全局 sequence 校验事件连续性;
- 盘口过薄时,行情展示可回退 price-index 指数数据(可配置)。

## 依赖

Kafka(撮合事件、指数价)、Redis(热点)、ClickHouse(历史)。
