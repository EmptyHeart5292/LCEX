# infra

本地开发与部署基础设施。

## 本地依赖(开发环境)

```bash
docker compose -f infra/docker-compose.yml up -d
```

| 服务 | 镜像 | 端口 | 用途 |
|---|---|---|---|
| PostgreSQL | postgres:16-alpine | 5432 | 账本/账户/订单/提现 |
| Redis | redis:7-alpine | 6379 | 缓存/限频/热点行情 |
| Kafka(KRaft 单节点) | bitnami/kafka:3.7 | 9092 | 事件总线 |
| ClickHouse | clickhouse/clickhouse-server:24.8 | 8123 / 9000 | K线与历史行情 |

## 说明

- Kafka advertised listener 配置为 `localhost:9092`:开发期业务服务直接在宿主机运行(go run / cargo run),连 localhost;容器间互访场景后续再调;
- 开发环境 Kafka 开启了自动建 topic,生产环境由显式的 topic 清单管理;
- k8s 生产编排随 Phase 4 补充。
