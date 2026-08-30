# db — 数据库迁移

PostgreSQL 迁移目录,格式兼容 [golang-migrate](https://github.com/golang-migrate/migrate):
`NNNNNN_name.up.sql` / `NNNNNN_name.down.sql`,编号单调递增,禁止修改已合入的迁移文件。

## 本地执行

```bash
docker compose -f infra/docker-compose.yml up -d postgres
docker exec -i cex-dev-postgres-1 psql -U cex -d cex < db/migrations/000001_ledger_core.up.sql
```

(Phase 1 引入 golang-migrate 后由 `scripts/db-migrate.sh` 统一执行。)

## 约定

- **MVP 阶段共享一个数据库 `cex`**,按模块分表;表归属服务的写入权在迁移文件头部注释声明,其他服务不得直接写;
- 跨服务引用只存 ID、不建 FK(如 `accounts.owner_id` 引用 account 服务的 users);
- 金额一律 `BIGINT` 定点整数(× 1e8),禁止浮点;
- 幂等在 DB 层用唯一约束兜底(如 `journals(biz_type, biz_id)`),应用层消费端仍需幂等。

## 账本不变式(违者熔断)

1. 每个 journal 逐币种 debit 总额 == credit 总额;
2. `accounts.balance` 必须等于该账户全部分录按方向累计值(公式按账户类别,见迁移文件)—— 校验:`SELECT * FROM v_balance_mismatch;` 恒返回 0 行;
3. 余额只由流水推导,任何代码路径禁止绕过分录直接 UPDATE balance。

## 方向语义(经典会计)

| 账户类别 | owner_type | 余额增加 | 余额减少 | 例子 |
|---|---|---|---|---|
| 资产类 | `system` | debit | credit | 链上热/冷钱包 |
| 负债类 | `user` / `market_maker` | credit | debit | 用户余额、冻结子账户 |
| 收入类 | `fee` | credit | debit | 手续费收入 |
| 权益类 | `equity` | credit | debit | 平台资本金 |

典型分录:

- **充值**:debit 热钱包(asset ↑)+ credit 用户 available(liability ↑),同一条 journal —— 链上转账入账即完成"钱包→用户"的转移;
- **提现**:debit 用户 available + credit 热钱包;
- **下单冻结**:debit 用户 available + credit 用户 frozen(同为负债类内部划转);
- **成交**:debit 买方 frozen + credit 卖方 available + credit fee(手续费),三发票、借贷平衡;
- **手续费**:debit 用户 available + credit fee。
