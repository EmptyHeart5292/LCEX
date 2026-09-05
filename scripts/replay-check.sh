#!/usr/bin/env bash
# 账本重放校验:账实不符视图、资产=负债+权益、充提记录均有 journal。
set -euo pipefail
PG_CT="${PG_CONTAINER:-cex-dev-postgres-1}"
q() { docker exec "$PG_CT" psql -U cex -d cex -tAc "$1"; }
fail() { echo "[replay-check] FAIL: $*" >&2; exit 1; }

docker exec "$PG_CT" true 2>/dev/null || fail "postgres 未运行"

MIS=$(q "SELECT count(*) FROM v_balance_mismatch")
[ "$MIS" = "0" ] || fail "v_balance_mismatch=$MIS"

EQ=$(q "SELECT (COALESCE(SUM(CASE WHEN owner_type='system' THEN balance END),0) = COALESCE(SUM(CASE WHEN owner_type<>'system' THEN balance END),0))::int FROM accounts")
[ "$EQ" = "1" ] || fail "资产 != 负债+权益"

OD=$(q "SELECT count(*) FROM deposits d WHERE d.status='credited' AND NOT EXISTS (SELECT 1 FROM journals j WHERE j.biz_type='deposit' AND j.biz_id='onchain-'||d.txid||'-'||d.output_index::text)")
[ "$OD" = "0" ] || fail "credited 充值无 journal: $OD"

OW=$(q "SELECT count(*) FROM withdrawals w WHERE w.status IN ('broadcasting','completed') AND NOT EXISTS (SELECT 1 FROM journals j WHERE j.biz_type='withdraw' AND j.biz_id='wd-'||w.id::text)")
[ "$OW" = "0" ] || fail "提现无 journal: $OW"

echo "[replay-check] PASS mismatch=0 assets=claims deposits/withdrawals journaled"
