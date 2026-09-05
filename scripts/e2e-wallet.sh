#!/usr/bin/env bash
# 资金闭环验收(mock 链):地址派生 → 确认数入账 → 提现扣账+手续费 → 链上确认。
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
export PATH="$HOME/go-sdk/go/bin:$PATH"
PG_CT="${PG_CONTAINER:-cex-dev-postgres-1}"
HTTP="http://localhost:8084"

log() { echo "[e2e-wd] $*"; }
fail() { echo "[e2e-wd] FAIL: $*" >&2; exit 1; }
q() { docker exec "$PG_CT" psql -U cex -d cex -tAc "$1"; }

command -v go >/dev/null || fail "需要 Go"
docker exec "$PG_CT" true 2>/dev/null || fail "postgres 未运行"

log "构建 + 迁移"
(cd "$ROOT" && go build -o /tmp/cex-wallet ./services/wallet)
bash "$ROOT/scripts/db-migrate.sh" >/dev/null

pkill -9 -f "tmp/cex-wallet" 2>/dev/null || true
sleep 0.3
psql() { docker exec -i "$PG_CT" psql -U cex -d cex -v ON_ERROR_STOP=1 "$@"; }
psql -c "TRUNCATE deposits, withdrawals, deposit_addresses, orders, journals, ledger_entries, accounts RESTART IDENTITY CASCADE" >/dev/null

CEX_CURRENCIES_FILE="$ROOT/packages/api-spec/currencies.yaml" /tmp/cex-wallet >/tmp/cex-wallet.log 2>&1 & WP=$!
trap 'kill $WP 2>/dev/null || true' EXIT
for i in $(seq 1 20); do
  curl -sf "$HTTP/healthz" >/dev/null && break
  [ "$i" = 20 ] && fail "wallet 未就绪"
  sleep 0.3
done

AUTH=(-H "X-User-Id: 201" -H "Content-Type: application/json")

log "派生 BTC 充值地址"
ADDR=$(curl -sf "${AUTH[@]}" "$HTTP/api/v1/deposit-addresses?currency=BTC&network=bitcoin" \
  | python3 -c "import json,sys; d=json.load(sys.stdin); assert d['code']==0; print(d['data']['address'])")
[ -n "$ADDR" ] || fail "空地址"
ADDR2=$(curl -sf "${AUTH[@]}" "$HTTP/api/v1/deposit-addresses?currency=BTC&network=bitcoin" \
  | python3 -c "import json,sys; print(json.load(sys.stdin)['data']['address'])")
[ "$ADDR" = "$ADDR2" ] || fail "地址应幂等: $ADDR vs $ADDR2"
log "address=$ADDR"

log "未达确认数 → 不入账"
curl -sf -X POST "$HTTP/internal/chain/deposits" -H "Content-Type: application/json" \
  -d "{\"address\":\"$ADDR\",\"txid\":\"tx-e2e-1\",\"outputIndex\":0,\"amount\":\"0.01\",\"confirmations\":0}" \
  | python3 -c "import json,sys; d=json.load(sys.stdin); assert d['code']==0 and d['data']['status']=='pending' and d['data']['credited'] is False, d"
BAL=$(curl -sf -H "X-User-Id: 201" "$HTTP/api/v1/account/balances" \
  | python3 -c "import json,sys; d=json.load(sys.stdin)['data'] or []; print(next((x['available'] for x in d if x['currency']=='BTC'),'0'))")
[ "$BAL" = "0" ] || fail "未确认不应入账: $BAL"

log "确认数达标 → 入账"
curl -sf -X POST "$HTTP/internal/chain/deposits" -H "Content-Type: application/json" \
  -d "{\"address\":\"$ADDR\",\"txid\":\"tx-e2e-1\",\"outputIndex\":0,\"amount\":\"0.01\",\"confirmations\":2}" \
  | python3 -c "import json,sys; d=json.load(sys.stdin); assert d['code']==0 and d['data']['credited'] is True, d"
BAL=$(curl -sf -H "X-User-Id: 201" "$HTTP/api/v1/account/balances" \
  | python3 -c "import json,sys; d=json.load(sys.stdin)['data']; print(next(x['available'] for x in d if x['currency']=='BTC'))")
[ "$BAL" = "0.01" ] || fail "入账后余额应为 0.01,实际 $BAL"

log "同笔重放幂等"
curl -sf -X POST "$HTTP/internal/chain/deposits" -H "Content-Type: application/json" \
  -d "{\"address\":\"$ADDR\",\"txid\":\"tx-e2e-1\",\"outputIndex\":0,\"amount\":\"0.01\",\"confirmations\":3}" \
  | python3 -c "import json,sys; d=json.load(sys.stdin); assert d['data']['status']=='credited', d"
BAL=$(curl -sf -H "X-User-Id: 201" "$HTTP/api/v1/account/balances" \
  | python3 -c "import json,sys; d=json.load(sys.stdin)['data']; print(next(x['available'] for x in d if x['currency']=='BTC'))")
[ "$BAL" = "0.01" ] || fail "重放后余额应变: $BAL"

log "低于最小充值拒绝"
CODE=$(curl -s -X POST "$HTTP/internal/chain/deposits" -H "Content-Type: application/json" \
  -d "{\"address\":\"$ADDR\",\"txid\":\"tx-tiny\",\"outputIndex\":0,\"amount\":\"0.0001\",\"confirmations\":2}" \
  | python3 -c "import json,sys; print(json.load(sys.stdin)['code'])")
[ "$CODE" = "10003" ] || fail "小额充值应 10003,实际 $CODE"

log "低于最小提现拒绝"
CODE=$(curl -s -X POST "$HTTP/api/v1/withdrawals" "${AUTH[@]}" \
  -d '{"currency":"BTC","network":"bitcoin","address":"bcrt1qdest","amount":"0.0001","clientOrderId":"wd-tiny"}' \
  | python3 -c "import json,sys; print(json.load(sys.stdin)['code'])")
[ "$CODE" = "60001" ] || fail "小额提现应 60001,实际 $CODE"

log "提现 0.001 + fee 0.0002"
WD=$(curl -sf -X POST "$HTTP/api/v1/withdrawals" "${AUTH[@]}" \
  -d '{"currency":"BTC","network":"bitcoin","address":"bcrt1qdest0001","amount":"0.001","clientOrderId":"wd-e2e-1"}')
echo "$WD" | python3 -c "import json,sys; d=json.load(sys.stdin); assert d['code']==0 and d['data']['status']=='broadcasting', d"
WID=$(echo "$WD" | python3 -c "import json,sys; print(json.load(sys.stdin)['data']['withdrawalId'])")
BAL=$(curl -sf -H "X-User-Id: 201" "$HTTP/api/v1/account/balances" \
  | python3 -c "import json,sys; d=json.load(sys.stdin)['data']; print(next(x['available'] for x in d if x['currency']=='BTC'))")
[ "$BAL" = "0.0088" ] || fail "提现后余额应为 0.0088,实际 $BAL"

log "链上确认提现"
curl -sf -X POST "$HTTP/internal/chain/withdrawals/$WID/confirm" -H "Content-Type: application/json" \
  -d '{"txid":"tx-wd-1"}' | python3 -c "import json,sys; d=json.load(sys.stdin); assert d['data']['status']=='completed', d"

MIS=$(q "SELECT count(*) FROM v_balance_mismatch")
[ "$MIS" = "0" ] || fail "账实不符 $MIS"

log "PASS ✔  地址/确认入账/幂等/最小额/提现扣费/链上确认"
