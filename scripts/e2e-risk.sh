#!/usr/bin/env bash
# 风控闸 + 对账:黑名单、日限额、提现暂停、充提 vs 账本。
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
export PATH="$HOME/go-sdk/go/bin:$PATH"
PG_CT="${PG_CONTAINER:-cex-dev-postgres-1}"
W="http://localhost:8084"
R="http://localhost:8086"

log() { echo "[e2e-risk] $*"; }
fail() { echo "[e2e-risk] FAIL: $*" >&2; exit 1; }

command -v go >/dev/null || fail "需要 Go"
docker exec "$PG_CT" true 2>/dev/null || fail "postgres 未运行"

log "构建"
(cd "$ROOT" && go build -o /tmp/cex-wallet ./services/wallet && go build -o /tmp/cex-risk ./services/risk)
bash "$ROOT/scripts/db-migrate.sh" >/dev/null
pkill -9 -f "tmp/cex-wallet" 2>/dev/null || true
pkill -9 -f "tmp/cex-risk" 2>/dev/null || true
sleep 0.3
docker exec -i "$PG_CT" psql -U cex -d cex -v ON_ERROR_STOP=1 -c \
  "TRUNCATE deposits, withdrawals, deposit_addresses, orders, journals, ledger_entries, accounts RESTART IDENTITY CASCADE" >/dev/null

CEX_CURRENCIES_FILE="$ROOT/packages/api-spec/currencies.yaml" CEX_RISK_URL="$R" \
  /tmp/cex-wallet >/tmp/cex-wallet.log 2>&1 & WP=$!
CEX_RISK_DENY_ADDRESSES="bcrt1qblocked000" CEX_RISK_DAILY_WITHDRAW="BTC:0.002" \
  /tmp/cex-risk >/tmp/cex-risk.log 2>&1 & RP=$!
trap 'kill $WP $RP 2>/dev/null || true' EXIT
for i in $(seq 1 25); do
  curl -sf "$W/healthz" >/dev/null && curl -sf "$R/healthz" >/dev/null && break
  [ "$i" = 25 ] && fail "wallet/risk 未就绪"
  sleep 0.3
done

AUTH=(-H "X-User-Id: 201" -H "Content-Type: application/json")
ADDR=$(curl -sf "${AUTH[@]}" "$W/api/v1/deposit-addresses?currency=BTC&network=bitcoin" \
  | python3 -c "import json,sys; print(json.load(sys.stdin)['data']['address'])")
curl -sf -X POST "$W/internal/chain/deposits" -H "Content-Type: application/json" \
  -d "{\"address\":\"$ADDR\",\"txid\":\"tx-risk-1\",\"outputIndex\":0,\"amount\":\"0.01\",\"confirmations\":2}" >/dev/null

log "黑名单地址"
CODE=$(curl -s -X POST "$W/api/v1/withdrawals" "${AUTH[@]}" \
  -d '{"currency":"BTC","network":"bitcoin","address":"bcrt1qblocked000","amount":"0.001","clientOrderId":"wd-deny"}' \
  | python3 -c "import json,sys; print(json.load(sys.stdin)['code'])")
[ "$CODE" = "60005" ] || fail "黑名单应 60005,实际 $CODE"

log "日限额内通过"
curl -sf -X POST "$W/api/v1/withdrawals" "${AUTH[@]}" \
  -d '{"currency":"BTC","network":"bitcoin","address":"bcrt1qok","amount":"0.0015","clientOrderId":"wd-ok"}' \
  | python3 -c "import json,sys; d=json.load(sys.stdin); assert d['code']==0, d"

log "超出日限额"
CODE=$(curl -s -X POST "$W/api/v1/withdrawals" "${AUTH[@]}" \
  -d '{"currency":"BTC","network":"bitcoin","address":"bcrt1qok","amount":"0.001","clientOrderId":"wd-over"}' \
  | python3 -c "import json,sys; print(json.load(sys.stdin)['code'])")
[ "$CODE" = "60005" ] || fail "日限额应 60005,实际 $CODE"

log "提现熔断"
curl -sf -X POST "$R/v1/pause" -H "Content-Type: application/json" -d '{"withdraw":true}' >/dev/null
CODE=$(curl -s -X POST "$W/api/v1/withdrawals" "${AUTH[@]}" \
  -d '{"currency":"BTC","network":"bitcoin","address":"bcrt1qok","amount":"0.001","clientOrderId":"wd-kill"}' \
  | python3 -c "import json,sys; print(json.load(sys.stdin)['code'])")
[ "$CODE" = "60004" ] || fail "熔断应 60004,实际 $CODE"
curl -sf -X POST "$R/v1/resume" -H "Content-Type: application/json" -d '{"withdraw":true}' >/dev/null

log "对账"
REC=$(curl -sf "$W/internal/reconcile")
echo "$REC" | python3 -c "import json,sys; d=json.load(sys.stdin)['data']; assert d['ok'] is True, d; print('[e2e-risk] reconcile ok', d)"
bash "$ROOT/scripts/replay-check.sh"

log "PASS ✔  黑名单/日限额/熔断/对账"
