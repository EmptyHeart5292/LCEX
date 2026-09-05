#!/usr/bin/env bash
# Phase 2 起步验收:mock 入账 → 做市按指数双边挂单 → 盘口出现买卖档。
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
export PATH="$HOME/go-sdk/go/bin:$PATH"
PG_CT="${PG_CONTAINER:-cex-dev-postgres-1}"
KC_CT="${KAFKA_CONTAINER:-cex-dev-kafka-1}"
KBIN="${KAFKA_BIN_DIR:-/opt/kafka/bin}"
BROKERS="${CEX_KAFKA_BROKERS:-localhost:9092}"
RUNNER="$ROOT/matching/target/release/cex-runner"
SYMBOL="btc-usdt"
P7=0
HTTP_O="http://localhost:8081"
HTTP_M="http://localhost:8082"
HTTP_PX="http://localhost:8083"
HTTP_W="http://localhost:8084"

log() { echo "[e2e-mm] $*"; }
fail() { echo "[e2e-mm] FAIL: $*" >&2; exit 1; }
psql() { docker exec -i "$PG_CT" psql -U cex -d cex -v ON_ERROR_STOP=1 "$@"; }
kc()   { timeout 20 docker exec -i "$KC_CT" "$KBIN/$(basename "$1")" --bootstrap-server "$BROKERS" "${@:2}"; }

command -v go >/dev/null || fail "需要 Go 工具链"
[ -x "$RUNNER" ] || fail "先构建 runner"
docker exec "$PG_CT" true 2>/dev/null || fail "postgres 未运行"
docker exec "$KC_CT" true 2>/dev/null || fail "kafka 未运行"

log "构建"
(cd "$ROOT" && go build -o /tmp/cex-order ./services/order && go build -o /tmp/cex-clearing ./services/clearing \
  && go build -o /tmp/cex-market ./services/market && go build -o /tmp/cex-price-index ./services/price-index \
  && go build -o /tmp/cex-wallet ./services/wallet && go build -o /tmp/cex-mm ./services/market-maker \
  && go build -o /tmp/cex-mockex ./scripts/mockex)
bash "$ROOT/scripts/db-migrate.sh" >/dev/null

pkill -9 -f "tmp/cex-order" 2>/dev/null || true
pkill -9 -f "tmp/cex-clearing" 2>/dev/null || true
pkill -9 -f "tmp/cex-market" 2>/dev/null || true
pkill -9 -f "tmp/cex-price-index" 2>/dev/null || true
pkill -9 -f "tmp/cex-wallet" 2>/dev/null || true
pkill -9 -f "tmp/cex-mm" 2>/dev/null || true
pkill -9 -f "tmp/cex-mockex" 2>/dev/null || true
pkill -9 -f "tmp/cex-runner" 2>/dev/null || true
sleep 0.5

psql -c "TRUNCATE orders, journals, ledger_entries, accounts RESTART IDENTITY CASCADE" >/dev/null
for t in "cex.orders.in.$SYMBOL" "cex.events.$SYMBOL"; do
  kc kafka-topics.sh --delete --topic "$t" >/dev/null 2>&1 || true
done
for t in "cex.orders.in.$SYMBOL" "cex.events.$SYMBOL"; do
  for _ in $(seq 1 20); do
    kc kafka-topics.sh --describe --topic "$t" 2>/dev/null | grep -q "Topic: $t" || break
    sleep 0.3
  done
  kc kafka-topics.sh --create --if-not-exists --topic "$t" --partitions 1 >/dev/null
done
for g in "cex-matching-$SYMBOL" clearing order-status-sync market; do
  kc kafka-consumer-groups.sh --reset-offsets --group "$g" --all-topics --to-earliest --execute >/dev/null 2>&1 || true
done

log "启动服务"
echo "50000 50001" > /tmp/mockex-px.txt
/tmp/cex-mockex -addr :9999 -stream "btcusdt@bookTicker" -bid 50000 -ask 50001 -file /tmp/mockex-px.txt >/tmp/mockex.log 2>&1 & PM=$!
CEX_KAFKA_BROKERS="$BROKERS" CEX_MARKETS_FILE="$ROOT/packages/api-spec/markets.yaml" /tmp/cex-clearing >/tmp/cex-clearing.log 2>&1 & P1=$!
CEX_KAFKA_BROKERS="$BROKERS" CEX_MARKETS_FILE="$ROOT/packages/api-spec/markets.yaml" /tmp/cex-order >/tmp/cex-order.log 2>&1 & P2=$!
CEX_KAFKA_BROKERS="$BROKERS" "$RUNNER" >/tmp/cex-runner-t.log 2>&1 & P3=$!
CEX_KAFKA_BROKERS="$BROKERS" /tmp/cex-market >/tmp/cex-market.log 2>&1 & P4=$!
CEX_KAFKA_BROKERS="$BROKERS" CEX_SYMBOLS="BTC-USDT" CEX_PX_SOURCES=binance \
  CEX_PX_BINANCE_URL="ws://localhost:9999/ws/btcusdt" CEX_PX_STALE_MS=3000 \
  /tmp/cex-price-index >/tmp/cex-price-index.log 2>&1 & P5=$!
/tmp/cex-wallet >/tmp/cex-wallet.log 2>&1 & P6=$!
trap 'kill $PM $P1 $P2 $P3 $P4 $P5 $P6 $P7 2>/dev/null || true; sleep 1; kill -9 $PM $P1 $P2 $P3 $P4 $P5 $P6 $P7 2>/dev/null || true' EXIT

for i in $(seq 1 30); do
  curl -sf "$HTTP_O/healthz" >/dev/null 2>&1 && curl -sf "$HTTP_W/healthz" >/dev/null 2>&1 && break
  [ "$i" = 30 ] && fail "order/wallet 未就绪"
  sleep 0.4
done

log "mock 入账 MM 9001"
curl -sf -X POST "$HTTP_W/internal/deposits" -H "Content-Type: application/json" \
  -d '{"userId":9001,"currency":"USDT","amount":"1000000","bizId":"dep-9001-usdt"}' | grep -q '"code":0' \
  || fail "USDT 入账失败"
curl -sf -X POST "$HTTP_W/internal/deposits" -H "Content-Type: application/json" \
  -d '{"userId":9001,"currency":"BTC","amount":"10","bizId":"dep-9001-btc"}' | grep -q '"code":0' \
  || fail "BTC 入账失败"
# 幂等
curl -sf -X POST "$HTTP_W/internal/deposits" -H "Content-Type: application/json" \
  -d '{"userId":9001,"currency":"USDT","amount":"1000000","bizId":"dep-9001-usdt"}' | grep -q '"replayed":true' \
  || fail "入账幂等失败"

MIS=$(docker exec "$PG_CT" psql -U cex -d cex -tAc "SELECT count(*) FROM v_balance_mismatch")
[ "$MIS" = "0" ] || fail "入账后账实不符: $MIS"

log "等待指数"
for i in $(seq 1 30); do
  OK=$(curl -s -m 2 "$HTTP_PX/index/BTC-USDT" | python3 -c "import json,sys; print(json.load(sys.stdin).get('ok'))" 2>/dev/null || echo "")
  [ "$OK" = "True" ] && break
  [ "$i" = 30 ] && fail "指数未就绪"
  sleep 0.4
done

log "启动做市"
CEX_ORDER_URL="$HTTP_O" CEX_INDEX_URL="$HTTP_PX" CEX_MM_USER_ID=9001 CEX_SYMBOLS="BTC-USDT" \
  CEX_MM_HALF_SPREAD_BPS=10 CEX_MM_QTY="0.05" CEX_MM_REFRESH_MS=400 \
  /tmp/cex-mm >/tmp/cex-mm.log 2>&1 & P7=$!

log "等待盘口双边"
for i in $(seq 1 40); do
  curl -sf "$HTTP_M/api/v1/depth?symbol=BTC-USDT&limit=5" >/tmp/e2e-mm-depth.json || true
  if python3 - <<'PY'
import json
raw=json.load(open("/tmp/e2e-mm-depth.json"))
d=raw.get("data") or raw
bids=d.get("bids") or []
asks=d.get("asks") or []
assert bids and asks
print("[e2e-mm] depth bids=%s asks=%s" % (bids[:1], asks[:1]))
PY
  then
    break
  fi
  [ "$i" = 40 ] && fail "盘口未出现双边,日志 /tmp/cex-mm.log /tmp/cex-order.log"
  sleep 0.4
done

python3 - <<'PY' || fail "档位应夹住指数"
import json,urllib.request
idx=json.load(urllib.request.urlopen("http://localhost:8083/index/BTC-USDT"))
depth=json.load(urllib.request.urlopen("http://localhost:8082/api/v1/depth?symbol=BTC-USDT&limit=5"))
d=depth.get("data") or depth
bid=float(d["bids"][0][0]); ask=float(d["asks"][0][0]); px=float(idx["index"])
assert bid < px < ask, (bid, px, ask)
print("[e2e-mm] bid %.4f < index %.4f < ask %.4f" % (bid, px, ask))
PY

log "PASS ✔  mock入账 + 做市双边挂单 + 账本平衡"
