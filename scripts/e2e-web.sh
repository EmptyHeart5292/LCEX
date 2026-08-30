#!/usr/bin/env bash
# PC 网站(迷你网关 + 静态页)验收:
#   1. 静态页可访问;2. API 经 :8080 反代可用;3. WS 经反代可推送;4. 下单/撤单走网关全链路。
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
export PATH="$HOME/go-sdk/go/bin:$PATH"
PG_CT="${PG_CONTAINER:-cex-dev-postgres-1}"
KC_CT="${KAFKA_CONTAINER:-cex-dev-kafka-1}"
KBIN="${KAFKA_BIN_DIR:-/opt/kafka/bin}"
RUNNER="$ROOT/matching/target/release/cex-runner"
BROKERS="${CEX_KAFKA_BROKERS:-localhost:9092}"
SYMBOL="btc-usdt"
HTTP="http://localhost:8080"

log() { echo "[e2e-web] $*"; }
fail() { echo "[e2e-web] FAIL: $*" >&2; exit 1; }
psql() { docker exec -i "$PG_CT" psql -U cex -d cex -v ON_ERROR_STOP=1 "$@"; }
q()    { docker exec "$PG_CT" psql -U cex -d cex -tAc "$1"; }
kc()   { timeout 20 docker exec -i "$KC_CT" "$KBIN/$(basename "$1")" --bootstrap-server "$BROKERS" "${@:2}"; }

command -v go >/dev/null || fail "需要 Go 工具链"
[ -x "$ROOT/matching/target/release/cex-runner" ] || fail "先构建 runner"
docker exec "$PG_CT" true 2>/dev/null || fail "postgres 容器未运行"
docker exec "$KC_CT" true 2>/dev/null || fail "kafka 容器未运行"

log "构建全部服务"
(cd "$ROOT" && go build -o /tmp/cex-order ./services/order && go build -o /tmp/cex-clearing ./services/clearing \
  && go build -o /tmp/cex-market ./services/market && go build -o /tmp/cex-web ./apps/web)
bash "$ROOT/scripts/db-migrate.sh" >/dev/null

# 清场:杀残留 → 清 DB → 重建 topic → 重置消费组 → 等端口释放
pkill -9 -f "tmp/cex-order" 2>/dev/null || true
pkill -9 -f "tmp/cex-clearing" 2>/dev/null || true
pkill -9 -f "tmp/cex-market" 2>/dev/null || true
pkill -9 -f "tmp/cex-web" 2>/dev/null || true
pkill -9 -f "tmp/cex-runner" 2>/dev/null || true
sleep 0.5
for p in 8080 8081 8082; do
  for _ in $(seq 1 20); do
    curl -s -m 1 "http://localhost:$p/healthz" -o /dev/null 2>/dev/null || break
    sleep 0.5
  done
done
psql -c "TRUNCATE orders, journals, ledger_entries, accounts RESTART IDENTITY CASCADE" >/dev/null
for t in "cex.orders.in.$SYMBOL" "cex.events.$SYMBOL"; do
  kc kafka-topics.sh --delete --topic "$t" >/dev/null 2>&1 || true
done
for t in "cex.orders.in.$SYMBOL" "cex.events.$SYMBOL"; do
  for _ in $(seq 1 30); do
    kc kafka-topics.sh --describe --topic "$t" 2>/dev/null | grep -q "Topic: $t" || break
    sleep 0.5
  done
  kc kafka-topics.sh --create --if-not-exists --topic "$t" --partitions 1 >/dev/null
done
for g in "cex-matching-$SYMBOL" clearing order-status-sync market; do
  kc kafka-consumer-groups.sh --reset-offsets --group "$g" --all-topics --to-earliest --execute >/dev/null 2>&1 || true
done

# 种子充值
psql >/dev/null <<'EOF'
INSERT INTO accounts (owner_id, owner_type, currency, type) VALUES
  (0,'system','USDT','available'), (0,'system','BTC','available'),
  (101,'user','USDT','available'), (101,'user','USDT','frozen'), (101,'user','BTC','available'), (101,'user','BTC','frozen'),
  (102,'user','USDT','available'), (102,'user','USDT','frozen'), (102,'user','BTC','available'), (102,'user','BTC','frozen')
ON CONFLICT DO NOTHING;
DO $$
BEGIN
  IF NOT EXISTS (SELECT 1 FROM journals WHERE biz_id='dep-101-usdt') THEN
    INSERT INTO journals (biz_type, biz_id) VALUES ('deposit','dep-101-usdt');
    UPDATE accounts SET balance = balance + 1000000000000 WHERE (owner_id,owner_type) IN ((0,'system'),(101,'user')) AND currency='USDT' AND type='available';
    INSERT INTO ledger_entries (journal_id, account_id, direction, amount, currency, balance_after)
      SELECT j.id, a.id, CASE WHEN a.owner_type='system' THEN 'debit' ELSE 'credit' END, 1000000000000, 'USDT', a.balance
      FROM journals j JOIN accounts a ON (a.owner_id,a.owner_type) IN ((0,'system'),(101,'user')) AND a.currency='USDT' AND a.type='available' AND j.biz_id='dep-101-usdt';
  END IF;
  IF NOT EXISTS (SELECT 1 FROM journals WHERE biz_id='dep-101-btc') THEN
    INSERT INTO journals (biz_type, biz_id) VALUES ('deposit','dep-101-btc');
    UPDATE accounts SET balance = balance + 20000000 WHERE (owner_id,owner_type) IN ((0,'system'),(101,'user')) AND currency='BTC' AND type='available';
    INSERT INTO ledger_entries (journal_id, account_id, direction, amount, currency, balance_after)
      SELECT j.id, a.id, CASE WHEN a.owner_type='system' THEN 'debit' ELSE 'credit' END, 20000000, 'BTC', a.balance
      FROM journals j JOIN accounts a ON (a.owner_id,a.owner_type) IN ((0,'system'),(101,'user')) AND a.currency='BTC' AND a.type='available' AND j.biz_id='dep-101-btc';
  END IF;
  IF NOT EXISTS (SELECT 1 FROM journals WHERE biz_id='dep-102-usdt') THEN
    INSERT INTO journals (biz_type, biz_id) VALUES ('deposit','dep-102-usdt');
    UPDATE accounts SET balance = balance + 1000000000000 WHERE (owner_id,owner_type) IN ((0,'system'),(102,'user')) AND currency='USDT' AND type='available';
    INSERT INTO ledger_entries (journal_id, account_id, direction, amount, currency, balance_after)
      SELECT j.id, a.id, CASE WHEN a.owner_type='system' THEN 'debit' ELSE 'credit' END, 1000000000000, 'USDT', a.balance
      FROM journals j JOIN accounts a ON (a.owner_id,a.owner_type) IN ((0,'system'),(102,'user')) AND a.currency='USDT' AND a.type='available' AND j.biz_id='dep-102-usdt';
  END IF;
  IF NOT EXISTS (SELECT 1 FROM journals WHERE biz_id='dep-102-btc') THEN
    INSERT INTO journals (biz_type, biz_id) VALUES ('deposit','dep-102-btc');
    UPDATE accounts SET balance = balance + 20000000 WHERE (owner_id,owner_type) IN ((0,'system'),(102,'user')) AND currency='BTC' AND type='available';
    INSERT INTO ledger_entries (journal_id, account_id, direction, amount, currency, balance_after)
      SELECT j.id, a.id, CASE WHEN a.owner_type='system' THEN 'debit' ELSE 'credit' END, 20000000, 'BTC', a.balance
      FROM journals j JOIN accounts a ON (a.owner_id,a.owner_type) IN ((0,'system'),(102,'user')) AND a.currency='BTC' AND a.type='available' AND j.biz_id='dep-102-btc';
  END IF;
END $$;
EOF

log "启动 order / clearing / runner / market / web"
CEX_KAFKA_BROKERS="$BROKERS" CEX_MARKETS_FILE="$ROOT/packages/api-spec/markets.yaml" /tmp/cex-clearing >/tmp/cex-clearing.log 2>&1 & P1=$!
CEX_KAFKA_BROKERS="$BROKERS" CEX_MARKETS_FILE="$ROOT/packages/api-spec/markets.yaml" /tmp/cex-order   >/tmp/cex-order.log    2>&1 & P2=$!
CEX_KAFKA_BROKERS="$BROKERS" "$RUNNER" >/tmp/cex-runner-t.log 2>&1 & P3=$!
CEX_KAFKA_BROKERS="$BROKERS" /tmp/cex-market  >/tmp/cex-market.log 2>&1 & P4=$!
CEX_KAFKA_BROKERS="$BROKERS" CEX_WEB_STATIC="$ROOT/apps/web/public" /tmp/cex-web >/tmp/cex-web.log 2>&1 & P5=$!
trap 'kill $P1 $P2 $P3 $P4 $P5 2>/dev/null || true; sleep 1.5; kill -9 $P1 $P2 $P3 $P4 $P5 2>/dev/null || true' EXIT


wait_group_member() { # 等待消费组成员就绪(rebalance 完成)
  for _ in $(seq 1 40); do
    M=$(kc kafka-consumer-groups.sh --describe --group "$1" 2>/dev/null | awk 'NR>1 && $7!="-" && $7!=""' | wc -l)
    [ "$M" != "0" ] && return 0
    sleep 0.5
  done
  return 1
}

for i in $(seq 1 20); do
  curl -sf "$HTTP/healthz" >/dev/null 2>&1 && break
  [ "$i" = 20 ] && fail "网关未就绪,日志见 /tmp/cex-web.log"
  sleep 0.5
done

wait_group_member "cex-matching-$SYMBOL" || fail "runner 消费组未就绪"
wait_group_member "clearing" || fail "clearing 消费组未就绪"
wait_group_member "order-status-sync" || fail "order-status-sync 消费组未就绪"
wait_group_member "market" || fail "market 消费组未就绪"

# 1. 静态页
log "静态页断言"
PAGE=$(curl -sf "$HTTP/" )
echo "$PAGE" | grep -q "LCEX" || fail "页面缺少品牌标识"
echo "$PAGE" | grep -q "order book\|订单簿" || fail "页面缺少订单簿"
curl -sf "$HTTP/app.js" | grep -q "X-User-Id" || fail "app.js 未正确服务"
log "静态页 ok"

place() { curl -sf -X POST "$HTTP/api/v1/orders" -H "X-User-Id: $1" -H "Content-Type: application/json" \
  -d '{"symbol":"BTC-USDT","clientOrderId":"'"$2"'","side":"'"$3"'","type":"LIMIT","timeInForce":"GTC","price":"'"$4"'","qty":"'"$5"'"}'; }

# 2. WS 经反代
log "WS 反代断言"
go run "$ROOT/scripts/wsprobe" -url "ws://localhost:8080/stream" -sub "ticker@BTC-USDT" -dur 3s >/tmp/e2e-web-ws.jsonl 2>&1 &
sleep 1.5
place 101 web-1 bid 50000 0.1 >/dev/null || true
sleep 3
place 101 web-2 bid 45000 0.05 >/dev/null || true
sleep 3
grep -q '"channel":"ticker"' /tmp/e2e-web-ws.jsonl || fail "WS 反代未收到 ticker 推送"
log "WS 反代 ok"

# 3. 下单/撤单走网关(含 X-User-Id 透传)
log "网关交易链路断言"
R=$(place 101 web-2 bid 45000 0.05)
echo "$R" | grep -q '"code":0' || fail "经网关下单失败: $R"
ID=$(echo "$R" | python3 -c "import json,sys; print(json.load(sys.stdin)['data']['orderId'])")
for i in $(seq 1 20); do
  [ "$(q "SELECT status FROM orders WHERE order_id=$ID")" = "open" ] && break
  [ "$i" = 20 ] && fail "订单未 open"
  sleep 0.5
done
BAL=$(curl -sf "$HTTP/api/v1/account/balances" -H "X-User-Id: 101")
echo "$BAL" | grep -q '"USDT"' || fail "经网关查余额失败"
curl -sf -X DELETE "$HTTP/api/v1/orders/$ID" -H "X-User-Id: 101" >/dev/null || fail "经网关撤单失败"
for i in $(seq 1 20); do
  [ "$(q "SELECT status FROM orders WHERE order_id=$ID")" = "canceled" ] && break
  [ "$i" = 20 ] && fail "未撤销"
  sleep 0.5
done

DEPTH=$(curl -sf "$HTTP/api/v1/depth?symbol=BTC-USDT")
echo "$DEPTH" | grep -q '"symbol":"BTC-USDT"' || fail "经网关查深度失败"

log "PASS ✔  静态页 / API 反代 / WS 反代 / 网关交易链路 全部通过"
