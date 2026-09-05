#!/usr/bin/env bash
# 行情服务验收(L4 扩展):交易闭环跑通后,校验 REST 快照与 WebSocket 推送。
#
# 场景:复用 e2e-trading 的种子与交易流,额外:
#   - 挂入 102 限价买 40000×0.05 → REST depth 显示该档;WS depth 收到 update
#   - ticker: last=50000, volume24h=0.2, bid=40000
#   - trades: 2 笔,taker 方向 ask
#   - klines 1m: 单根蜡烛 o=h=l=c=50000, vol=0.2
#   - 撤单 → depth 档位清空(bids 里 40000 数量为 0)
#
# 前置:postgres/kafka 容器运行;runner 二进制已构建;Go 工具链可用。
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
export PATH="$HOME/go-sdk/go/bin:$PATH"
PG_CT="${PG_CONTAINER:-cex-dev-postgres-1}"
KC_CT="${KAFKA_CONTAINER:-cex-dev-kafka-1}"
KBIN="${KAFKA_BIN_DIR:-/opt/kafka/bin}"
BROKERS="${CEX_KAFKA_BROKERS:-localhost:9092}"
SYMBOL="btc-usdt"

ORDER_BIN=/tmp/cex-order
CLEARING_BIN=/tmp/cex-clearing
MARKET_BIN=/tmp/cex-market
RUNNER="$ROOT/matching/target/release/cex-runner"
HTTP_O="http://localhost:8081"
HTTP_M="http://localhost:8082"

log() { echo "[e2e-mkt] $*"; }
fail() { echo "[e2e-mkt] FAIL: $*" >&2; exit 1; }

wait_port_free() { # 上轮服务优雅退出需要几秒,端口释放后再启动
  for _ in $(seq 1 30); do
    curl -s -m 1 "http://localhost:$1/healthz" -o /dev/null 2>/dev/null || return 0
    sleep 0.5
  done
  return 1
}

wait_group_member() { # 等待消费组成员就绪(rebalance 完成,陈旧成员被踢)
  for _ in $(seq 1 40); do
    M=$(kc kafka-consumer-groups.sh --describe --group "$1" 2>/dev/null | awk 'NR>1 && $7!="-" && $7!=""' | wc -l)
    [ "$M" != "0" ] && return 0
    sleep 0.5
  done
  return 1
}
psql() { docker exec -i "$PG_CT" psql -U cex -d cex -v ON_ERROR_STOP=1 "$@"; }
q()    { docker exec "$PG_CT" psql -U cex -d cex -tAc "$1"; }
kc()   { timeout 20 docker exec -i "$KC_CT" "$KBIN/$(basename "$1")" --bootstrap-server "$BROKERS" "${@:2}"; }

command -v go >/dev/null || fail "需要 Go 工具链(~/go-sdk)"
[ -x "$RUNNER" ] || fail "先构建 runner: (cd matching && cargo build --release -p cex-runner)"
docker exec "$PG_CT" true 2>/dev/null || fail "postgres 容器未运行"
docker exec "$KC_CT" true 2>/dev/null || fail "kafka 容器未运行"

# 0. 构建
log "构建服务"
(cd "$ROOT" && go build -o "$ORDER_BIN" ./services/order && go build -o "$CLEARING_BIN" ./services/clearing && go build -o "$MARKET_BIN" ./services/market)
bash "$ROOT/scripts/db-migrate.sh" >/dev/null

# 1. 清场(pkill 残留进程,清 DB,重建 topic,重置消费组)
pkill -9 -f cex-clearing 2>/dev/null || true
pkill -9 -f cex-order 2>/dev/null || true
pkill -9 -f cex-market 2>/dev/null || true
pkill -9 -f cex-runner 2>/dev/null || true
sleep 0.5
wait_port_free 8081 || true
wait_port_free 8082 || true


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

# 2. 种子充值
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
  IF NOT EXISTS (SELECT 1 FROM journals WHERE biz_id='dep-102-usdt') THEN
    INSERT INTO journals (biz_type, biz_id) VALUES ('deposit','dep-102-usdt');
    UPDATE accounts SET balance = balance + 1000000000000 WHERE (owner_id,owner_type) IN ((0,'system'),(102,'user')) AND currency='USDT' AND type='available';
    INSERT INTO ledger_entries (journal_id, account_id, direction, amount, currency, balance_after)
      SELECT j.id, a.id, CASE WHEN a.owner_type='system' THEN 'debit' ELSE 'credit' END, 1000000000000, 'USDT', a.balance
      FROM journals j JOIN accounts a ON (a.owner_id,a.owner_type) IN ((0,'system'),(102,'user')) AND a.currency='USDT' AND a.type='available' AND j.biz_id='dep-102-usdt';
  END IF;
  IF NOT EXISTS (SELECT 1 FROM journals WHERE biz_id='dep-101-btc') THEN
    INSERT INTO journals (biz_type, biz_id) VALUES ('deposit','dep-101-btc');
    UPDATE accounts SET balance = balance + 20000000 WHERE (owner_id,owner_type) IN ((0,'system'),(101,'user')) AND currency='BTC' AND type='available';
    INSERT INTO ledger_entries (journal_id, account_id, direction, amount, currency, balance_after)
      SELECT j.id, a.id, CASE WHEN a.owner_type='system' THEN 'debit' ELSE 'credit' END, 20000000, 'BTC', a.balance
      FROM journals j JOIN accounts a ON (a.owner_id,a.owner_type) IN ((0,'system'),(101,'user')) AND a.currency='BTC' AND a.type='available' AND j.biz_id='dep-101-btc';
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

# 3. 启动服务(含 market)


log "启动 clearing / order / runner / market"
CEX_KAFKA_BROKERS="$BROKERS" CEX_MARKETS_FILE="$ROOT/packages/api-spec/markets.yaml" "$CLEARING_BIN" >/tmp/cex-clearing.log 2>&1 & P1=$!
CEX_KAFKA_BROKERS="$BROKERS" CEX_MARKETS_FILE="$ROOT/packages/api-spec/markets.yaml" "$ORDER_BIN"   >/tmp/cex-order.log    2>&1 & P2=$!
CEX_KAFKA_BROKERS="$BROKERS" "$RUNNER" >/tmp/cex-runner-t.log 2>&1 & P3=$!
CEX_KAFKA_BROKERS="$BROKERS" CEX_MARKETS_FILE="$ROOT/packages/api-spec/markets.yaml" CEX_WS_PING_SECONDS=3 "$MARKET_BIN" >/tmp/cex-market.log 2>&1 & P4=$!
trap 'kill $P1 $P2 $P3 $P4 2>/dev/null || true; sleep 1.5; kill -9 $P1 $P2 $P3 $P4 2>/dev/null || true' EXIT

for i in $(seq 1 20); do
  curl -sf "$HTTP_O/healthz" >/dev/null 2>&1 && curl -sf "$HTTP_M/healthz" >/dev/null 2>&1 && break
  [ "$i" = 20 ] && fail "服务未就绪(order: /tmp/cex-order.log, market: /tmp/cex-market.log)"
  sleep 0.5
done

place() { curl -sf -X POST "$HTTP_O/api/v1/orders" -H "X-User-Id: $1" -H "Content-Type: application/json" \
  -d "{\"symbol\":\"BTC-USDT\",\"clientOrderId\":\"$2\",\"side\":\"$3\",\"type\":\"LIMIT\",\"timeInForce\":\"GTC\",\"price\":\"$4\",\"qty\":\"$5\"}"; }

# 4. 先订阅 WS 再驱动交易(保证收到全部推送)
log "启动 WS 探针(ticker/depth/trades/kline)"
go run "$ROOT/scripts/wsprobe" -url "ws://localhost:8082/stream" \
  -sub "ticker@BTC-USDT,depth@BTC-USDT@50,trades@BTC-USDT,kline@BTC-USDT@1m" -dur 14s >/tmp/e2e-ws.jsonl 2>&1 &
WSPID=$!

sleep 1.5
# 5. 交易流:101 买 102 卖,然后 102 挂 40000 买单并撤销
place 101 e2e-1 bid 50000 0.1 >/dev/null || fail "101 下单失败"
place 102 e2e-2 ask 50000 0.1 >/dev/null || fail "102 下单失败"
place 102 e2e-3 bid 40000 0.05 >/dev/null || fail "102 挂单失败"
ID3=$(q "SELECT order_id FROM orders WHERE client_order_id='e2e-3'")

# 等待主成交 + 挂单进入盘口
wait_group_member "cex-matching-$SYMBOL" || fail "runner 消费组未就绪"
wait_group_member "clearing" || fail "clearing 消费组未就绪"
wait_group_member "order-status-sync" || fail "order-status-sync 消费组未就绪"
wait_group_member "market" || fail "market 消费组未就绪"
for i in $(seq 1 30); do
  N=$(q "SELECT count(*) FROM orders WHERE client_order_id IN ('e2e-1','e2e-2') AND status='filled'")
  S=$(q "SELECT status FROM orders WHERE order_id=$ID3")
  [ "$N" = "2" ] && [ "$S" = "open" ] && break
  [ "$i" = 30 ] && fail "订单未达预期状态: N=$N S=$S"
  sleep 0.5
done

# 6. REST 断言
log "REST 断言"
curl -sf "$HTTP_M/api/v1/depth?symbol=BTC-USDT&limit=10" >/tmp/e2e-depth.json || fail "depth 请求失败"
python3 - <<'PY' || fail "depth 断言失败"
import json, sys
raw = json.load(open("/tmp/e2e-depth.json"))
assert raw.get("code") == 0, raw
d = raw["data"]
assert d["bids"] == [["40000", "0.05"]], d["bids"]
assert d["asks"] == [], d["asks"]
assert d["seq"] > 0
print("[e2e-mkt] depth ok:", d["bids"])
PY

curl -sf "$HTTP_M/api/v1/tickers?symbol=BTC-USDT" >/tmp/e2e-ticker.json || fail "ticker 请求失败"
python3 - <<'PY' || fail "ticker 断言失败"
import json, sys
raw = json.load(open("/tmp/e2e-ticker.json"))
assert raw.get("code") == 0, raw
t = raw["data"][0]
assert t["last"] == "50000", t
assert t["bid"] == "40000", t
assert t["volume24h"] == "0.1", t
assert t["high24h"] == "50000" and t["low24h"] == "50000", t
print("[e2e-mkt] ticker ok: last=%s vol=%s" % (t["last"], t["volume24h"]))
PY

curl -sf "$HTTP_M/api/v1/trades?symbol=BTC-USDT" >/tmp/e2e-trades.json || fail "trades 请求失败"
python3 - <<'PY' || fail "trades 断言失败"
import json, sys
raw = json.load(open("/tmp/e2e-trades.json"))
assert raw.get("code") == 0, raw
ts = raw["data"]
assert len(ts) == 1, len(ts)
assert all(t["price"] == "50000" and t["qty"] == "0.1" for t in ts)
assert all(t["side"] == "ask" for t in ts), "taker 应为卖方 102"
print("[e2e-mkt] trades ok: %d 笔" % len(ts))
PY

curl -sf "$HTTP_M/api/v1/klines?symbol=BTC-USDT&interval=1m&limit=5" >/tmp/e2e-klines.json || fail "klines 请求失败"
python3 - <<'PY' || fail "klines 断言失败"
import json, sys
raw = json.load(open("/tmp/e2e-klines.json"))
assert raw.get("code") == 0, raw
ks = raw["data"]
assert len(ks) == 1, ks
k = ks[0]
assert k[1] == k[2] == k[3] == k[4] == "50000", k
assert k[5] == "0.1", k
print("[e2e-mkt] klines ok: %d 根蜡烛 vol=%s" % (len(ks), ks[0][5]))
PY

# 7. 撤单 → WS 应收到深度档清空;REST depth 归零
curl -sf -X DELETE "$HTTP_O/api/v1/orders/$ID3" -H "X-User-Id: 102" >/dev/null || fail "撤单失败"
for i in $(seq 1 20); do
  [ "$(q "SELECT status FROM orders WHERE order_id=$ID3")" = "canceled" ] && break
  [ "$i" = 20 ] && fail "e2e-3 未撤销"
  sleep 0.5
done
sleep 1
BIDS=$(curl -sf "$HTTP_M/api/v1/depth?symbol=BTC-USDT" | python3 -c "import json,sys; print(json.load(sys.stdin)['data']['bids'])")
[ "$BIDS" = "[]" ] || fail "撤单后 bids 应为空: $BIDS"
log "撤单后盘口清空 ok"

# 8. WS 断言
log "WS 断言"
kill "$WSPID" 2>/dev/null || true
sleep 0.5
python3 - <<'PY' || fail "WS 断言失败"
import json

msgs = []
for line in open("/tmp/e2e-ws.jsonl"):
    line = line.strip()
    if not line:
        continue
    try:
        msgs.append(json.loads(line))
    except json.JSONDecodeError:
        pass  # probe 的非 JSON 行

def ch(m, name):
    return m.get("channel") == name

subs = [m for m in msgs if m.get("event") == "subscribe"]
assert subs, "缺少订阅确认"
depths = [m for m in msgs if ch(m, "depth") and m.get("type") == "snapshot"]
assert depths, "缺少 depth 快照"
updates = [m for m in msgs if ch(m, "depth") and m.get("type") == "update"]
assert any(
    any(lv[0] == "40000" and lv[1] == "0.05" for lv in m["bids"]) for m in updates
), "缺少 40000 档新增推送: %s" % [m["bids"] for m in updates]
assert any(
    any(lv[0] == "40000" and lv[1] == "0" for lv in m["bids"]) for m in updates
), "缺少撤单后档位清空推送"
trades = [m for m in msgs if ch(m, "trades")]
assert len(trades) == 1, "WS trades 应 1 条,实际 %d" % len(trades)
tickers = [m for m in msgs if ch(m, "ticker")]
assert tickers and tickers[-1]["data"]["last"] == "50000"
klines = [m for m in msgs if ch(m, "kline")]
assert klines and klines[-1]["data"]["volume"] == "0.1"
pings = [m for m in msgs if m.get("channel") == "ping"]
assert pings, "缺少服务端心跳"
print("[e2e-mkt] ws ok: snapshot/update/trades/ticker/kline/ping 全部收到")
PY

log "PASS ✔  行情 REST + WebSocket 全部断言通过"
