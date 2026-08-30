#!/usr/bin/env bash
# price-index 验收:mock 交易所(无外网依赖)→ 适配器 → 聚合 → HTTP/Redis/Kafka。
#
# 场景:
#   1. mockex 推 bid=50000 / ask=50001 → binance 适配器解析出中间价 50000.5;
#   2. HTTP /index/BTC-USDT 返回 index=50000.5,binance usable;
#   3. Redis cex:index:BTC-USDT 存在且含该价;
#   4. Kafka cex.index.btc-usdt 收到快照;
#   5. 改价文件 → 50010/50011 → 指数随动(验证聚合循环)。
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
export PATH="$HOME/go-sdk/go/bin:$PATH"
KC_CT="${KAFKA_CONTAINER:-cex-dev-kafka-1}"
KBIN="${KAFKA_BIN_DIR:-/opt/kafka/bin}"
BROKERS="${CEX_KAFKA_BROKERS:-localhost:9092}"
REDIS_CT="${REDIS_CONTAINER:-cex-dev-redis-1}"

PX_BIN=/tmp/cex-price-index
MOCK_BIN=/tmp/cex-mockex

log() { echo "[e2e-px] $*"; }
fail() { echo "[e2e-px] FAIL: $*" >&2; exit 1; }
kc()   { timeout 20 docker exec -i "$KC_CT" "$KBIN/$(basename "$1")" --bootstrap-server "$BROKERS" "${@:2}"; }

command -v go >/dev/null || fail "需要 Go 工具链"
docker exec "$KC_CT" true 2>/dev/null || fail "kafka 容器未运行"
docker exec "$REDIS_CT" redis-cli ping 2>/dev/null | grep -q PONG || fail "redis 容器未运行"

# 0. 构建与清场
log "构建"
(cd "$ROOT" && go build -o "$PX_BIN" ./services/price-index && go build -o "$MOCK_BIN" ./scripts/mockex)
pkill -9 -f "tmp/cex-price-index" 2>/dev/null || true
pkill -9 -f "tmp/cex-mockex" 2>/dev/null || true
sleep 0.5
# index topic 只承载瞬态快照,无需清场(避免 KRaft 删除-重建竞态);
# 断言只看消息内容,历史快照同为 50000.5 不影响结论
kc kafka-topics.sh --create --if-not-exists --topic "cex.index.btc-usdt" --partitions 1 >/dev/null 2>&1 || true

# 1. 启动 mock 交易所 + price-index(仅 binance 源,URL 指向 mock)
log "启动 mockex / price-index"
"$MOCK_BIN" -addr :9999 -stream "btcusdt@bookTicker" -bid 50000 -ask 50001 -file /tmp/mockex-px.txt >/tmp/mockex.log 2>&1 & M=$!
CEX_KAFKA_BROKERS="$BROKERS" CEX_SYMBOLS="BTC-USDT" CEX_PX_SOURCES=binance \
  CEX_PX_BINANCE_URL="ws://localhost:9999/ws/btcusdt" CEX_PX_STALE_MS=3000 \
  "$PX_BIN" >/tmp/cex-price-index.log 2>&1 & P=$!
trap 'kill $M $P 2>/dev/null || true; sleep 1; kill -9 $M $P 2>/dev/null || true' EXIT
echo "50000 50001" > /tmp/mockex-px.txt

# 2. 等 index 出现
log "等待指数生成"
for i in $(seq 1 30); do
  PX=$(curl -s -m 2 "http://localhost:8083/index/BTC-USDT" | python3 -c "import json,sys; d=json.load(sys.stdin); print(d['index'], d['ok'], d.get('sources',{}).get('binance',{}).get('status',''))" 2>/dev/null || echo "")
  [ "$(echo "$PX" | cut -d' ' -f2)" = "True" ] && [ "$(echo "$PX" | cut -d' ' -f3)" = "usable" ] && break
  [ "$i" = 30 ] && fail "指数未生成,最后状态: $PX"
  sleep 0.5
done
log "index = $PX"
INDEX1=$(echo "$PX" | cut -d' ' -f1)
[ "$INDEX1" = "50000.5" ] || fail "指数应为 50000.5((50000+50001)/2),实际 $INDEX1"

# 3. Redis 断言
log "Redis 断言"
docker exec "$REDIS_CT" redis-cli GET "cex:index:BTC-USDT" >/tmp/e2e-px-redis.json
python3 - <<'PY' || fail "Redis 断言失败"
import json, sys
d = json.load(open("/tmp/e2e-px-redis.json"))
assert d["index"] == "50000.5", d
assert d["ok"] is True, d
assert d["sources"]["binance"]["status"] == "usable", d
print("[e2e-px] redis ok:", d["index"])
PY

# 4. Kafka 断言
log "Kafka 断言"
# topic 不清场(避免 KRaft 删建竞态);从 latest 开始只收本轮新快照
kc kafka-console-consumer.sh --topic "cex.index.btc-usdt" --partition 0 --offset latest --max-messages 3 --timeout-ms 10000 2>/dev/null >/tmp/e2e-px-kafka.jsonl || true
python3 - <<'PY' || fail "Kafka 断言失败"
import json
rows = []
for line in open("/tmp/e2e-px-kafka.jsonl"):
    line = line.strip()
    if line:
        try:
            rows.append(json.loads(line))
        except json.JSONDecodeError:
            pass
assert rows, "kafka 未收到新快照"
assert all(r["index"] == "50000.5" for r in rows), rows
assert all(r["sources"]["binance"]["status"] == "usable" for r in rows), rows
print("[e2e-px] kafka ok: %d 条新快照 index=50000.5" % len(rows))
PY

# 5. 改价 → 指数随动
log "改价 50010/50011,等待指数随动"
echo "50010 50011" > /tmp/mockex-px.txt
for i in $(seq 1 20); do
  PX2=$(curl -s -m 2 "http://localhost:8083/index/BTC-USDT" | python3 -c "import json,sys; print(json.load(sys.stdin)['index'])" 2>/dev/null || echo "")
  [ "$PX2" = "50010.5" ] && break
  [ "$i" = 20 ] && fail "指数未随动,实际: $PX2"
  sleep 0.5
done
log "指数随动 ok: $PX2"

log "PASS ✔  指数价生成 / HTTP / Redis / Kafka / 随动 全部通过"
