#!/usr/bin/env bash
# 撮合链路端到端验收:真实 Kafka + cex-runner
#
# 流程:重置 topic → 启动 runner → 投递 4 条指令
#       (挂单 / 吃单 / IOC 吃单 / Post-Only 拒绝)
#       → 消费输出 → 断言事件数、成交价格数量、订单状态机、seq 连续性
#
# 前置:
#   docker compose -f infra/docker-compose.yml up -d kafka
#   (cd matching && cargo build --release -p cex-runner)
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
BROKERS="${CEX_KAFKA_BROKERS:-localhost:9092}"
RUNNER="$ROOT/matching/target/release/cex-runner"
CT="${KAFKA_CONTAINER:-cex-dev-kafka-1}"
KBIN="${KAFKA_BIN_DIR:-/opt/kafka/bin}" # apache 官方镜像;bitnami 为 /opt/bitnami/kafka/bin
SYMBOL="btc-usdt"
T_IN="cex.orders.in.$SYMBOL"
T_TR="cex.trades.$SYMBOL"
T_OE="cex.order-events.$SYMBOL"

log() { echo "[e2e] $*"; }
fail() { echo "[e2e] FAIL: $*" >&2; exit 1; }

command -v docker >/dev/null || fail "需要 docker"
[ -x "$RUNNER" ] || fail "先构建: (cd matching && cargo build --release -p cex-runner)"
docker exec "$CT" true 2>/dev/null || fail "kafka 容器未运行: docker compose -f infra/docker-compose.yml up -d kafka"

kc() { docker exec -i "$CT" "$KBIN/$(basename "$1")" --bootstrap-server "$BROKERS" "${@:2}"; }

# 1. 清理旧状态(删除 topic 重建,保证从零开始)
log "重置 topic 与消费组 offset"
kc kafka-topics.sh --delete --topic "$T_IN" >/dev/null 2>&1 || true
kc kafka-topics.sh --delete --topic "$T_TR" >/dev/null 2>&1 || true
kc kafka-topics.sh --delete --topic "$T_OE" >/dev/null 2>&1 || true
sleep 5
for t in "$T_IN" "$T_TR" "$T_OE"; do
  kc kafka-topics.sh --create --if-not-exists --topic "$t" --partitions 1 >/dev/null
done
kc kafka-consumer-groups.sh --reset-offsets --group "cex-matching-$SYMBOL" --all-topics --to-earliest --execute >/dev/null 2>&1 || true

# 2. 启动 runner
log "启动 cex-runner"
CEX_KAFKA_BROKERS="$BROKERS" "$RUNNER" >/tmp/cex-runner-e2e.log 2>&1 &
RUNNER_PID=$!
trap 'kill "$RUNNER_PID" 2>/dev/null || true' EXIT
sleep 4
kill -0 "$RUNNER_PID" 2>/dev/null || fail "runner 启动失败,日志见 /tmp/cex-runner-e2e.log"

# 3. 投递指令(定点整数 ×1e8:50000 USDT = 5e12;1 BTC = 1e8;0.5 = 5e7)
#    覆盖:挂单 → 吃单(部分成交 maker)→ IOC 吃完 → Post-Only 空盘挂入 → 撤单
#          → 挂单 → Post-Only 越价拒绝
#    注意:必须先撤销 60000 的挂入买单,否则后续卖单会被它吃掉
log "投递测试指令"
P=5000000000000
cat <<EOF | kc kafka-console-producer.sh --topic "$T_IN" >/dev/null
{"type":"place","order_id":1,"user_id":1,"side":"ask","order_type":"limit","tif":"gtc","stp":"none","post_only":false,"price":$P,"qty":100000000}
{"type":"place","order_id":2,"user_id":2,"side":"bid","order_type":"limit","tif":"gtc","stp":"none","post_only":false,"price":$P,"qty":50000000}
{"type":"place","order_id":3,"user_id":2,"side":"bid","order_type":"limit","tif":"ioc","stp":"none","post_only":false,"price":$P,"qty":50000000}
{"type":"place","order_id":4,"user_id":2,"side":"bid","order_type":"limit","tif":"gtc","stp":"none","post_only":true,"price":6000000000000,"qty":10000000}
{"type":"cancel","order_id":4,"user_id":2}
{"type":"place","order_id":5,"user_id":1,"side":"ask","order_type":"limit","tif":"gtc","stp":"none","post_only":false,"price":$P,"qty":10000000}
{"type":"place","order_id":6,"user_id":2,"side":"bid","order_type":"limit","tif":"gtc","stp":"none","post_only":true,"price":6000000000000,"qty":10000000}
EOF

# 4. 消费输出(trades 2 条 / order-events 9 条,超时即失败)
log "消费输出事件"
kc kafka-console-consumer.sh --topic "$T_TR" --from-beginning --max-messages 2 --timeout-ms 20000 >/tmp/e2e-trades.jsonl 2>/dev/null || true
kc kafka-console-consumer.sh --topic "$T_OE" --from-beginning --max-messages 9 --timeout-ms 20000 >/tmp/e2e-orders.jsonl 2>/dev/null || true

# 5. 断言
assert_count() { [ "$(wc -l < "$1")" -eq "$2" ] || fail "$1 期望 $2 条,实际 $(wc -l < "$1")"; }
assert_count /tmp/e2e-trades.jsonl 2
assert_count /tmp/e2e-orders.jsonl 9
grep -q "\"price\":$P" /tmp/e2e-trades.jsonl || fail "成交价格不符"
grep -q '"qty":50000000' /tmp/e2e-trades.jsonl || fail "成交数量不符(应为 0.5 BTC × 2 笔)"
for s in open partially_filled filled canceled rejected; do
  grep -q "\"status\":\"$s\"" /tmp/e2e-orders.jsonl || fail "order-events 缺少状态: $s"
done
# seq 跨 topic 连续无空洞:1..11
SEQS=$( { grep -oh '"seq":[0-9]*' /tmp/e2e-trades.jsonl /tmp/e2e-orders.jsonl | cut -d: -f2; } | sort -n | tr '\n' ' ' | sed 's/ $//')
[ "$SEQS" = "1 2 3 4 5 6 7 8 9 10 11" ] || fail "seq 不连续: $SEQS"

log "PASS ✔  trades=2, order-events=9, seq 1..11 连续,状态机完整"
