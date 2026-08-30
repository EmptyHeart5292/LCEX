#!/usr/bin/env bash
# 撮合链路端到端验收:真实 Kafka + cex-runner
#
# 流程:重置 topic → 启动 runner → 投递 7 条指令
#       (挂单 / 吃单 / IOC 吃单 / Post-Only 空盘挂入 / 撤单 / 挂单 / Post-Only 越价拒绝)
#       → 消费单一 events topic → 断言事件数、成交价格数量、订单状态机、seq 连续性
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
T_EV="cex.events.$SYMBOL"

log() { echo "[e2e] $*"; }
fail() { echo "[e2e] FAIL: $*" >&2; exit 1; }

command -v docker >/dev/null || fail "需要 docker"
[ -x "$RUNNER" ] || fail "先构建: (cd matching && cargo build --release -p cex-runner)"
docker exec "$CT" true 2>/dev/null || fail "kafka 容器未运行: docker compose -f infra/docker-compose.yml up -d kafka"

kc() { timeout 20 docker exec -i "$CT" "$KBIN/$(basename "$1")" --bootstrap-server "$BROKERS" "${@:2}"; }

wait_topic_gone() { # topic 删除是异步的:必须等到真删除,否则 create 命中旧数据
  for _ in $(seq 1 30); do
    kc kafka-topics.sh --describe --topic "$1" 2>/dev/null | grep -q "Topic: $1" || return 0
    sleep 0.5
  done
  return 1
}

# 1. 清理旧状态(删除 topic 重建,保证从零开始)
log "重置 topic 与消费组 offset"
kc kafka-topics.sh --delete --topic "$T_IN" >/dev/null 2>&1 || true
kc kafka-topics.sh --delete --topic "$T_EV" >/dev/null 2>&1 || true
wait_topic_gone "$T_IN" || true
wait_topic_gone "$T_EV" || true
for t in "$T_IN" "$T_EV"; do
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

# 4. 消费单一 events topic(预期 11 条:2 trade + 9 order_update)
log "消费输出事件"
kc kafka-console-consumer.sh --topic "$T_EV" --from-beginning --max-messages 11 --timeout-ms 20000 >/tmp/e2e-events.jsonl 2>/dev/null || true

# 5. 断言
python3 - <<'PYEOF' || fail "事件断言未通过"
import json, sys

events = [json.loads(l) for l in open("/tmp/e2e-events.jsonl") if l.strip()]
assert len(events) == 11, f"期望 11 条事件,实际 {len(events)}"
seqs = sorted(e["seq"] for e in events)
assert seqs == list(range(1, 12)), f"seq 不连续: {seqs}"

trades = [e["data"] for e in events if e["kind"] == "trade"]
updates = [e["data"] for e in events if e["kind"] == "order_update"]
assert len(trades) == 2, f"成交应 2 笔,实际 {len(trades)}"
assert len(updates) == 9, f"订单更新应 9 条,实际 {len(updates)}"

for t in trades:
    assert t["price"] == 5000000000000, t
    assert t["qty"] == 50000000, t
    assert t["maker_order_id"] == 1, "两笔都应以订单 1 为 maker"

statuses = {u["status"] for u in updates}
assert {"open", "partially_filled", "filled", "canceled", "rejected"} <= statuses, statuses

# 关键顺序断言:成交(seq 2,5)必须先于对应订单的终态更新 —— 清算保序的前提
by_seq = {e["seq"]: e for e in events}
filled_1 = min(u for u in updates if u["order_id"] == 1 and u["status"] == "filled")
seq_filled_1 = next(e["seq"] for e in events
                    if e["kind"] == "order_update" and e["data"]["order_id"] == 1
                    and e["data"]["status"] == "filled")
assert seq_filled_1 > 5, "订单 1 的终态更新必须晚于第二笔成交(seq 5)"

print("[e2e] 事件断言通过: trades=2, updates=9, seq 1..11 连续")
PYEOF

log "PASS ✔"
