#!/usr/bin/env bash
# 全链路资金正确性验收(L4):REST 下单 → 冻结 → 撮合 → 清算 → 余额/账本校验
#
# 场景:
#   101 限价买 0.1 BTC @ 50000(冻结 5005 USDT = 5000 + taker 费 5)
#   102 限价卖 0.1 BTC @ 50000(冻结 0.1 BTC)
#   → 成交:taker=102(ask),maker=101(bid);手续费双方各 5 USDT
#   102 追加市价外限价买单 40000 × 0.05 后撤销 → 验证撤单解冻
#
# 断言:双方余额精确、fee 账户入账、orders 状态/体积、reserved 归零、
#       账实不符视图 0 行、clientOrderId 幂等、余额不足拒绝、市价买单拒绝。
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
RUNNER="$ROOT/matching/target/release/cex-runner"
HTTP="http://localhost:8081"

log() { echo "[e2e-t] $*"; }
fail() { echo "[e2e-t] FAIL: $*" >&2; exit 1; }

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

wait_topic_gone() { # topic 删除是异步的:必须等到真删除,否则 create 命中旧数据
  for _ in $(seq 1 30); do
    kc kafka-topics.sh --describe --topic "$1" 2>/dev/null | grep -q "Topic: $1" || return 0
    sleep 0.5
  done
  return 1
}

command -v go >/dev/null || fail "需要 Go 工具链(~/go-sdk)"
[ -x "$RUNNER" ] || fail "先构建 runner: (cd matching && cargo build --release -p cex-runner)"
docker exec "$PG_CT" true 2>/dev/null || fail "postgres 容器未运行"
docker exec "$KC_CT" true 2>/dev/null || fail "kafka 容器未运行"

# 0. 构建 + 迁移
log "构建 Go 服务"
(cd "$ROOT" && go build -o "$ORDER_BIN" ./services/order && go build -o "$CLEARING_BIN" ./services/clearing)
wait_port_free 8081 || true
log "执行迁移"
bash "$ROOT/scripts/db-migrate.sh" >/dev/null

# 1. 重置测试状态(仅 dev 库:清空订单与账本,kafka topic 重建)
log "重置订单/账本/topic"


psql -c "TRUNCATE orders, journals, ledger_entries, accounts RESTART IDENTITY CASCADE" >/dev/null
for t in "cex.orders.in.$SYMBOL" "cex.events.$SYMBOL"; do
  kc kafka-topics.sh --delete --topic "$t" >/dev/null 2>&1 || true
done
for t in "cex.orders.in.$SYMBOL" "cex.events.$SYMBOL"; do
  wait_topic_gone "$t" || true
done
for t in "cex.orders.in.$SYMBOL" "cex.events.$SYMBOL"; do
  kc kafka-topics.sh --create --if-not-exists --topic "$t" --partitions 1 >/dev/null
done
# 重建 topic 后必须清掉三个消费组的陈旧 offset,否则会从旧位点开始整轮跳过
for g in "cex-matching-$SYMBOL" clearing order-status-sync; do
  kc kafka-consumer-groups.sh --reset-offsets --group "$g" --all-topics --to-earliest --execute >/dev/null 2>&1 || true
done

# 2. 种子:充值(热钱包与用户同时入账,保持会计恒等)
log "种子充值:101/102 各 10000 USDT + 0.2 BTC"
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

# 3. 启动服务
# 起服前确认消费组无残留成员(正常情况下上轮已优雅 LeaveGroup)
for g in clearing order-status-sync "cex-matching-$SYMBOL"; do
  for _ in $(seq 1 30); do
    L=$(kc kafka-consumer-groups.sh --describe --group "$g" 2>/dev/null | grep -c . || true)
    [ "$L" = "0" ] && break
    sleep 0.5
  done
done



log "启动 clearing / order / runner"
CEX_KAFKA_BROKERS="$BROKERS" CEX_MARKETS_FILE="$ROOT/packages/api-spec/markets.yaml" "$CLEARING_BIN" >/tmp/cex-clearing.log 2>&1 & P1=$!
CEX_KAFKA_BROKERS="$BROKERS" CEX_MARKETS_FILE="$ROOT/packages/api-spec/markets.yaml" "$ORDER_BIN"   >/tmp/cex-order.log    2>&1 & P2=$!
CEX_KAFKA_BROKERS="$BROKERS" "$RUNNER" >/tmp/cex-runner-t.log 2>&1 & P3=$!
trap 'kill $P1 $P2 $P3 2>/dev/null || true' EXIT

for i in $(seq 1 20); do
  curl -sf "$HTTP/healthz" >/dev/null 2>&1 && break
  [ "$i" = 20 ] && fail "order 服务未就绪,日志见 /tmp/cex-order.log"
  sleep 0.5
done

place() { # user clientOrderId side price qty
  curl -sf -X POST "$HTTP/api/v1/orders" \
    -H "X-User-Id: $1" -H "Content-Type: application/json" \
    -d "{\"symbol\":\"BTC-USDT\",\"clientOrderId\":\"$2\",\"side\":\"$3\",\"type\":\"LIMIT\",\"timeInForce\":\"GTC\",\"price\":\"$4\",\"qty\":\"$5\"}"
}

# 4. 下单
log "101 限价买 / 102 限价卖"
R1=$(place 101 e2e-1 bid 50000 0.1) || fail "101 下单失败"
R2=$(place 102 e2e-2 ask 50000 0.1) || fail "102 下单失败"
ID1=$(echo "$R1" | python3 -c "import json,sys; print(json.load(sys.stdin)['data']['orderId'])")
ID2=$(echo "$R2" | python3 -c "import json,sys; print(json.load(sys.stdin)['data']['orderId'])")
log "orderId: $ID1 / $ID2"

# 幂等探针:同 clientOrderId 重复提交 → 同一订单
RID=$(place 101 e2e-1 bid 50000 0.1 | python3 -c "import json,sys; print(json.load(sys.stdin)['data']['orderId'])")
[ "$RID" = "$ID1" ] || fail "clientOrderId 幂等失败: $RID != $ID1"

# 错误探针:余额不足(103 无资金)→ 51001;市价买 → 50012
CODE1=$(curl -s -X POST "$HTTP/api/v1/orders" -H "X-User-Id: 103" -H "Content-Type: application/json" \
  -d '{"symbol":"BTC-USDT","clientOrderId":"e2e-bad","side":"bid","type":"LIMIT","price":"50000","qty":"0.1"}' | python3 -c "import json,sys; print(json.load(sys.stdin)['code'])")
[ "$CODE1" = "51001" ] || fail "余额不足应返回 51001,实际 $CODE1"
CODE2=$(curl -s -X POST "$HTTP/api/v1/orders" -H "X-User-Id: 101" -H "Content-Type: application/json" \
  -d '{"symbol":"BTC-USDT","clientOrderId":"e2e-mbuy","side":"bid","type":"MARKET","qty":"0.1"}' | python3 -c "import json,sys; print(json.load(sys.stdin)['code'])")
[ "$CODE2" = "50012" ] || fail "市价买单应返回 50012,实际 $CODE2"

# 5. 撤单路径:102 限价买 40000 × 0.05(挂入后撤销)
log "102 限价买 40000 × 0.05 后撤销"
place 102 e2e-3 bid 40000 0.05 >/dev/null
ID3=$(q "SELECT order_id FROM orders WHERE client_order_id='e2e-3'")
for i in $(seq 1 20); do
  [ "$(q "SELECT status FROM orders WHERE order_id=$ID3")" = "open" ] && break
  [ "$i" = 20 ] && fail "e2e-3 未进入 open"
  sleep 0.5
done
curl -sf -X DELETE "$HTTP/api/v1/orders/$ID3" -H "X-User-Id: 102" >/dev/null || fail "撤单请求失败"

# 6. 等待主成交两单终态
log "等待成交结算"
wait_group_member "cex-matching-$SYMBOL" || fail "runner 消费组未就绪"
wait_group_member "clearing" || fail "clearing 消费组未就绪"
wait_group_member "order-status-sync" || fail "order-status-sync 消费组未就绪"
for i in $(seq 1 40); do
  N=$(q "SELECT count(*) FROM orders WHERE order_id IN ($ID1,$ID2) AND status='filled'")
  C=$(q "SELECT status FROM orders WHERE order_id=$ID3")
  [ "$N" = "2" ] && [ "$C" = "canceled" ] && break
  [ "$i" = 40 ] && fail "订单未到终态: $N / $C"
  sleep 0.5
done

# 7. 资金断言(定点 ×1e8)
log "校验余额与账本"
assert_eq() { [ "$2" = "$3" ] || fail "$1: 期望 $3,实际 $2"; }
assert_eq "101 USDT available" "$(q "SELECT balance FROM accounts WHERE owner_id=101 AND owner_type='user' AND currency='USDT' AND type='available'")" "499500000000"
assert_eq "101 USDT frozen"    "$(q "SELECT balance FROM accounts WHERE owner_id=101 AND owner_type='user' AND currency='USDT' AND type='frozen'")"    "0"
assert_eq "101 BTC available"  "$(q "SELECT balance FROM accounts WHERE owner_id=101 AND owner_type='user' AND currency='BTC' AND type='available'")"  "30000000"
assert_eq "102 USDT available" "$(q "SELECT balance FROM accounts WHERE owner_id=102 AND owner_type='user' AND currency='USDT' AND type='available'")" "1499500000000"
assert_eq "102 USDT frozen"    "$(q "SELECT balance FROM accounts WHERE owner_id=102 AND owner_type='user' AND currency='USDT' AND type='frozen'")"    "0"
assert_eq "102 BTC available"  "$(q "SELECT balance FROM accounts WHERE owner_id=102 AND owner_type='user' AND currency='BTC' AND type='available'")"  "10000000"
assert_eq "102 BTC frozen"     "$(q "SELECT balance FROM accounts WHERE owner_id=102 AND owner_type='user' AND currency='BTC' AND type='frozen'")"     "0"
assert_eq "fee USDT"           "$(q "SELECT balance FROM accounts WHERE owner_id=0 AND owner_type='fee' AND currency='USDT'")"                         "1000000000"
assert_eq "账实不符行数"         "$(q "SELECT count(*) FROM v_balance_mismatch")" "0"
assert_eq "成交单 filled_qty"    "$(q "SELECT count(*) FROM orders WHERE order_id IN ($ID1,$ID2) AND filled_qty=10000000 AND reserved=0")" "2"
assert_eq "会计恒等:资产=负债+权益" "$(q "
  WITH a AS (SELECT COALESCE(SUM(CASE WHEN owner_type='system' THEN balance END),0) AS assets FROM accounts),
       l AS (SELECT COALESCE(SUM(CASE WHEN owner_type<>'system' THEN balance END),0) AS claims FROM accounts)
  SELECT (a.assets = l.claims)::int FROM a, l")" "1"

log "PASS ✔  下单→冻结→撮合→清算→解冻 全链路资金正确,账本平衡"
