#!/usr/bin/env bash
# 数据库迁移:顺序执行 db/migrations/*.up.sql,用 schema_migrations 记录进度。
# 仅支持 PostgreSQL(dev:容器内 psql)。
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
PG_CT="${PG_CONTAINER:-cex-dev-postgres-1}"

psql() { docker exec -i "$PG_CT" psql -U cex -d cex -v ON_ERROR_STOP=1 "$@"; }
q()    { docker exec "$PG_CT" psql -U cex -d cex -tAc "$1"; }

docker exec "$PG_CT" true 2>/dev/null || { echo "[migrate] postgres 容器未运行" >&2; exit 1; }

psql -c "CREATE TABLE IF NOT EXISTS schema_migrations (version TEXT PRIMARY KEY, applied_at TIMESTAMPTZ DEFAULT now())" >/dev/null

for f in "$ROOT"/db/migrations/*.up.sql; do
  v="$(basename "$f" | cut -d_ -f1)"
  if [ "$(q "SELECT count(*) FROM schema_migrations WHERE version='$v'")" = "1" ]; then
    echo "[migrate] skip $v (applied)"
    continue
  fi
  # 基线兼容:账本表在引入本脚本前已手工建好时,直接登记为已应用
  if [ "$v" = "000001" ] && [ "$(q "SELECT to_regclass('accounts') IS NOT NULL")" = "t" ]; then
    echo "[migrate] baseline $v (tables already exist)"
    psql -c "INSERT INTO schema_migrations(version) VALUES ('$v')" >/dev/null
    continue
  fi
  echo "[migrate] apply $v"
  psql -f - < "$f" >/dev/null
  psql -c "INSERT INTO schema_migrations(version) VALUES ('$v')" >/dev/null
done
echo "[migrate] done"
