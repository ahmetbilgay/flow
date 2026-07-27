#!/bin/bash
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"

echo "🐳 Integration test suite (MySQL + PostgreSQL + Redis)"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"

export FLOW_MYSQL_DSN="${FLOW_MYSQL_DSN:-root:root@tcp(127.0.0.1:3306)/dgmos_flow?parseTime=true}"
export FLOW_POSTGRES_DSN="${FLOW_POSTGRES_DSN:-postgres://postgres:root@127.0.0.1:5432/dgmos_flow?sslmode=disable}"
export FLOW_REDIS_ADDR="${FLOW_REDIS_ADDR:-127.0.0.1:6379}"

if ! command -v docker >/dev/null 2>&1; then
  echo "❌ docker is required for integration tests"
  exit 1
fi

echo "Starting docker-compose services..."
docker compose up -d mysql postgres redis

echo "Waiting for databases..."
for i in $(seq 1 60); do
  if docker compose exec -T mysql mysqladmin ping -h 127.0.0.1 -uroot -proot --silent 2>/dev/null; then
    break
  fi
  sleep 2
done

for i in $(seq 1 60); do
  if docker compose exec -T postgres pg_isready -U postgres 2>/dev/null; then
    break
  fi
  sleep 2
done

for i in $(seq 1 30); do
  if docker compose exec -T redis redis-cli ping 2>/dev/null | grep -q PONG; then
    break
  fi
  sleep 1
done

echo ""
echo "Running integration tests..."
FLOW_INTEGRATION_REQUIRED=1 go test -tags=integration ./... -count=1 -race -timeout=10m -v

echo ""
echo "✅ Integration tests passed"
