#!/usr/bin/env bash
# Stop all pipeline services (host processes) and optionally the infra.
cd "$(dirname "$0")/.."

for pidfile in logs/*.pid; do
  [ -f "$pidfile" ] || continue
  name=$(basename "$pidfile" .pid)
  pid=$(cat "$pidfile")
  if kill -0 "$pid" 2>/dev/null; then
    kill "$pid" 2>/dev/null && echo "stopped $name (pid $pid)"
  fi
  rm -f "$pidfile"
done

if [ "${1:-}" = "--infra" ]; then
  docker compose -f deploy/docker-compose.yml down
  echo "infra stopped (kafka + elasticsearch)"
fi
