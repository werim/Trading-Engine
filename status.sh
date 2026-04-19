#!/bin/bash

set -euo pipefail

BASE_DIR="$(cd "$(dirname "$0")" && pwd)"
PID_DIR="$BASE_DIR/pids"

ORDER_PID_FILE="$PID_DIR/order.pid"
POSITION_PID_FILE="$PID_DIR/position.pid"

echo "=============================="
echo "   TRADING ENGINE STATUS"
echo "=============================="
echo ""

is_running() {
  local pid="${1:-}"
  if [ -z "$pid" ]; then
    return 1
  fi
  ps -p "$pid" > /dev/null 2>&1
}

check_process() {
  local pid_file="$1"
  local name="$2"

  if [ ! -f "$pid_file" ]; then
    echo "❌ $name: PID file not found"
    return
  fi

  local pid
  pid="$(cat "$pid_file" 2>/dev/null || true)"

  if is_running "$pid"; then
    echo "🟢 $name: RUNNING (PID $pid)"

    # ekstra bilgi: CPU / RAM
    ps -p "$pid" -o %cpu,%mem,etime,command | tail -n 1
  else
    echo "🔴 $name: NOT RUNNING (stale PID $pid)"
  fi
}

# Kontroller
check_process "$ORDER_PID_FILE" "ORDER ENGINE"
echo ""
check_process "$POSITION_PID_FILE" "POSITION ENGINE"

echo ""
echo "------------------------------"

# Ekstra: çalışan python süreçleri
echo "🐍 Active Python Processes:"
ps aux | grep python | grep -v grep || echo "No active python processes"

echo ""
echo "=============================="