#!/bin/bash

set -euo pipefail

BASE_DIR="$(cd "$(dirname "$0")" && pwd)"
cd "$BASE_DIR" || exit 1

PID_DIR="$BASE_DIR/pids"
LOG_DIR="$BASE_DIR/logs"

ORDER_PID_FILE="$PID_DIR/order.pid"
POSITION_PID_FILE="$PID_DIR/position.pid"

is_running() {
  local pid="${1:-}"
  if [ -z "$pid" ]; then
    return 1
  fi
  ps -p "$pid" > /dev/null 2>&1
}

show_status() {
  local pid_file="$1"
  local label="$2"

  if [ ! -f "$pid_file" ]; then
    echo "$label: PID file missing"
    return
  fi

  local pid
  pid="$(cat "$pid_file" 2>/dev/null || true)"

  if is_running "${pid:-}"; then
    echo "$label: RUNNING (PID $pid)"
  else
    echo "$label: NOT RUNNING (stale PID file: $pid)"
  fi
}

echo "========== TRADING ENGINE STATUS =========="
show_status "$ORDER_PID_FILE" "order.py"
show_status "$POSITION_PID_FILE" "position.py"
echo
echo "--- tail: order.log ---"
tail -n 10 "$LOG_DIR/order.log" 2>/dev/null || true
echo
echo "--- tail: position.log ---"
tail -n 10 "$LOG_DIR/position.log" 2>/dev/null || true
echo "==========================================="

# Ekstra: çalışan python süreçleri
echo "🐍 Active Python Processes:"
ps aux | grep python | grep -v grep || echo "No active python processes"
