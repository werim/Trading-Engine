#!/bin/bash

set -euo pipefail

BASE_DIR="$(cd "$(dirname "$0")" && pwd)"
cd "$BASE_DIR" || exit 1

PID_DIR="$BASE_DIR/pids"
ORDER_PID_FILE="$PID_DIR/order.pid"
POSITION_PID_FILE="$PID_DIR/position.pid"

is_running() {
  local pid="${1:-}"
  if [ -z "$pid" ]; then
    return 1
  fi
  ps -p "$pid" > /dev/null 2>&1
}

stop_by_pid_file() {
  local pid_file="$1"
  local label="$2"

  if [ ! -f "$pid_file" ]; then
    echo "$label PID file not found"
    return 0
  fi

  local pid
  pid="$(cat "$pid_file" 2>/dev/null || true)"

  if is_running "${pid:-}"; then
    echo "Stopping $label (PID $pid)"
    kill "$pid" 2>/dev/null || true
    sleep 2

    if is_running "$pid"; then
      echo "Force killing $label (PID $pid)"
      kill -9 "$pid" 2>/dev/null || true
    fi
  else
    echo "$label is not running"
  fi

  rm -f "$pid_file"
}

echo "========== TRADING ENGINE STOP =========="
stop_by_pid_file "$ORDER_PID_FILE" "main_order.py"
stop_by_pid_file "$POSITION_PID_FILE" "main_position.py"
echo "Stopped."