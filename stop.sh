#!/bin/bash

set -euo pipefail

BASE_DIR="$(cd "$(dirname "$0")" && pwd)"
cd "$BASE_DIR" || exit 1

PID_DIR="$BASE_DIR/pids"
ORDER_PID_FILE="$PID_DIR/order.pid"
POSITION_PID_FILE="$PID_DIR/position.pid"

mkdir -p "$PID_DIR"

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
      sleep 1
    fi

    if is_running "$pid"; then
      echo "Failed to stop $label (PID $pid)"
      exit 1
    else
      echo "$label stopped"
    fi
  else
    echo "$label not running, cleaning stale PID file"
  fi

  rm -f "$pid_file"
}

echo "========== TRADING ENGINE STOP =========="

stop_by_pid_file "$ORDER_PID_FILE" "order.py"
stop_by_pid_file "$POSITION_PID_FILE" "position.py"

echo "All tracked engine processes stopped."
echo "========================================"