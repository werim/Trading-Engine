#!/bin/bash

set -euo pipefail

BASE_DIR="$(cd "$(dirname "$0")" && pwd)"
cd "$BASE_DIR" || exit 1

DATA_DIR="$BASE_DIR/data"
LOG_DIR="$BASE_DIR/logs"
PID_DIR="$BASE_DIR/pids"

ORDER_PID_FILE="$PID_DIR/order.pid"
POSITION_PID_FILE="$PID_DIR/position.pid"

mkdir -p "$DATA_DIR" "$LOG_DIR" "$PID_DIR"

if [ -d "$BASE_DIR/venv" ]; then
  # shellcheck disable=SC1091
  source "$BASE_DIR/venv/bin/activate"
fi

if [ ! -f "$BASE_DIR/order.py" ]; then
  echo "order.py not found in $BASE_DIR"
  exit 1
fi

if [ ! -f "$BASE_DIR/position.py" ]; then
  echo "position.py not found in $BASE_DIR"
  exit 1
fi

is_running() {
  local pid="${1:-}"
  if [ -z "$pid" ]; then
    return 1
  fi
  ps -p "$pid" > /dev/null 2>&1
}

stop_existing_if_any() {
  local pid_file="$1"
  local label="$2"

  if [ -f "$pid_file" ]; then
    local pid
    pid="$(cat "$pid_file" 2>/dev/null || true)"
    if is_running "${pid:-}"; then
      echo "Stopping existing $label (PID $pid)"
      kill "$pid" 2>/dev/null || true
      sleep 2
      if is_running "$pid"; then
        echo "Force killing $label (PID $pid)"
        kill -9 "$pid" 2>/dev/null || true
      fi
    fi
    rm -f "$pid_file"
  fi
}

echo "========== TRADING ENGINE START =========="
echo "Base dir : $BASE_DIR"
echo "Data dir : $DATA_DIR"
echo "Log dir  : $LOG_DIR"
echo "PID dir  : $PID_DIR"
echo

stop_existing_if_any "$ORDER_PID_FILE" "order.py"
stop_existing_if_any "$POSITION_PID_FILE" "position.py"

echo "Starting order.py ..."
nohup python3 "$BASE_DIR/order.py" >> "$LOG_DIR/order.log" 2>&1 &
ORDER_PID=$!
echo "$ORDER_PID" > "$ORDER_PID_FILE"
sleep 1

if ! is_running "$ORDER_PID"; then
  echo "order.py failed to start"
  exit 1
fi

echo "Starting position.py ..."
nohup python3 "$BASE_DIR/position.py" >> "$LOG_DIR/position.log" 2>&1 &
POSITION_PID=$!
echo "$POSITION_PID" > "$POSITION_PID_FILE"
sleep 1

if ! is_running "$POSITION_PID"; then
  echo "position.py failed to start"
  exit 1
fi

echo
echo "Engine started successfully."
echo "order.py PID    : $ORDER_PID"
echo "position.py PID : $POSITION_PID"
echo
echo "Logs:"
echo "  tail -f $LOG_DIR/order.log"
echo "  tail -f $LOG_DIR/position.log"
echo "=========================================="