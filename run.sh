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

is_running() {
  local pid="${1:-}"
  if [ -z "$pid" ]; then
    return 1
  fi
  ps -p "$pid" > /dev/null 2>&1
}

echo "========== TRADING ENGINE START =========="
echo "Base dir : $BASE_DIR"
echo "Data dir : $DATA_DIR"
echo "Log dir  : $LOG_DIR"
echo "PID dir  : $PID_DIR"
echo

if [ -f "$ORDER_PID_FILE" ]; then
  OLD_PID="$(cat "$ORDER_PID_FILE" 2>/dev/null || true)"
  if is_running "$OLD_PID"; then
    echo "order.py already running with PID $OLD_PID"
  else
    rm -f "$ORDER_PID_FILE"
  fi
fi

if [ -f "$POSITION_PID_FILE" ]; then
  OLD_PID="$(cat "$POSITION_PID_FILE" 2>/dev/null || true)"
  if is_running "$OLD_PID"; then
    echo "position.py already running with PID $OLD_PID"
  else
    rm -f "$POSITION_PID_FILE"
  fi
fi

if [ ! -f "$ORDER_PID_FILE" ]; then
  echo "Starting main_order.py ..."
  nohup python3 main_order.py > "$LOG_DIR/order.nohup.log" 2>&1 &
  echo $! > "$ORDER_PID_FILE"
  echo "main_order.py started with PID $(cat "$ORDER_PID_FILE")"
fi

if [ ! -f "$POSITION_PID_FILE" ]; then
  echo "Starting main_position.py ..."
  nohup python3 main_position.py > "$LOG_DIR/position.nohup.log" 2>&1 &
  echo $! > "$POSITION_PID_FILE"
  echo "main_position.py started with PID $(cat "$POSITION_PID_FILE")"
fi

echo
echo "Engine started."