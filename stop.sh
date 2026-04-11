#!/bin/bash

set -u

BASE_DIR="$(cd "$(dirname "$0")" && pwd)"
cd "$BASE_DIR" || exit 1

PID_FILE=".run_pid"
POS_PID_FILE=".position.pid"

echo "🛑 Stopping unified trading engine..."

if [ -f "$POS_PID_FILE" ]; then
  POS_PID=$(cat "$POS_PID_FILE" 2>/dev/null || true)
  if [ -n "${POS_PID:-}" ] && ps -p "$POS_PID" > /dev/null 2>&1; then
    echo "Stopping position supervisor PID: $POS_PID"
    kill -TERM "$POS_PID" 2>/dev/null || true
    sleep 1
    ps -p "$POS_PID" > /dev/null 2>&1 && kill -KILL "$POS_PID" 2>/dev/null || true
  fi
  rm -f "$POS_PID_FILE"
fi

if [ -f "$PID_FILE" ]; then
  MAIN_PID=$(cat "$PID_FILE" 2>/dev/null || true)
  if [ -n "${MAIN_PID:-}" ] && ps -p "$MAIN_PID" > /dev/null 2>&1; then
    echo "Stopping main run.sh PID: $MAIN_PID"
    kill -TERM "$MAIN_PID" 2>/dev/null || true
    sleep 1
    ps -p "$MAIN_PID" > /dev/null 2>&1 && kill -KILL "$MAIN_PID" 2>/dev/null || true
  fi
  rm -f "$PID_FILE"
fi

# safety sweep
pkill -TERM -f "position.py" 2>/dev/null || true
pkill -TERM -f "order.py" 2>/dev/null || true
pkill -TERM -f "run.sh" 2>/dev/null || true
sleep 1
pkill -KILL -f "position.py" 2>/dev/null || true
pkill -KILL -f "order.py" 2>/dev/null || true
pkill -KILL -f "run.sh" 2>/dev/null || true
pkill -KILL -f "position_ws.py" 2>/dev/null || true
echo
echo "Remaining related processes:"
pgrep -af "position.py|order.py|run.sh" || echo "No related processes found ✅"

echo "✅ Unified trading engine stopped."