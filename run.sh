#!/bin/bash

set -euo pipefail

BASE_DIR="$(cd "$(dirname "$0")" && pwd)"
cd "$BASE_DIR" || exit 1

mkdir -p logs data

if [ -d "venv" ]; then
  # shellcheck disable=SC1091
  source venv/bin/activate
fi

RUN_PID_FILE=".run_pid"
ORDER_PID_FILE=".order.pid"
POSITION_PID_FILE=".position.pid"
POSITION_WS_PID_FILE=".position_ws.pid"

echo $$ > "$RUN_PID_FILE"

cleanup() {
  echo
  echo "🛑 Stopping engine..."

  for f in "$ORDER_PID_FILE" "$POSITION_PID_FILE" "$POSITION_WS_PID_FILE" "$PRICE_CACHE_PID_FILE"; do
    if [ -f "$f" ]; then
      pid="$(cat "$f" 2>/dev/null || true)"
      if [ -n "${pid:-}" ] && ps -p "$pid" >/dev/null 2>&1; then
        kill "$pid" 2>/dev/null || true
      fi
      rm -f "$f"
    fi
  done

  rm -f "$RUN_PID_FILE"
  wait || true
  echo "✅ Engine stopped."
}

trap cleanup SIGINT SIGTERM EXIT

run_price_cache() {
  echo "⚡ price_cache.py $(date)" | tee -a logs/price_cache.log
  python3 price_cache.py >> logs/price_cache.log 2>&1 &
  echo $! > "$PRICE_CACHE_PID_FILE"
}

run_position_ws() {
  echo "⚡ position_ws.py $(date)" | tee -a logs/position_ws.log
  python3 position_ws.py >> logs/position_ws.log 2>&1 &
  echo $! > "$POSITION_WS_PID_FILE"
}

run_position() {
  echo "⚡ position.py $(date)" | tee -a logs/position.supervisor.log
  python3 position.py >> logs/position.log 2>&1 &
  echo $! > "$POSITION_PID_FILE"
}

run_cycle_loop() {
  while true; do
    now=$(date +%s)

    # align to 5-minute grid (00,05,10...)
    next=$(( ((now / 300) + 1) * 300 ))
    sleep_seconds=$(( next - now ))

    echo "⏳ next cycle in $sleep_seconds sec"
    sleep "$sleep_seconds"

    echo "🧠 order.py $(date)"
    python3 order.py >> logs/order.log 2>&1

    echo "⚡ position.py (post-order sync)"
    python3 position.py >> logs/position.log 2>&1
  done
}

echo "🚀 Trading Engine"
# order loop stays in foreground so run.sh remains the main process
run_cycle_loop