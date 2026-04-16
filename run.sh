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

echo $$ > "$RUN_PID_FILE"

cleanup() {
  echo
  echo "🛑 Stopping engine..."

  for f in "$ORDER_PID_FILE" "$POSITION_PID_FILE"; do
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

run_position() {
  echo "⚡ position.py $(date)" | tee -a logs/position.supervisor.log
  python3 position.py >> logs/position.log 2>&1 &
  echo $! > "$POSITION_PID_FILE"
}

run_cycle_loop() {
  echo "⚡ order supervisor $(date)" | tee -a logs/order.supervisor.log
  # tail -f position.log

  while true; do
    now=$(date +%s)
    next=$(( ((now / 300) + 1) * 300 ))
    sleep_seconds=$(( next - now ))

    echo "⏳ next cycle in $sleep_seconds sec" | tee -a logs/order.supervisor.log
    sleep "$sleep_seconds"

    echo "🧠 order.py $(date)" | tee -a logs/order.supervisor.log
    python3 order.py >> logs/order.log 2>&1
    echo $! > "$ORDER_PID_FILE" 2>/dev/null || true
  done
}

# shellcheck disable=SC2046
echo "🚀 Trading Engine Start"

run_position

# foreground kalsın ki run.sh ana process olsun
run_cycle_loop