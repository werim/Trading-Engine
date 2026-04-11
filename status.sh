#!/bin/bash

set -u

BASE_DIR="$(cd "$(dirname "$0")" && pwd)"
cd "$BASE_DIR" || exit 1

DATA_DIR="$BASE_DIR/data"
LOG_DIR="$BASE_DIR/logs"

echo "=========== ENGINE STATUS ==========="

get_config_value() {
  local pycode="$1"
  python3 - <<PY 2>/dev/null || true
from config import CONFIG
$pycode
PY
}

EXECUTION_MODE="$(get_config_value 'print(CONFIG.ENGINE.EXECUTION_MODE.upper())')"
STRATEGY_MODE="$(get_config_value 'print(CONFIG.ENGINE.STRATEGY_MODE.upper())')"
SCORE_FILE="$(get_config_value 'print(CONFIG.ADAPTIVE.SCORE_FILE)')"

[ -n "${EXECUTION_MODE:-}" ] || EXECUTION_MODE="UNKNOWN"
[ -n "${STRATEGY_MODE:-}" ] || STRATEGY_MODE="UNKNOWN"
[ -n "${SCORE_FILE:-}" ] || SCORE_FILE="$DATA_DIR/score.txt"

echo "Execution mode : $EXECUTION_MODE"
echo "Strategy mode  : $STRATEGY_MODE"

show_pid_status() {
  local file="$1"
  local label="$2"
  local expected="$3"

  if [ -f "$file" ]; then
    local pid=""
    pid="$(cat "$file" 2>/dev/null || true)"

    if [ -n "$pid" ] && ps -p "$pid" > /dev/null 2>&1; then
      local cmd=""
      cmd="$(ps -p "$pid" -o args= 2>/dev/null || true)"

      if echo "$cmd" | grep -q "$expected"; then
        echo "$label : RUNNING (PID $pid) ✅"
      else
        echo "$label : PID $pid alive but unexpected process ⚠️"
        echo "  cmd: $cmd"
      fi
    else
      echo "$label : NOT RUNNING ❌"
    fi
  else
    echo "$label : NOT RUNNING ❌"
  fi
}

echo
echo "PID files:"
show_pid_status ".run_pid" "run.sh" "run.sh"
show_pid_status ".order.pid" "order worker" "order.py"
show_pid_status ".position.pid" "position worker" "position.py"
show_pid_status ".position_ws.pid" "position ws" "position_ws.py"
show_pid_status ".price_cache.pid" "price cache" "price_cache.py"

echo
echo "Script processes:"
pgrep -af "run.sh|position.py|position_ws.py|order.py|price_cache.py" || echo "No script processes found ✅"

echo
echo "CSV Snapshot:"

count_rows() {
  local file="$1"
  if [ -f "$file" ]; then
    local lines=0
    lines=$(wc -l < "$file" 2>/dev/null || echo 0)
    if [ "${lines:-0}" -gt 0 ]; then
      echo $((lines - 1))
    else
      echo 0
    fi
  else
    echo 0
  fi
}

printf "  %-28s : %s\n" "open_orders.csv"           "$(count_rows "$DATA_DIR/open_orders.csv")"
printf "  %-28s : %s\n" "closed_orders.csv"         "$(count_rows "$DATA_DIR/closed_orders.csv")"
printf "  %-28s : %s\n" "open_positions.csv"        "$(count_rows "$DATA_DIR/open_positions.csv")"
printf "  %-28s : %s\n" "closed_positions.csv"      "$(count_rows "$DATA_DIR/closed_positions.csv")"
printf "  %-28s : %s\n" "real_open_orders.csv"      "$(count_rows "$DATA_DIR/real_open_orders.csv")"
printf "  %-28s : %s\n" "real_closed_orders.csv"    "$(count_rows "$DATA_DIR/real_closed_orders.csv")"
printf "  %-28s : %s\n" "real_open_positions.csv"   "$(count_rows "$DATA_DIR/real_open_positions.csv")"
printf "  %-28s : %s\n" "real_closed_positions.csv" "$(count_rows "$DATA_DIR/real_closed_positions.csv")"
printf "  %-28s : %s\n" "event_log.csv"             "$(count_rows "$DATA_DIR/event_log.csv")"

echo
echo "Score:"
if [ -f "$SCORE_FILE" ]; then
  cat "$SCORE_FILE"
else
  echo "0"
fi

file_mtime() {
  local file="$1"

  if date -r "$file" "+%Y-%m-%d %H:%M:%S" >/dev/null 2>&1; then
    date -r "$file" "+%Y-%m-%d %H:%M:%S"
    return
  fi

  if stat -f "%Sm" -t "%Y-%m-%d %H:%M:%S" "$file" >/dev/null 2>&1; then
    stat -f "%Sm" -t "%Y-%m-%d %H:%M:%S" "$file"
    return
  fi

  if stat -c "%y" "$file" >/dev/null 2>&1; then
    stat -c "%y" "$file" | cut -d'.' -f1
    return
  fi

  echo "unknown"
}

show_log() {
  local file="$1"
  local label="$2"

  echo
  echo "Recent $label:"
  if [ -f "$file" ]; then
    echo "--- last modified: $(file_mtime "$file")"
    tail -n 15 "$file"
  else
    echo "no $file"
  fi
}

show_log "$LOG_DIR/order.log" "order.log"
show_log "$LOG_DIR/order.supervisor.log" "order.supervisor.log"
show_log "$LOG_DIR/position.log" "position.log"
show_log "$LOG_DIR/position.supervisor.log" "position.supervisor.log"