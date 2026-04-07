#!/bin/bash

clear

echo "=========== ENGINE STATUS ==========="

# --- PROCESS CHECK ---
ORDER_PID=$(pgrep -f order.py)
POSITION_PID=$(pgrep -f position.py)

if [ -z "$ORDER_PID" ]; then
  echo "order.py   : NOT RUNNING ❌"
else
  echo "order.py   : RUNNING (PID $ORDER_PID) ✅"
fi

if [ -z "$POSITION_PID" ]; then
  echo "position.py: NOT RUNNING ❌"
else
  echo "position.py: RUNNING (PID $POSITION_PID) ✅"
fi

# --- SCORE ---
if [ -f score.txt ]; then
  SCORE=$(cat score.txt)
else
  SCORE="N/A"
fi

echo ""
echo "Score: $SCORE"

# --- CSV SNAPSHOT ---
echo ""
echo "CSV Snapshot:"

count_lines () {
  if [ -f "$1" ]; then
    echo $(( $(wc -l < "$1") - 1 ))
  else
    echo "0"
  fi
}

printf "  open_orders.csv       : %s\n" "$(count_lines open_orders.csv)"
printf "  closed_orders.csv     : %s\n" "$(count_lines closed_orders.csv)"
printf "  history_orders.csv    : %s\n" "$(count_lines history_orders.csv)"
printf "  open_positions.csv    : %s\n" "$(count_lines open_positions.csv)"
printf "  closed_positions.csv  : %s\n" "$(count_lines closed_positions.csv)"
printf "  history_positions.csv : %s\n" "$(count_lines history_positions.csv)"
printf "  event_log.csv         : %s\n" "$(count_lines event_log.csv)"

# --- LAST LOGS ---
echo ""
echo "Recent order.log:"
if [ -f order.log ]; then
  tail -n 5 order.log
else
  echo "No order.log"
fi

echo ""
echo "Recent position.log:"
if [ -f position.log ]; then
  tail -n 5 position.log
else
  echo "No position.log"
fi

# --- WARNING SYSTEM ---
echo ""
echo "=========== WARNINGS ==========="

if [ -z "$POSITION_PID" ]; then
  echo "⚠️  POSITION ENGINE DEAD"
fi

if [ -z "$ORDER_PID" ]; then
  echo "⚠️  ORDER ENGINE DEAD"
fi

OPEN_POS=$(count_lines open_positions.csv)
OPEN_ORD=$(count_lines open_orders.csv)

if [ "$OPEN_ORD" -gt 0 ] && [ "$OPEN_POS" -eq 0 ]; then
  echo "⚠️  Orders var ama pozisyon açılmıyor!"
fi

if grep -q "ERROR" order.log 2>/dev/null; then
  echo "⚠️  order.log içinde ERROR var"
fi

if grep -q "ERROR" position.log 2>/dev/null; then
  echo "⚠️  position.log içinde ERROR var"
fi

echo ""
echo "=================================="