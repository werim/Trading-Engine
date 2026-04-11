#!/bin/bash

set -euo pipefail

BASE_DIR="$(cd "$(dirname "$0")" && pwd)"
cd "$BASE_DIR" || exit 1

DATA_DIR="$BASE_DIR/data"
LOG_DIR="$BASE_DIR/logs"

mkdir -p "$DATA_DIR" "$LOG_DIR"

RESET_SCORE=0
RESET_ALL=0

for arg in "$@"; do
  case "$arg" in
    --score)
      RESET_SCORE=1
      ;;
    --all)
      RESET_ALL=1
      RESET_SCORE=1
      ;;
    *)
      echo "Unknown option: $arg"
      echo "Usage: ./reset.sh [--score] [--all]"
      exit 1
      ;;
  esac
done

EXECUTION_MODE=$(python3 - <<'PY'
from config import CONFIG
print(CONFIG.ENGINE.EXECUTION_MODE.upper())
PY
)

STRATEGY_MODE=$(python3 - <<'PY'
from config import CONFIG
print(CONFIG.ENGINE.STRATEGY_MODE.upper())
PY
)

SCORE_FILE=$(python3 - <<'PY'
from config import CONFIG
print(CONFIG.ADAPTIVE.SCORE_FILE)
PY
)

echo "Resetting unified trading engine data..."
echo "Execution mode : $EXECUTION_MODE"
echo "Strategy mode  : $STRATEGY_MODE"

write_orders_header() {
  local file="$1"
  cat > "$file" <<'EOF'
order_id,symbol,side,entry_zone_low,entry_zone_high,entry_trigger,sl,tp,rr,score,tf_context,setup_type,setup_reason,created_at,updated_at,expires_at,status,live_price,zone_touched,alarm_touched_sent,alarm_near_trigger_sent,last_alarm_at,exchange_order_placed,exchange_order_id,gross_profit_pct,net_profit_pct,net_loss_pct,net_rr,expected_net_profit_usdt,total_cost_pct
EOF
}

write_closed_orders_header() {
  local file="$1"
  cat > "$file" <<'EOF'
order_id,symbol,side,entry_zone_low,entry_zone_high,entry_trigger,sl,tp,rr,score,tf_context,setup_type,setup_reason,created_at,updated_at,expires_at,status,live_price,zone_touched,alarm_touched_sent,alarm_near_trigger_sent,last_alarm_at,exchange_order_placed,exchange_order_id,gross_profit_pct,net_profit_pct,net_loss_pct,net_rr,expected_net_profit_usdt,total_cost_pct,close_reason
EOF
}

write_positions_header() {
  local file="$1"
  cat > "$file" <<'EOF'
position_id,order_id,symbol,side,entry,qty,sl,tp,rr,score,tf_context,setup_type,setup_reason,opened_at,updated_at,status,live_price,pnl_pct,net_pnl_pct,net_pnl_usdt,fees_usdt,sl_order_id,tp_order_id,protection_armed
EOF
}

reset_paper_files() {
  echo "Clearing PAPER mode CSV files..."
  write_orders_header "$DATA_DIR/open_orders.csv"
  write_closed_orders_header "$DATA_DIR/closed_orders.csv"
  write_positions_header "$DATA_DIR/open_positions.csv"
  write_positions_header "$DATA_DIR/closed_positions.csv"
}

reset_real_files() {
  echo "Clearing REAL mode CSV files..."
  write_orders_header "$DATA_DIR/real_open_orders.csv"
  write_closed_orders_header "$DATA_DIR/real_closed_orders.csv"
  write_positions_header "$DATA_DIR/real_open_positions.csv"
  write_positions_header "$DATA_DIR/real_closed_positions.csv"
}

reset_shared_files() {
  cat > "$DATA_DIR/event_log.csv" <<'EOF'
time,event,symbol,side,details,score
EOF

  : > "$LOG_DIR/order.log"
  : > "$LOG_DIR/order.supervisor.log"
  : > "$LOG_DIR/position.log"
  : > "$LOG_DIR/position.supervisor.log"
  : > "$LOG_DIR/position_ws.log"

  rm -f \
    .run_pid \
    .order.pid \
    .position.pid \
    .position_ws.pid \
    engine.lock \
    engine_orders.lock \
    engine_positions.lock \
    engine_open_orders.lock \
    engine_generation.lock \
    engine_trigger.lock
}

if [ "$RESET_ALL" -eq 1 ]; then
  reset_paper_files
  reset_real_files
  reset_shared_files
else
  if [ "$EXECUTION_MODE" = "REAL" ]; then
    reset_real_files
  elif [ "$EXECUTION_MODE" = "PAPER" ]; then
    reset_paper_files
  else
    echo "❌ Unknown CONFIG.ENGINE.EXECUTION_MODE: $EXECUTION_MODE"
    exit 1
  fi

  reset_shared_files
fi

if [ "$RESET_SCORE" -eq 1 ]; then
  echo "Resetting adaptive score..."
  mkdir -p "$(dirname "$SCORE_FILE")"
  echo "0" > "$SCORE_FILE"
else
  if [ -f "$SCORE_FILE" ]; then
    echo "Keeping adaptive score: $(cat "$SCORE_FILE" 2>/dev/null || echo 0)"
  else
    echo "Adaptive score file not found, creating with 0"
    mkdir -p "$(dirname "$SCORE_FILE")"
    echo "0" > "$SCORE_FILE"
  fi
fi

echo "✅ Reset complete."