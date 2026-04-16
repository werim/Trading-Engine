#!/bin/bash

set -euo pipefail

BASE_DIR="$(cd "$(dirname "$0")" && pwd)"
cd "$BASE_DIR" || exit 1

DATA_DIR="$BASE_DIR/data"
LOG_DIR="$BASE_DIR/logs"
ARCHIVE_DIR="$BASE_DIR/archive"
TIMESTAMP="$(date +"%Y%m%d_%H%M%S")"

mkdir -p "$DATA_DIR" "$LOG_DIR" "$ARCHIVE_DIR"

echo "========== TRADING ENGINE RESET =========="

mkdir -p "$ARCHIVE_DIR/$TIMESTAMP"

archive_if_exists() {
  local file="$1"
  if [ -f "$file" ]; then
    mv "$file" "$ARCHIVE_DIR/$TIMESTAMP/"
    echo "Archived: $file"
  fi
}

archive_if_exists "$DATA_DIR/open_orders.csv"
archive_if_exists "$DATA_DIR/open_positions.csv"
archive_if_exists "$DATA_DIR/closed_positions.csv"
archive_if_exists "$DATA_DIR/symbol_meta.json"
archive_if_exists "$DATA_DIR/market_cache.json"

archive_if_exists "$LOG_DIR/order.log"
archive_if_exists "$LOG_DIR/position.log"
archive_if_exists "$LOG_DIR/engine.log"

cat > "$DATA_DIR/open_orders.csv" <<'EOF'
order_id,symbol,side,entry_zone_low,entry_zone_high,entry_trigger,sl,tp,rr,score,tf_context,setup_type,setup_reason,created_at,updated_at,expires_at,status,live_price,zone_touched,alarm_touched_sent,alarm_near_trigger_sent,last_alarm_at,expected_net_pnl_pct,stop_net_loss_pct,volume_24h_usdt,spread_pct,funding_rate_pct
EOF

cat > "$DATA_DIR/open_positions.csv" <<'EOF'
position_id,order_id,symbol,side,entry,qty,sl,tp,rr,score,tf_context,setup_type,setup_reason,opened_at,updated_at,status,live_price,pnl_pct,net_pnl_pct,net_pnl_usdt,fees_usdt,sl_order_id,tp_order_id,protection_armed,partial_taken,break_even_armed,highest_price,lowest_price
EOF

cat > "$DATA_DIR/closed_positions.csv" <<'EOF'
position_id,order_id,symbol,side,entry,qty,sl,tp,rr,score,tf_context,setup_type,setup_reason,opened_at,updated_at,status,live_price,pnl_pct,net_pnl_pct,net_pnl_usdt,fees_usdt,sl_order_id,tp_order_id,protection_armed,partial_taken,break_even_armed,highest_price,lowest_price,closed_at,close_reason,close_price
EOF

touch "$LOG_DIR/order.log"
touch "$LOG_DIR/position.log"
touch "$LOG_DIR/engine.log"

echo
echo "Reset complete."
echo "Archived old files into: $ARCHIVE_DIR/$TIMESTAMP"
echo "Fresh CSV and log files created."
echo "=========================================="