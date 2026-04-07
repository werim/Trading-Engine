#!/bin/bash

set -e

BASE_DIR="$(cd "$(dirname "$0")" && pwd)"
cd "$BASE_DIR"

echo "Resetting CSV files and score..."

cat > open_orders.csv << 'EOF'
order_id,symbol,side,entry_zone_low,entry_zone_high,entry_trigger,sl,tp,rr,score,tf_context,setup_type,setup_reason,created_at,status,live_price,zone_touched
EOF

cat > closed_orders.csv << 'EOF'
order_id,symbol,side,entry_zone_low,entry_zone_high,entry_trigger,sl,tp,rr,score,tf_context,setup_type,setup_reason,created_at,closed_at,status,close_reason,close_price
EOF

cat > history_orders.csv << 'EOF'
order_id,symbol,side,entry_zone_low,entry_zone_high,entry_trigger,sl,tp,rr,score,tf_context,setup_type,setup_reason,created_at,status
EOF

cat > open_positions.csv << 'EOF'
position_id,order_id,symbol,side,entry,sl,tp,opened_at,trigger_price,status,live_price,pnl_pct
EOF

cat > closed_positions.csv << 'EOF'
position_id,order_id,symbol,side,entry,sl,tp,opened_at,closed_at,close_reason,close_price,pnl_pct,score_after_close,status
EOF

cat > history_positions.csv << 'EOF'
position_id,order_id,symbol,side,entry,sl,tp,opened_at,trigger_price,status
EOF

cat > event_log.csv << 'EOF'
time,event,symbol,side,details,score
EOF

echo "0" > score.txt

: > order.log
: > position.log

rm -f order.pid position.pid
rm -f engine.lock engine_orders.lock engine_positions.lock engine_open_orders.lock engine_generation.lock engine_trigger.lock

echo "Reset complete."