#!/bin/bash
# chmod +x *.sh
  #./stop.sh
  #./status.sh
  #./run.sh
  #./status.sh
# tail -f order.log
# tail -f position.log


pkill -f "order.py" 2>/dev/null
pkill -f "position.py" 2>/dev/null
rm -f order.pid position.pid

set -e

BASE_DIR="$(cd "$(dirname "$0")" && pwd)"
cd "$BASE_DIR"

ORDER_PID_FILE="order.pid"
POSITION_PID_FILE="position.pid"

echo "Starting trading engine in: $BASE_DIR"

if [ ! -d "venv" ]; then
  echo "Creating virtual environment..."
  python3 -m venv venv
fi

echo "Activating virtual environment..."
source venv/bin/activate

echo "Installing requirements..."
pip install --upgrade pip
pip install -r requirements.txt

# Eğer eski süreçler varsa temizle
if [ -f "$ORDER_PID_FILE" ]; then
  OLD_PID=$(cat "$ORDER_PID_FILE" 2>/dev/null || true)
  if [ -n "$OLD_PID" ] && ps -p "$OLD_PID" > /dev/null 2>&1; then
    echo "order.py already running with PID $OLD_PID"
  else
    rm -f "$ORDER_PID_FILE"
  fi
fi

if [ -f "$POSITION_PID_FILE" ]; then
  OLD_PID=$(cat "$POSITION_PID_FILE" 2>/dev/null || true)
  if [ -n "$OLD_PID" ] && ps -p "$OLD_PID" > /dev/null 2>&1; then
    echo "position.py already running with PID $OLD_PID"
  else
    rm -f "$POSITION_PID_FILE"
  fi
fi

# order.py başlat
if [ ! -f "$ORDER_PID_FILE" ]; then
  echo "Starting order.py..."
  nohup venv/bin/python order.py > order.log 2>&1 &
  echo $! > "$ORDER_PID_FILE"
  echo "order.py started with PID $(cat "$ORDER_PID_FILE")"
fi

# position.py başlat
if [ ! -f "$POSITION_PID_FILE" ]; then
  echo "Starting position.py..."
  nohup venv/bin/python position.py > position.log 2>&1 &
  echo $! > "$POSITION_PID_FILE"
  echo "position.py started with PID $(cat "$POSITION_PID_FILE")"
fi

echo ""
echo "Trading engine started."
echo "Logs:"
echo "  order.log"
echo "  position.log"
echo ""
echo "Use ./status.sh to inspect current state."