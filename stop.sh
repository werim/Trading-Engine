#!/bin/bash

echo "Stopping tracked processes if any..."

if [ -f order.pid ]; then
  kill "$(cat order.pid)" 2>/dev/null
  rm -f order.pid
fi

if [ -f position.pid ]; then
  kill "$(cat position.pid)" 2>/dev/null
  rm -f position.pid
fi

echo "Killing stray processes..."
pkill -f "order.py" 2>/dev/null
pkill -f "position.py" 2>/dev/null

rm -f order.pid position.pid

echo "Trading engine stopped."