#!/bin/bash

set -euo pipefail

BASE_DIR="$(cd "$(dirname "$0")" && pwd)"
cd "$BASE_DIR" || exit 1

echo "========== TRADING ENGINE BOOTSTRAP =========="

mkdir -p data logs pids archive

if ! command -v python3 >/dev/null 2>&1; then
  echo "python3 not found"
  exit 1
fi

if [ ! -d "venv" ]; then
  echo "Creating virtual environment..."
  python3 -m venv venv
fi

# shellcheck disable=SC1091
source "$BASE_DIR/venv/bin/activate"

echo "Upgrading pip..."
python3 -m pip install --upgrade pip

if [ -f "$BASE_DIR/requirements.txt" ]; then
  echo "Installing requirements..."
  pip install -r "$BASE_DIR/requirements.txt"
else
  echo "requirements.txt not found"
  exit 1
fi

if [ ! -f "$BASE_DIR/.env" ]; then
  if [ -f "$BASE_DIR/.env.example" ]; then
    cp "$BASE_DIR/.env.example" "$BASE_DIR/.env"
    echo ".env created from .env.example"
  else
    echo ".env.example not found, skipped creating .env"
  fi
else
  echo ".env already exists, left untouched"
fi

chmod +x \
  "$BASE_DIR/run.sh" \
  "$BASE_DIR/stop.sh" \
  "$BASE_DIR/reset.sh" \
  "$BASE_DIR/status.sh" \
  "$BASE_DIR/bootstrap.sh" 2>/dev/null || true

touch logs/order.log logs/position.log logs/engine.log

echo
echo "Bootstrap complete."
echo
echo "Next:"
echo "1) Edit .env"
echo "2) source venv/bin/activate"
echo "3) ./run.sh"
echo "============================================="