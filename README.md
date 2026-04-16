# 🚀 Trading Engine

A modular, regime-aware crypto trading engine built for **Binance Futures**.

This system scans markets in real time, generates setups, and either:
- executes real trades (**REAL mode**)
- or simulates trades safely (**PAPER mode**)

---

## Execution Modes

Set the mode with:

```env
EXECUTION_MODE=PAPER
# or
EXECUTION_MODE=REAL
```

### PAPER mode
- No authenticated order placement/cancel calls are sent to Binance.
- Orders and positions are simulated locally.
- Protective SL/TP IDs are synthetic (`paper-sl`, `paper-tp`).

### REAL mode
- Entry/management logic syncs with Binance open orders and positions.
- Exchange fill confirmation is required before treating an order as a live position.
- SL/TP protections are created and validated on the exchange.

---

## Mode Responsibilities by File

- `config.py` → reads and normalizes `EXECUTION_MODE` (`PAPER` by default).
- `order.py` → routes order execution to simulation or live exchange sync.
- `position.py` → performs paper lifecycle or real exchange-protected lifecycle.
- `strategy.py` / `market.py` → signal and market data pipeline used by both modes.
- `binance_real.py` → authenticated/live Binance Futures client.

---

## Quick Start

1. Install dependencies:

```bash
pip install -r requirements.txt
```

2. Configure environment (example):

```env
EXECUTION_MODE=PAPER
BINANCE_API_KEY=...
BINANCE_API_SECRET=...
```

3. Run the engine:

```bash
./run.sh
```

---

## Safety Notes

- Start with `EXECUTION_MODE=PAPER` to validate behavior.
- Switch to `REAL` only after confirming logs, sizing, and protection logic.
- Keep API key permissions minimal and use IP restrictions where possible.
