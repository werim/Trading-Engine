# 🚀 Trading Engine

A modular, regime-aware crypto trading engine built for **Binance Futures**.

This system scans markets in real-time, generates high-quality setups, and either:
- executes real trades (**REAL mode**)  
- or simulates trades safely (**PAPER mode**)

---

## 🧠 Core Philosophy

> Trade less. Trade better.

The engine focuses on:
- High **Risk/Reward (RR)**
- Positive **expected net PnL**
- Market regime awareness (trend vs range)
- Low friction conditions (spread, funding)

---

## ⚙️ Features

### ✅ Regime-Aware Strategy

| Market | Behavior |
|------|--------|
| TREND (1D LONG/SHORT) | Breakout + Pullback |
| RANGE (1D RANGE) | Pullback only (default) |

---

### 📊 Smart Trade Filtering

Trades are filtered by:
- Score
- Risk/Reward (RR)
- Expected **net PnL**
- Spread %
- Funding rate %

---

### 💰 Real Net PnL Tracking

The system tracks:

- `pnl_pct`
- `net_pnl_pct` (after fees + slippage)
- `net_pnl_usdt`

---

### 🧪 Paper Trading Mode

```env
EXECUTION_MODE=PAPER