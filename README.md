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
<<<<<<< ours

 (cd "$(git rev-parse --show-toplevel)" && git apply --3way <<'EOF' 
diff --git a/README.md b/README.md
index bb976f538ede4a850a38ffddbc1ce7585e75b1c6..9e115e45b4fbb3a5a7cde8fb86083fef53a05dfe 100644
--- a/README.md
+++ b/README.md
@@ -33,26 +33,53 @@ The engine focuses on:
 
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
-EXECUTION_MODE=PAPER
\ No newline at end of file
+EXECUTION_MODE=PAPER
```

---

## 🧪 Advanced Backtesting + Optimization

A new `backtest.py` module provides parameter grid-search optimization to maximize **net_pnl_pct** while penalizing drawdown.

### Optimized parameters
- RR target values (`--rr-values`)
- Stop-loss multipliers (`--sl-multipliers`)
- Trailing profiles (off/tight/balanced/loose)

### Metrics produced
- Best configuration by fitness: `net_pnl_pct - drawdown_penalty * max_drawdown_pct`
- Equity curve
- Drawdown curve
- Per-symbol performance (trade count, win rate, net/avg pnl)

### Run
```bash
python backtest.py \
  --closed-positions data/closed_positions.csv \
  --rr-values 2.0,2.4,2.8,3.2 \
  --sl-multipliers 0.8,1.0,1.2 \
  --drawdown-penalty 0.5
```
>>>>>>> theirs
