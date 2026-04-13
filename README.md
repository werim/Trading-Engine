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
diff --git a/README.md b/README.md
index bb976f538ede4a850a38ffddbc1ce7585e75b1c6..74a6aa4641239d912bc34f705007833e30bf027c 100644
--- a/README.md
+++ b/README.md
@@ -33,26 +33,70 @@ The engine focuses on:
 
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
+```
+
+---
+
+## 🧪 Advanced Backtesting + Optimizer (Historical Replay)
+
+`backtest.py` now performs a full historical replay on Binance Futures klines and simulates order/position lifecycle in the same spirit as `order.py` + `position.py`:
+
+- setup generation (breakout/pullback style with EMA + ATR)
+- order staging (`candidate_to_order`)
+- zone-based fills
+- position updates with break-even + trailing stop behavior
+- TP/SL exit bookkeeping with fees/slippage
+
+### Grid Search Parameters
+- RR targets (`--rr-values`)
+- SL multipliers (`--sl-multipliers`)
+- Trailing profiles (off/tight/balanced/loose)
+
+### Optimization Objective
+Maximize `net_pnl_pct` while controlling drawdown via:
+
+`fitness = net_pnl_pct - drawdown_penalty * max_drawdown_pct`
+
+### Diagnostics
+- equity curve + drawdown curve
+- per-symbol performance (win-rate, PF, streaks, avg win/loss)
+- monthly net pnl
+- expectancy + average trade duration
+
+### Run Example
+```bash
+python backtest.py \
+  --start-date 2025-01-01 \
+  --end-date 2025-06-01 \
+  --max-symbols 8 \
+  --interval 1h \
+  --rr-values 2.0,2.4,2.8,3.2 \
+  --sl-multipliers 0.8,1.0,1.2 \
+  --drawdown-penalty 0.7 \
+  --best-env-out data/best_backtest.env
+```
+
+The optimizer prints the best configuration and writes a `.env`-style output file containing tuned parameters.
+ python backtest.py --symbols BTCUSDT --start-date 2025-01-01 --end-date 2025-02-01 --interval 1h --rr-values 2.0 --sl-multipliers 1.0 --max-symbols 10 --drawdown-penalty 0.7 --best-env-out /tmp/best_backtest.env