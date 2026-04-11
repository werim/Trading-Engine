# 🚀 Trading Engine !!! FOR PAPER TRADING ONLY !!!

A modular, algorithmic trading engine designed for **Binance Futures**, focused on **pullback & breakout strategies**, multi-timeframe analysis, and automated execution.

---

## 🧠 Features

* 📊 Multi-timeframe trend analysis (1H / 4H / 1D)
* 🎯 Pullback & Breakout trade detection
* ⚡ Real-time price tracking via WebSocket
* 🤖 Automated order execution (Binance Futures)
* 🔔 Telegram alerts (entry, TP, SL)
* 📁 CSV-based state management (orders & positions)
* 🧩 Modular and extendable architecture

---

## ⚙️ How It Works

1. **Scanner (`order.py`)**

   * Scans market
   * Detects valid setups
   * Creates *OPEN_ORDER*

2. **Zone Tracking**

   * Price enters zone → `zone_touched = 1`

3. **Trigger Logic**

   * Trigger price hit → sends **STOP / LIMIT order**

4. **Execution (`binance_real.py`)**

   * Order is sent to Binance Futures

5. **Position Tracking (`position_ws.py`)**

   * Monitors live positions
   * Detects TP / SL hit

6. **Logging & Alerts**

   * Logs all actions
   * Sends Telegram notifications

---

## 📂 Project Structure

```
Trading-Engine/
│
├── order.py              # Trade setup & order creation
├── position.py           # Position management (TP/SL logic)
├── position_ws.py        # Live tracking via WebSocket
├── price_cache.py        # Fast price fetching layer
├── structure.py          # Market structure analysis
├── utils.py              # Helper functions
├── config.py             # Strategy & risk settings
├── env.py                # .env loader
├── binance_real.py       # Binance API integration
├── alert.py              # Telegram alerts
├── market.py             # Market data & symbol filtering
├── adaptive.py           # Adaptive strategy / dynamic thresholds
│
├── run.sh                # Main runner script
├── requirements.txt      # Dependencies
├── .env.example          # Environment template
├── .gitignore
│
├── data/                 # Runtime data (ignored)
├── logs/                 # Logs (ignored)
└── docs/                 # Optional documentation
```

---

## 🔑 Setup

### 1. Clone repo

```bash
git clone https://github.com/werim/Trading-Engine.git
cd Trading-Engine
```

---

### 2. Create virtual environment

```bash
python3 -m venv venv
source venv/bin/activate
```

---

### 3. Install dependencies

```bash
pip install -r requirements.txt
```

---

### 4. Configure environment

Create `.env` file from example:

```bash
cp .env.example .env
```

Fill in your credentials:

```env
BINANCE_API_KEY=your_key
BINANCE_API_SECRET=your_secret

TELEGRAM_BOT_TOKEN=your_token
TELEGRAM_CHAT_ID=your_chat_id
```

---

## ▶️ Run the Engine

```bash
chmod +x run.sh
./run.sh
```

---

## 📊 Trading Logic

### ✅ Entry Conditions

* Multi-timeframe alignment
* Valid structure (trend or range)
* Pullback or breakout confirmation
* Minimum score threshold

---

### 🎯 Order Flow

```
ZONE → TOUCH → TRIGGER → ORDER → POSITION → TP/SL
```

---

### 📌 Example Trade

```
Symbol: BTCUSDT
Type: LONG
Zone: 64000 - 64200
Trigger: 64300
SL: 63800
TP: 66000
RR: 2.5
```

---

## ⚠️ Important Notes

* Uses **Binance Futures API**
* Some order types require **Algo Orders**
* Make sure your API permissions are correct:

  * Futures enabled
  * Trading enabled

---

## 🧪 Modes

Defined in `config.py`:

```python
EXECUTION_MODE = "REAL"   # REAL or PAPER
STRATEGY_MODE = "BALANCED"
```

---

## 📁 Data & Logs

* `data/` → open orders, positions, history
* `logs/` → event logs, system logs

⚠️ These are ignored in Git (`.gitignore`)

---

## 🔔 Telegram Alerts

The system sends:

* ✅ New trade alerts
* 🎯 TP hit
* 🛑 SL hit
* ⚠️ Errors

---

## 🛠️ Customization

You can tweak:

* Risk management (`config.py`)
* Indicators & structure (`structure.py`)
* Order logic (`order.py`)
* Execution logic (`binance_real.py`)

---

## 🚨 Disclaimer

This software is for educational purposes only.

Trading involves risk. Use at your own responsibility.

---

## 👤 Author

Developed by **Sencer**

---

## ⭐ Support

If you like the project:

* ⭐ Star the repo
* 🍴 Fork it
* 🧠 Improve it

---

Happy trading. 📈
