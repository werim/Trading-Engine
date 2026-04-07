import csv
import os
import tempfile
import time
import uuid
from contextlib import contextmanager
from datetime import datetime, UTC
from typing import Dict, List, Optional, Tuple, Union

import fcntl
import pandas as pd
import requests

from structure import evaluate_symbol
import config


# =========================================================
# Config
# =========================================================

TOP_N = getattr(config, "TOP_N", 50)
USE_MARK_PRICE = getattr(config, "USE_MARK_PRICE", False)
KLINE_LIMIT = getattr(config, "KLINE_LIMIT", 220)

OPEN_ORDERS_CSV = getattr(config, "OPEN_ORDERS_CSV", "open_orders.csv")
CLOSED_ORDERS_CSV = getattr(config, "CLOSED_ORDERS_CSV", "closed_orders.csv")
HISTORY_ORDERS_CSV = getattr(config, "HISTORY_ORDERS_CSV", "history_orders.csv")

OPEN_POSITIONS_CSV = getattr(config, "OPEN_POSITIONS_CSV", "open_positions.csv")
CLOSED_POSITIONS_CSV = getattr(config, "CLOSED_POSITIONS_CSV", "closed_positions.csv")
HISTORY_POSITIONS_CSV = getattr(config, "HISTORY_POSITIONS_CSV", "history_positions.csv")

EVENT_LOG_CSV = getattr(config, "EVENT_LOG_CSV", "event_log.csv")
SCORE_FILE = getattr(config, "SCORE_FILE", "score.txt")

ENTRY_INTERVAL = getattr(config, "ENTRY_INTERVAL", "1h")
ENTRY_CONFIRM_INTERVAL = getattr(config, "ENTRY_CONFIRM_INTERVAL", "15m")

MIN_SIGNAL_SCORE = getattr(config, "MIN_SIGNAL_SCORE", 2)
RECALCULATE_OPEN_ORDERS = getattr(config, "RECALCULATE_OPEN_ORDERS", True)
ORDER_EXPIRY_HOURS = getattr(config, "ORDER_EXPIRY_HOURS", 8)
BLOCK_EXISTING_SYMBOL_STATE = getattr(config, "BLOCK_EXISTING_SYMBOL_STATE", True)
CANCEL_IF_TP_PASSED_BEFORE_FILL = getattr(config, "CANCEL_IF_TP_PASSED_BEFORE_FILL", True)

MAX_ENTRY_DRIFT_PCT = getattr(config, "MAX_ENTRY_DRIFT_PCT", 3.0)
MAX_SL_DISTANCE_PCT = getattr(config, "MAX_SL_DISTANCE_PCT", 10.0)
MIN_TP_DISTANCE_PCT = getattr(config, "MIN_TP_DISTANCE_PCT", 0.20)

SL_ATR_BUFFER_MULT = getattr(config, "SL_ATR_BUFFER_MULT", 0.2)
TP_RR_MULT = getattr(config, "TP_RR_MULT", 1.8)

ADAPTIVE_MODE_ENABLED = getattr(config, "ADAPTIVE_MODE_ENABLED", False)
ADAPTIVE_MODE = getattr(config, "ADAPTIVE_MODE", None)
MODE_NORMAL = getattr(config, "MODE_NORMAL", {})
MODE_DEFENSIVE = getattr(config, "MODE_DEFENSIVE", {})


# =========================================================
# Headers
# =========================================================

OPEN_ORDER_HEADERS = [
    "order_id",
    "symbol",
    "side",
    "entry_zone_low",
    "entry_zone_high",
    "entry_trigger",
    "sl",
    "tp",
    "rr",
    "score",
    "tf_context",
    "setup_type",
    "setup_reason",
    "created_at",
    "status",
    "live_price",
    "zone_touched",
]

CLOSED_ORDER_HEADERS = [
    "order_id",
    "symbol",
    "side",
    "entry_zone_low",
    "entry_zone_high",
    "entry_trigger",
    "sl",
    "tp",
    "rr",
    "score",
    "tf_context",
    "setup_type",
    "setup_reason",
    "created_at",
    "closed_at",
    "status",
    "close_reason",
    "close_price",
]

HISTORY_ORDER_HEADERS = [
    "order_id",
    "symbol",
    "side",
    "entry_zone_low",
    "entry_zone_high",
    "entry_trigger",
    "sl",
    "tp",
    "rr",
    "score",
    "tf_context",
    "setup_type",
    "setup_reason",
    "created_at",
    "status",
]

OPEN_POSITION_HEADERS = [
    "position_id",
    "order_id",
    "symbol",
    "side",
    "entry",
    "sl",
    "tp",
    "opened_at",
    "trigger_price",
    "status",
    "live_price",
    "pnl_pct",
    "partial_taken",
]

CLOSED_POSITION_HEADERS = [
    "position_id",
    "order_id",
    "symbol",
    "side",
    "entry",
    "sl",
    "tp",
    "opened_at",
    "closed_at",
    "close_reason",
    "close_price",
    "pnl_pct",
    "score_after_close",
    "status",
    "partial_taken",
]

HISTORY_POSITION_HEADERS = [
    "position_id",
    "order_id",
    "symbol",
    "side",
    "entry",
    "sl",
    "tp",
    "opened_at",
    "trigger_price",
    "status",
    "partial_taken"
]

EVENT_HEADERS = ["time", "event", "symbol", "side", "details", "score"]


# =========================================================
# Core helpers
# =========================================================

@contextmanager
def file_lock(lock_name: str = "engine.lock"):
    with open(lock_name, "w") as f:
        fcntl.flock(f, fcntl.LOCK_EX)
        try:
            yield
        finally:
            fcntl.flock(f, fcntl.LOCK_UN)


def utc_now() -> str:
    return datetime.now(UTC).strftime("%Y-%m-%d %H:%M:%S UTC")


def parse_utc(dt_str: str) -> Optional[datetime]:
    try:
        return datetime.strptime(dt_str, "%Y-%m-%d %H:%M:%S UTC").replace(tzinfo=UTC)
    except Exception:
        return None


def safe_float(value, default: float = 0.0) -> float:
    try:
        if value in (None, "", "None"):
            return default
        return float(value)
    except Exception:
        return default


def safe_get(
    url: str,
    params: dict,
    timeout: Union[int, Tuple[int, int]] = (20, 60),
    retries: int = 5,
    backoff: float = 2,
):
    last_exception = None

    for attempt in range(retries):
        try:
            response = requests.get(url, params=params, timeout=timeout)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            last_exception = e
            print(f"[SAFE_GET] attempt {attempt + 1}/{retries} failed: {e}", flush=True)
            if attempt < retries - 1:
                time.sleep(backoff ** attempt)

    print(f"[SAFE_GET] FINAL FAIL {url} -> {last_exception}", flush=True)
    return None


# =========================================================
# CSV helpers
# =========================================================

def ensure_csv(path: str, headers: List[str]) -> None:
    if not os.path.exists(path):
        with open(path, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=headers)
            writer.writeheader()


def append_csv(path: str, row: dict, headers: List[str]) -> None:
    ensure_csv(path, headers)
    with open(path, "a", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=headers)
        writer.writerow({h: row.get(h, "") for h in headers})


def rewrite_csv(path: str, rows: List[dict], headers: List[str]) -> None:
    dir_name = os.path.dirname(path) or "."
    fd, temp_path = tempfile.mkstemp(dir=dir_name, suffix=".tmp", text=True)
    os.close(fd)

    try:
        with open(temp_path, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=headers)
            writer.writeheader()
            for row in rows:
                writer.writerow({h: row.get(h, "") for h in headers})
        os.replace(temp_path, path)
    finally:
        if os.path.exists(temp_path):
            try:
                os.remove(temp_path)
            except Exception:
                pass


def read_csv(path: str) -> List[dict]:
    if not os.path.exists(path):
        return []
    with open(path, "r", newline="", encoding="utf-8") as f:
        return list(csv.DictReader(f))


def initialize_csvs() -> None:
    ensure_csv(OPEN_ORDERS_CSV, OPEN_ORDER_HEADERS)
    ensure_csv(CLOSED_ORDERS_CSV, CLOSED_ORDER_HEADERS)
    ensure_csv(HISTORY_ORDERS_CSV, HISTORY_ORDER_HEADERS)

    ensure_csv(OPEN_POSITIONS_CSV, OPEN_POSITION_HEADERS)
    ensure_csv(CLOSED_POSITIONS_CSV, CLOSED_POSITION_HEADERS)
    ensure_csv(HISTORY_POSITIONS_CSV, HISTORY_POSITION_HEADERS)

    ensure_csv(EVENT_LOG_CSV, EVENT_HEADERS)

    if not os.path.exists(SCORE_FILE):
        with open(SCORE_FILE, "w", encoding="utf-8") as f:
            f.write("0")


# =========================================================
# Score / event helpers
# =========================================================

def get_score() -> int:
    if not os.path.exists(SCORE_FILE):
        return 0
    try:
        with open(SCORE_FILE, "r", encoding="utf-8") as f:
            return int((f.read() or "0").strip())
    except Exception:
        return 0


def set_score(score: int) -> None:
    with open(SCORE_FILE, "w", encoding="utf-8") as f:
        f.write(str(score))


def add_score(delta: int) -> int:
    score = get_score() + delta
    set_score(score)
    return score


def log_event(event: str, symbol: str = "", side: str = "", details: str = "") -> None:
    append_csv(
        EVENT_LOG_CSV,
        {
            "time": utc_now(),
            "event": event,
            "symbol": symbol,
            "side": side,
            "details": details,
            "score": get_score(),
        },
        EVENT_HEADERS,
    )


# =========================================================
# Exchange / market data helpers
# =========================================================

def tick_to_decimals(tick: float) -> int:
    text = f"{tick:.16f}".rstrip("0")
    if "." not in text:
        return 0
    return len(text.split(".")[1])


def round_price(price: float, decimals: int) -> float:
    return round(price, decimals)


def get_exchange_info():
    data = safe_get("https://fapi.binance.com/fapi/v1/exchangeInfo", {})
    if not data:
        return {"symbols": []}
    return data


def get_top_volume_symbols(top_n: int = TOP_N) -> List[str]:
    tickers = safe_get("https://fapi.binance.com/fapi/v1/ticker/24hr", {})
    if not tickers:
        print("[ERROR] ticker fetch failed, skipping cycle", flush=True)
        return []

    exchange_info = get_exchange_info()
    valid_symbols = set()

    for s in exchange_info["symbols"]:
        if (
            s.get("contractType") == "PERPETUAL"
            and s.get("status") == "TRADING"
            and s.get("quoteAsset") == "USDT"
        ):
            valid_symbols.add(s["symbol"])

    filtered = []
    for t in tickers:
        sym = t.get("symbol")
        if sym in valid_symbols:
            filtered.append((sym, safe_float(t.get("quoteVolume"), 0.0)))

    filtered.sort(key=lambda x: x[1], reverse=True)
    return [sym for sym, _ in filtered[:top_n]]


def build_price_precision_map(symbols: List[str]) -> Dict[str, int]:
    info = get_exchange_info()
    wanted = set(symbols)
    out = {}

    for s in info["symbols"]:
        sym = s["symbol"]
        if sym not in wanted:
            continue

        tick_size = None
        for f in s.get("filters", []):
            if f.get("filterType") == "PRICE_FILTER":
                tick_size = safe_float(f.get("tickSize"))
                break

        out[sym] = tick_to_decimals(tick_size) if tick_size else 4

    return out


def get_live_price(symbol: str) -> float:
    if USE_MARK_PRICE:
        data = safe_get("https://fapi.binance.com/fapi/v1/premiumIndex", {"symbol": symbol})
        if not data:
            return 0.0
        return safe_float(data.get("markPrice"))

    data = safe_get("https://fapi.binance.com/fapi/v1/ticker/price", {"symbol": symbol})
    if not data:
        return 0.0
    return safe_float(data.get("price"))


def get_klines(symbol: str, interval: str, limit: int = KLINE_LIMIT) -> List[dict]:
    data = safe_get(
        "https://fapi.binance.com/fapi/v1/klines",
        {"symbol": symbol, "interval": interval, "limit": limit},
    )
    if not data:
        return []

    candles = []
    for k in data:
        candles.append(
            {
                "open_time": int(k[0]),
                "open": safe_float(k[1]),
                "high": safe_float(k[2]),
                "low": safe_float(k[3]),
                "close": safe_float(k[4]),
                "volume": safe_float(k[5]),
                "close_time": int(k[6]),
            }
        )
    return candles


def klines_to_df(klines: List[dict]) -> pd.DataFrame:
    rows = []
    for k in klines:
        rows.append(
            {
                "open": safe_float(k["open"]),
                "high": safe_float(k["high"]),
                "low": safe_float(k["low"]),
                "close": safe_float(k["close"]),
                "volume": safe_float(k.get("volume", 0.0)),
            }
        )
    return pd.DataFrame(rows)


# =========================================================
# Adaptive mode helpers
# =========================================================

def calculate_mode() -> str:
    if not ADAPTIVE_MODE_ENABLED:
        return "NORMAL"

    score = get_score()
    hot = getattr(ADAPTIVE_MODE, "HOT_SCORE_THRESHOLD", 2) if ADAPTIVE_MODE else 2
    cold = getattr(ADAPTIVE_MODE, "COLD_SCORE_THRESHOLD", -2) if ADAPTIVE_MODE else -2

    if score <= cold:
        return "DEFENSIVE"
    if score >= hot:
        return "NORMAL"
    return "NORMAL"


def get_mode_settings() -> Tuple[str, dict]:
    mode = calculate_mode()
    if mode == "DEFENSIVE":
        return mode, MODE_DEFENSIVE
    return mode, MODE_NORMAL


# =========================================================
# Backward compatibility helpers
# =========================================================

def get_order_entry_zone_low(order: dict) -> float:
    if "entry_zone_low" in order and order.get("entry_zone_low") not in ("", None):
        return safe_float(order.get("entry_zone_low"))
    return safe_float(order.get("entry"))


def get_order_entry_zone_high(order: dict) -> float:
    if "entry_zone_high" in order and order.get("entry_zone_high") not in ("", None):
        return safe_float(order.get("entry_zone_high"))
    return safe_float(order.get("entry"))


def get_order_entry_trigger(order: dict) -> float:
    if "entry_trigger" in order and order.get("entry_trigger") not in ("", None):
        return safe_float(order.get("entry_trigger"))
    return safe_float(order.get("entry"))


def get_order_zone_touched(order: dict) -> bool:
    raw = str(order.get("zone_touched", "")).strip().lower()
    return raw in ("1", "true", "yes", "y")


def set_order_zone_touched(order: dict, value: bool) -> None:
    order["zone_touched"] = "1" if value else "0"


# =========================================================
# Zone / trigger logic
# =========================================================

def price_inside_entry_zone(order: dict, live_price: float) -> bool:
    low = get_order_entry_zone_low(order)
    high = get_order_entry_zone_high(order)
    return low <= live_price <= high


def resolve_entry(order: dict, live_price: float) -> float:
    raw_entry = order.get("entry")
    if raw_entry is not None and str(raw_entry).strip() != "":
        return safe_float(raw_entry)

    zone_low = safe_float(order.get("entry_zone_low"))
    zone_high = safe_float(order.get("entry_zone_high"))

    if zone_low <= live_price <= zone_high:
        return live_price

    if zone_low > 0 and zone_high > 0:
        return (zone_low + zone_high) / 2.0

    trigger = safe_float(order.get("entry_trigger"))
    return trigger if trigger > 0 else live_price


def should_trigger_entry(order: dict, live_price: float) -> bool:
    trigger = get_order_entry_trigger(order)
    side = order["side"]

    if side == "LONG":
        return live_price >= trigger
    return live_price <= trigger


def should_cancel_before_fill(order: dict, live_price: float) -> Optional[str]:
    if not CANCEL_IF_TP_PASSED_BEFORE_FILL:
        return None

    side = order["side"]
    tp = safe_float(order.get("tp"))
    sl = safe_float(order.get("sl"))

    if side == "LONG":
        if live_price >= tp:
            return "TP_PASSED_BEFORE_FILL"
        if live_price <= sl:
            return "SL_PASSED_BEFORE_FILL"
    else:
        if live_price <= tp:
            return "TP_PASSED_BEFORE_FILL"
        if live_price >= sl:
            return "SL_PASSED_BEFORE_FILL"

    return None


# =========================================================
# Order builders / recalculation
# =========================================================

def build_order(symbol: str, precision: int, min_score_override=None, debug=False):
    try:
        klines_1h = get_klines(symbol, ENTRY_INTERVAL, KLINE_LIMIT)
        klines_15m = get_klines(symbol, ENTRY_CONFIRM_INTERVAL, KLINE_LIMIT)

        if not klines_1h or not klines_15m:
            return None, "EMPTY_KLINES"

        df_1h = klines_to_df(klines_1h)
        df_15m = klines_to_df(klines_15m)

        order, reason = evaluate_symbol(
            symbol=symbol,
            precision=precision,
            df_1h_raw=df_1h,
            df_15m_raw=df_15m,
            min_score_override=min_score_override,
        )

        if not order:
            return None, reason

        order["order_id"] = str(uuid.uuid4())[:8]
        order["created_at"] = utc_now()
        order["status"] = "OPEN_ORDER"
        order["live_price"] = round_price(get_live_price(symbol), precision)
        order["zone_touched"] = str(order.get("zone_touched", "0") or "0")

        return order, reason

    except Exception as e:
        return None, f"BUILD_ORDER_ERROR: {e}"


def recalculate_order_levels(existing_order: dict, precision: int) -> Tuple[str, Optional[dict], str]:
    try:
        symbol = existing_order["symbol"]

        klines_1h = get_klines(symbol, ENTRY_INTERVAL, KLINE_LIMIT)
        klines_15m = get_klines(symbol, ENTRY_CONFIRM_INTERVAL, KLINE_LIMIT)

        if not klines_1h or not klines_15m:
            return "INVALID", None, "EMPTY_KLINES"

        df_1h = klines_to_df(klines_1h)
        df_15m = klines_to_df(klines_15m)

        new_order, reason = evaluate_symbol(
            symbol=symbol,
            precision=precision,
            df_1h_raw=df_1h,
            df_15m_raw=df_15m,
            min_score_override=None,
        )

        if not new_order:
            return "INVALID", None, reason

        new_order["order_id"] = existing_order.get("order_id")
        new_order["created_at"] = existing_order.get("created_at")
        new_order["status"] = existing_order.get("status", "OPEN_ORDER")
        new_order["live_price"] = round_price(get_live_price(symbol), precision)
        new_order["zone_touched"] = existing_order.get("zone_touched", "0")

        return "UPDATED", new_order, reason

    except Exception as e:
        return "INVALID", None, f"RECALC_ERROR: {e}"


# =========================================================
# Order state helpers
# =========================================================

def order_expired(order: dict) -> bool:
    created_at = parse_utc(order.get("created_at", ""))
    if created_at is None:
        return False

    age_seconds = (datetime.now(UTC) - created_at).total_seconds()
    return age_seconds > ORDER_EXPIRY_HOURS * 3600


def symbol_has_existing_state(symbol: str, open_orders: List[dict], open_positions: List[dict]) -> bool:
    for row in open_orders:
        if row.get("symbol") == symbol:
            return True
    for row in open_positions:
        if row.get("symbol") == symbol:
            return True
    return False


# =========================================================
# Order / position row helpers
# =========================================================

def history_order_row(order: dict) -> dict:
    return {
        "order_id": order["order_id"],
        "symbol": order["symbol"],
        "side": order["side"],
        "entry_zone_low": get_order_entry_zone_low(order),
        "entry_zone_high": get_order_entry_zone_high(order),
        "entry_trigger": get_order_entry_trigger(order),
        "sl": safe_float(order["sl"]),
        "tp": safe_float(order["tp"]),
        "rr": order.get("rr", ""),
        "score": order.get("score", ""),
        "tf_context": order.get("tf_context", ""),
        "setup_type": order.get("setup_type", ""),
        "setup_reason": order.get("setup_reason", ""),
        "created_at": order.get("created_at", ""),
        "status": order.get("status", "OPEN_ORDER"),
    }


def close_order_row(order: dict, reason: str, close_price: Optional[float] = None) -> dict:
    return {
        "order_id": order["order_id"],
        "symbol": order["symbol"],
        "side": order["side"],
        "entry_zone_low": get_order_entry_zone_low(order),
        "entry_zone_high": get_order_entry_zone_high(order),
        "entry_trigger": get_order_entry_trigger(order),
        "sl": safe_float(order["sl"]),
        "tp": safe_float(order["tp"]),
        "rr": order.get("rr", ""),
        "score": order.get("score", ""),
        "tf_context": order.get("tf_context", ""),
        "setup_type": order.get("setup_type", ""),
        "setup_reason": order.get("setup_reason", ""),
        "created_at": order.get("created_at", ""),
        "closed_at": utc_now(),
        "status": "CLOSED_ORDER",
        "close_reason": reason,
        "close_price": "" if close_price is None else close_price,
    }


def open_position_from_order(order: dict, trigger_price: float) -> dict:
    return {
        "position_id": str(uuid.uuid4())[:8],
        "order_id": order["order_id"],
        "symbol": order["symbol"],
        "side": order["side"],
        "entry": resolve_entry(order, trigger_price),
        "sl": safe_float(order["sl"]),
        "tp": safe_float(order["tp"]),
        "opened_at": utc_now(),
        "trigger_price": trigger_price,
        "status": "OPEN_POSITION",
        "live_price": trigger_price,
        "pnl_pct": 0.0,
    }


def history_position_row(position: dict) -> dict:
    return {
        "position_id": position["position_id"],
        "order_id": position["order_id"],
        "symbol": position["symbol"],
        "side": position["side"],
        "entry": safe_float(position["entry"]),
        "sl": safe_float(position["sl"]),
        "tp": safe_float(position["tp"]),
        "opened_at": position["opened_at"],
        "trigger_price": safe_float(position.get("trigger_price")),
        "status": position.get("status", "OPEN_POSITION"),
    }


def close_position_row(
    position: dict,
    reason: str,
    close_price: float,
    score_after_close: Optional[int] = None,
) -> dict:
    if score_after_close is None:
        score_after_close = get_score()
    side = position["side"]

    return {
        "position_id": position["position_id"],
        "order_id": position["order_id"],
        "symbol": position["symbol"],
        "side": side,
        "entry": safe_float(position["entry"]),
        "sl": safe_float(position["sl"]),
        "tp": safe_float(position["tp"]),
        "opened_at": position["opened_at"],
        "closed_at": utc_now(),
        "close_reason": reason,
        "close_price": close_price,
        "pnl_pct": round(position_pnl_percent(side, position, close_price), 4),
        "score_after_close": score_after_close,
        "status": "CLOSED_POSITION",
    }


# =========================================================
# Position helpers
# =========================================================

def position_pnl_percent(side, entry_price, live_price):
    if entry_price == 0:
        return 0.0

    side = str(side).upper()

    if side == "LONG":
        return ((live_price - entry_price) / entry_price) * 100
    elif side == "SHORT":
        return ((entry_price - live_price) / entry_price) * 100
    else:
        return 0.0


def should_close_position(position: dict, live_price: float) -> Optional[str]:
    side = position["side"]
    sl = safe_float(position["sl"])
    tp = safe_float(position["tp"])

    if side == "LONG":
        if live_price <= sl:
            return "SL"
        if live_price >= tp:
            return "TP"
    else:
        if live_price >= sl:
            return "SL"
        if live_price <= tp:
            return "TP"

    return None