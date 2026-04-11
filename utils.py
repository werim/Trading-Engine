#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from __future__ import annotations

from env import load_env
load_env()

import csv
import os
import time
import uuid
from datetime import datetime
from typing import Any, Dict, List, Optional

import requests

from config import CONFIG
from binance_real import BinanceFuturesClient


_client = BinanceFuturesClient(
    api_key=os.getenv("BINANCE_API_KEY", "").strip(),
    api_secret=os.getenv("BINANCE_API_SECRET", "").strip(),
    testnet=os.getenv("BINANCE_TESTNET", "true").strip().lower() == "true",
)


# =========================================================
# MODE
# =========================================================
def is_real_mode() -> bool:
    return str(CONFIG.ENGINE.EXECUTION_MODE).upper() == "REAL"


# =========================================================
# FILE PATHS
# =========================================================
def open_orders_file() -> str:
    return CONFIG.TRADE.REAL_OPEN_ORDERS_FILE if is_real_mode() else CONFIG.TRADE.PAPER_OPEN_ORDERS_FILE


def closed_orders_file() -> str:
    return CONFIG.TRADE.REAL_CLOSED_ORDERS_FILE if is_real_mode() else CONFIG.TRADE.PAPER_CLOSED_ORDERS_FILE


def open_positions_file() -> str:
    return CONFIG.TRADE.REAL_OPEN_POSITIONS_FILE if is_real_mode() else CONFIG.TRADE.PAPER_OPEN_POSITIONS_FILE


def closed_positions_file() -> str:
    return CONFIG.TRADE.REAL_CLOSED_POSITIONS_FILE if is_real_mode() else CONFIG.TRADE.PAPER_CLOSED_POSITIONS_FILE


def event_log_file() -> str:
    path = getattr(CONFIG.TRADE, "EVENT_LOG_FILE", "").strip()
    return path or "data/event_log.csv"


# =========================================================
# CORE HELPERS
# =========================================================
def ensure_parent_dir(path: str) -> None:
    if not path:
        return
    parent = os.path.dirname(path)
    if parent:
        os.makedirs(parent, exist_ok=True)


def utc_ts() -> str:
    return datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S UTC")


def now_utc() -> str:
    return utc_ts()


def parse_utc_ts(ts: str) -> Optional[datetime]:
    if not ts:
        return None

    for fmt in ("%Y-%m-%d %H:%M:%S UTC", "%Y-%m-%d %H:%M:%S"):
        try:
            return datetime.strptime(ts, fmt)
        except Exception:
            continue
    return None


def to_float(x: Any, default: float = 0.0) -> float:
    try:
        return float(x)
    except Exception:
        return default


def safe_int(x: Any, default: int = 0) -> int:
    try:
        return int(float(x))
    except Exception:
        return default


def fmt_price(x: Any) -> str:
    try:
        v = float(x)
    except Exception:
        v = 0.0

    if abs(v) >= 1000:
        return f"{v:.2f}"
    if abs(v) >= 1:
        return f"{v:.4f}"
    return f"{v:.8f}".rstrip("0").rstrip(".")


def pct_diff(a: float, b: float) -> float:
    if b == 0:
        return 0.0
    return abs(a - b) / abs(b)


def new_local_id() -> str:
    return uuid.uuid4().hex[:8]


# =========================================================
# LOGGING
# =========================================================
def _safe_append_text(path: str, line: str) -> None:
    if not path:
        print(line, flush=True)
        return

    ensure_parent_dir(path)
    with open(path, "a", encoding="utf-8") as f:
        f.write(line + "\n")
        f.flush()


def log_message(msg: str, file: str) -> None:
    line = f"[{utc_ts()}] {msg}"
    print(line, flush=True)

    try:
        _safe_append_text(file, line)
    except Exception as e:
        print(f"[{utc_ts()}] LOG_WRITE_FAIL file={file} error={e} original={msg}", flush=True)


def append_csv_row(path: str, row: Dict[str, Any]) -> None:
    if not path:
        return

    ensure_parent_dir(path)

    file_exists = os.path.exists(path)
    file_empty = (not file_exists) or os.path.getsize(path) == 0
    fieldnames = list(row.keys())

    with open(path, "a", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        if file_empty:
            writer.writeheader()
        writer.writerow(row)
        f.flush()


def log_event(event: str, symbol: str, side: str, details: str, score: int) -> None:
    row = {
        "time": utc_ts(),
        "event": str(event),
        "symbol": str(symbol),
        "side": str(side),
        "details": str(details),
        "score": int(score),
    }

    path = event_log_file()

    try:
        append_csv_row(path, row)
    except Exception as e:
        fallback = getattr(CONFIG.TRADE, "ORDER_LOG_FILE", "logs/order.log")
        log_message(
            f"EVENT_LOG_WRITE_FAIL file={path} event={event} symbol={symbol} error={e}",
            fallback,
        )


# =========================================================
# HTTP
# =========================================================
def safe_get(
    url: str,
    params: Optional[Dict[str, Any]] = None,
    headers: Optional[Dict[str, str]] = None,
    timeout: int = 10,
    retries: int = 5,
    sleep_sec: int = 2,
):
    last_err = None

    for i in range(retries):
        try:
            r = requests.get(url, params=params, headers=headers, timeout=timeout)
            r.raise_for_status()
            return r
        except Exception as e:
            last_err = e
            print(f"[SAFE_GET] attempt {i + 1}/{retries} failed: {e}", flush=True)
            if i < retries - 1:
                time.sleep(sleep_sec)

    raise last_err


# =========================================================
# CSV HELPERS
# =========================================================
def read_csv_rows(path: str) -> List[Dict[str, Any]]:
    if not path or not os.path.exists(path) or os.path.getsize(path) == 0:
        return []

    try:
        with open(path, newline="", encoding="utf-8") as f:
            return list(csv.DictReader(f))
    except Exception:
        return []


def write_csv_rows(path: str, rows: List[Dict[str, Any]]) -> None:
    if not path:
        return

    ensure_parent_dir(path)
    tmp_path = f"{path}.tmp"

    if not rows:
        with open(tmp_path, "w", encoding="utf-8") as f:
            f.write("")
            f.flush()
        os.replace(tmp_path, path)
        return

    fieldnames = list(rows[0].keys())

    with open(tmp_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)
        f.flush()

    os.replace(tmp_path, path)


# =========================================================
# DATA ACCESS
# =========================================================
def load_open_orders() -> List[Dict[str, Any]]:
    return read_csv_rows(open_orders_file())


def get_open_orders() -> List[Dict[str, Any]]:
    return load_open_orders()


def write_open_orders(rows: List[Dict[str, Any]]) -> None:
    write_csv_rows(open_orders_file(), rows)


def save_open_orders(rows: List[Dict[str, Any]]) -> None:
    write_open_orders(rows)


def append_open_order(row: Dict[str, Any]) -> None:
    rows = load_open_orders()
    rows.append(row)
    write_open_orders(rows)


def append_closed_order(row: Dict[str, Any]) -> None:
    append_csv_row(closed_orders_file(), row)


def get_open_positions() -> List[Dict[str, Any]]:
    return read_csv_rows(open_positions_file())


def load_open_positions() -> List[Dict[str, Any]]:
    return get_open_positions()


def write_open_positions(rows: List[Dict[str, Any]]) -> None:
    write_csv_rows(open_positions_file(), rows)


def save_open_positions(rows: List[Dict[str, Any]]) -> None:
    write_open_positions(rows)


def append_open_position(row: Dict[str, Any]) -> None:
    rows = get_open_positions()
    rows.append(row)
    write_open_positions(rows)


def append_open_positions(row: Dict[str, Any]) -> None:
    append_open_position(row)


def append_closed_position(row: Dict[str, Any]) -> None:
    append_csv_row(closed_positions_file(), row)


def remove_open_position(symbol: str) -> None:
    rows = get_open_positions()
    rows = [r for r in rows if r.get("symbol") != symbol]
    write_open_positions(rows)


# =========================================================
# STATE HELPERS
# =========================================================
def has_existing_position(symbol: str) -> bool:
    for row in get_open_positions():
        if row.get("symbol") == symbol and row.get("status") == "OPEN_POSITION":
            return True
    return False


def has_open_order_for_symbol(symbol: str) -> bool:
    for row in load_open_orders():
        if row.get("symbol") == symbol and row.get("status") in ("OPEN_ORDER", "ARMED_ORDER"):
            return True
    return False


def has_existing_symbol_state(symbol: str) -> bool:
    return has_existing_position(symbol) or has_open_order_for_symbol(symbol)


def order_expired(order: Dict[str, Any]) -> bool:
    exp = parse_utc_ts(order.get("expires_at", ""))
    if not exp:
        return False
    return datetime.utcnow() > exp


# =========================================================
# ORDER / POSITION HELPERS
# =========================================================
def price_in_zone(side: str, live_price: float, zone_low: float, zone_high: float) -> bool:
    low = min(zone_low, zone_high)
    high = max(zone_low, zone_high)
    return low <= live_price <= high


def should_trigger_entry(order: Dict[str, Any], live_price: float) -> bool:
    side = str(order.get("side", "")).upper()
    trigger = to_float(order.get("entry_trigger"))
    touched = str(order.get("zone_touched", "0")) == "1"

    if not touched or trigger <= 0:
        return False

    if side == "LONG":
        return live_price >= trigger
    if side == "SHORT":
        return live_price <= trigger
    return False


def update_zone_touch(order: Dict[str, Any], live_price: float) -> bool:
    zone_low = to_float(order.get("entry_zone_low"))
    zone_high = to_float(order.get("entry_zone_high"))
    touched_now = price_in_zone(order.get("side", ""), live_price, zone_low, zone_high)
    if touched_now:
        order["zone_touched"] = "1"
    return touched_now


def update_zone_touched(order: Dict[str, Any], live_price: float) -> str:
    zone_low = to_float(order.get("entry_zone_low"))
    zone_high = to_float(order.get("entry_zone_high"))
    if price_in_zone(order.get("side", ""), live_price, zone_low, zone_high):
        return "1"
    return order.get("zone_touched", "0")


def set_alarm_mark(order: Dict[str, Any], key: str) -> None:
    order[key] = "1"
    order["last_alarm_at"] = utc_ts()


def can_send_alarm(order: Dict[str, Any]) -> bool:
    return True


def trigger_alarm(msg: str) -> None:
    print(f"ALARM: {msg}", flush=True)


def make_order_row(candidate: Dict[str, Any]) -> Dict[str, Any]:
    now = utc_ts()
    return {
        "order_id": new_local_id(),
        "symbol": candidate["symbol"],
        "side": candidate["side"],
        "entry_zone_low": fmt_price(candidate["entry_zone_low"]),
        "entry_zone_high": fmt_price(candidate["entry_zone_high"]),
        "entry_trigger": fmt_price(candidate["entry_trigger"]),
        "sl": fmt_price(candidate["sl"]),
        "tp": fmt_price(candidate["tp"]),
        "rr": f"{float(candidate['rr']):.2f}",
        "score": str(candidate["score"]),
        "tf_context": candidate.get("tf_context", ""),
        "setup_type": candidate.get("setup_type", ""),
        "setup_reason": candidate.get("setup_reason", ""),
        "created_at": now,
        "updated_at": now,
        "expires_at": candidate.get("expires_at", ""),
        "status": "OPEN_ORDER",
        "live_price": fmt_price(candidate.get("live_price", 0)),
        "zone_touched": "0",
        "alarm_touched_sent": "0",
        "alarm_near_trigger_sent": "0",
        "last_alarm_at": "",
        "exchange_order_placed": "0",
        "exchange_order_id": "",
    }


def make_position_row_from_order(order: Dict[str, Any], entry: float) -> Dict[str, Any]:
    now = utc_ts()
    return {
        "position_id": order.get("order_id", new_local_id()),
        "order_id": order.get("exchange_order_id", ""),
        "symbol": order["symbol"],
        "side": order["side"],
        "entry": fmt_price(entry),
        "sl": order["sl"],
        "tp": order["tp"],
        "rr": order.get("rr", ""),
        "score": order.get("score", "0"),
        "tf_context": order.get("tf_context", ""),
        "setup_type": order.get("setup_type", ""),
        "setup_reason": order.get("setup_reason", ""),
        "opened_at": now,
        "updated_at": now,
        "status": "OPEN_POSITION",
        "live_price": fmt_price(entry),
        "pnl_pct": "0.0000",
    }


# =========================================================
# EXCHANGE PRECISION
# =========================================================
_exchange_info_cache: Optional[Dict[str, Any]] = None


def get_exchange_info() -> Dict[str, Any]:
    global _exchange_info_cache
    if _exchange_info_cache is None:
        _exchange_info_cache = _client.exchange_info()
    return _exchange_info_cache


def get_symbol_info(symbol: str) -> Optional[Dict[str, Any]]:
    info = get_exchange_info()
    for s in info.get("symbols", []):
        if s.get("symbol") == symbol:
            return s
    return None


def _step_decimals(step: float) -> int:
    text = f"{step:.16f}".rstrip("0")
    if "." not in text:
        return 0
    return len(text.split(".")[1])


def round_step(value: float, step: float) -> float:
    if step <= 0:
        return value
    return float(int(value / step) * step)


def round_price(symbol: str, value: float) -> float:
    info = get_symbol_info(symbol)
    if not info:
        return value

    for f in info.get("filters", []):
        if f.get("filterType") == "PRICE_FILTER":
            tick_size = to_float(f.get("tickSize"), 0.0)
            rounded = round_step(value, tick_size)
            decimals = _step_decimals(tick_size)
            return float(f"{rounded:.{decimals}f}")
    return value


def round_qty(symbol: str, value: float) -> float:
    info = get_symbol_info(symbol)
    if not info:
        return value

    for f in info.get("filters", []):
        if f.get("filterType") == "LOT_SIZE":
            step_size = to_float(f.get("stepSize"), 0.0)
            min_qty = to_float(f.get("minQty"), 0.0)
            rounded = round_step(value, step_size)
            if rounded < min_qty:
                rounded = min_qty
            decimals = _step_decimals(step_size)
            return float(f"{rounded:.{decimals}f}")
    return value


# =========================================================
# MARKET DATA
# =========================================================
def get_price(symbol: str) -> float:
    try:
        data = _client.ticker_price(symbol=symbol)
        return float(data.get("price", 0) or 0)
    except Exception:
        return 0.0


def safe_get_live_price(symbol: str) -> Optional[float]:
    price = get_price(symbol)
    if price <= 0:
        return None
    return price


def get_top_symbols(limit: int = 100) -> List[str]:
    try:
        info = _client.exchange_info()
        valid_symbols = set()

        for s in info.get("symbols", []):
            symbol = s.get("symbol", "")
            status = s.get("status", "")
            contract_type = s.get("contractType", "")
            quote_asset = s.get("quoteAsset", "")

            if (
                symbol.endswith("USDT")
                and quote_asset == "USDT"
                and contract_type == "PERPETUAL"
                and status == "TRADING"
            ):
                valid_symbols.add(symbol)

        tickers = safe_get(
            f"{_client.base_url}/fapi/v1/ticker/24hr",
            timeout=10,
            retries=3,
            sleep_sec=1,
        ).json()

        ranked = []
        for t in tickers:
            symbol = t.get("symbol", "")
            if symbol not in valid_symbols:
                continue

            quote_volume = to_float(t.get("quoteVolume", 0))
            ranked.append((symbol, quote_volume))

        ranked.sort(key=lambda x: x[1], reverse=True)
        return [symbol for symbol, _ in ranked[:limit]]

    except Exception as e:
        log_message(f"TOP_SYMBOLS_FETCH_FAIL error={e}", CONFIG.TRADE.ORDER_LOG_FILE)
        base = [
            "BTCUSDT", "ETHUSDT", "SOLUSDT", "BNBUSDT", "XRPUSDT",
            "DOGEUSDT", "ADAUSDT", "AVAXUSDT", "LINKUSDT", "DOTUSDT",
        ]
        return base[:limit]


def get_all_symbols(limit: int = 100) -> List[str]:
    return get_top_symbols(limit=limit)


def get_klines(symbol: str, interval: str, limit: int) -> List[Dict[str, Any]]:
    try:
        raw = _client.klines(symbol=symbol, interval=interval, limit=limit)
        out = []
        for k in raw:
            out.append(
                {
                    "open_time": k[0],
                    "open": to_float(k[1]),
                    "high": to_float(k[2]),
                    "low": to_float(k[3]),
                    "close": to_float(k[4]),
                    "volume": to_float(k[5]),
                    "close_time": k[6],
                }
            )
        return out
    except Exception:
        return []


# =========================================================
# TECHNICALS
# =========================================================
def ema(values: List[float], period: int) -> List[float]:
    if not values:
        return []
    k = 2 / (period + 1)
    out = [values[0]]
    for v in values[1:]:
        out.append(v * k + out[-1] * (1 - k))
    return out


def atr(klines: List[Dict[str, Any]], period: int) -> float:
    if len(klines) < period + 1:
        return 0.0

    trs = []
    for i in range(1, len(klines)):
        high = to_float(klines[i]["high"])
        low = to_float(klines[i]["low"])
        prev_close = to_float(klines[i - 1]["close"])
        tr = max(high - low, abs(high - prev_close), abs(low - prev_close))
        trs.append(tr)

    if len(trs) < period:
        return 0.0

    return sum(trs[-period:]) / period


def classify_trend(closes: List[float]) -> str:
    if len(closes) < max(CONFIG.TRADE.EMA_FAST, CONFIG.TRADE.EMA_MID):
        return "RANGE"

    fast = ema(closes, CONFIG.TRADE.EMA_FAST)[-1]
    mid = ema(closes, CONFIG.TRADE.EMA_MID)[-1]
    last = closes[-1]

    if last > fast > mid:
        return "LONG"
    if last < fast < mid:
        return "SHORT"
    return "RANGE"