#order.py
import csv
import os
import tempfile
import time
import uuid
from contextlib import contextmanager
from datetime import datetime, UTC, timedelta
from typing import Dict, List, Optional, Tuple, Union

import fcntl
import pandas as pd
import requests

from structure import evaluate_symbol
from config import (
    TOP_N,
    USE_MARK_PRICE,
    KLINE_LIMIT,
    OPEN_ORDERS_CSV,
    CLOSED_ORDERS_CSV,
    HISTORY_ORDERS_CSV,
    OPEN_POSITIONS_CSV,
    CLOSED_POSITIONS_CSV,
    HISTORY_POSITIONS_CSV,
    EVENT_LOG_CSV,
    SCORE_FILE,
    ENTRY_INTERVAL,
    ENTRY_CONFIRM_INTERVAL,
    MIN_SIGNAL_SCORE,
    PRICE_POLL_SECONDS,
    POSITION_CHECK_INTERVAL,
    RECALCULATE_OPEN_ORDERS,
    ORDER_EXPIRY_HOURS,
    BLOCK_EXISTING_SYMBOL_STATE,
    CANCEL_IF_TP_PASSED_BEFORE_FILL,
    MAX_ENTRY_DRIFT_PCT,
    MODE_NORMAL,
    MODE_DEFENSIVE,
    ADAPTIVE_MODE_ENABLED,
    ADAPTIVE_MODE,
)


@contextmanager
def file_lock(lock_name: str = "engine.lock"):
    with open(lock_name, "w") as f:
        fcntl.flock(f, fcntl.LOCK_EX)
        try:
            yield
        finally:
            fcntl.flock(f, fcntl.LOCK_UN)


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
]

EVENT_HEADERS = ["time", "event", "symbol", "side", "details", "score"]


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


# =========================================================
# Backward compatibility
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


def safe_get(
    url: str,
    params: dict,
    timeout: Union[int, Tuple[int, int]] = (5, 20),
    retries: int = 3,
    backoff: float = 1.5,
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


def calculate_mode() -> str:
    if not ADAPTIVE_MODE_ENABLED:
        return "NORMAL"

    score = get_score()
    hot = getattr(ADAPTIVE_MODE, "HOT_SCORE_THRESHOLD", 2)
    cold = getattr(ADAPTIVE_MODE, "COLD_SCORE_THRESHOLD", -2)

    if score <= cold:
        return "DEFENSIVE"
    return "NORMAL"


def get_mode_settings() -> Tuple[str, dict]:
    mode = calculate_mode()
    if mode == "DEFENSIVE":
        return mode, MODE_DEFENSIVE
    return mode, MODE_NORMAL


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
        order["zone_touched"] = "0"

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

        if new_order["side"] != existing_order["side"]:
            return "INVALID", None, "SIDE_CHANGED"

        old_trigger = get_order_entry_trigger(existing_order)
        new_trigger = get_order_entry_trigger(new_order)
        if old_trigger > 0:
            drift_pct = abs((new_trigger - old_trigger) / old_trigger) * 100.0
            if drift_pct > MAX_ENTRY_DRIFT_PCT:
                return "INVALID", None, f"ENTRY_DRIFT_FAIL drift_pct={drift_pct:.2f}"

        new_order["order_id"] = existing_order["order_id"]
        new_order["created_at"] = existing_order["created_at"]
        new_order["status"] = existing_order.get("status", "OPEN_ORDER")
        new_order["live_price"] = round_price(get_live_price(symbol), precision)
        new_order["zone_touched"] = existing_order.get("zone_touched", "0")

        return "UPDATED", new_order, reason

    except Exception as e:
        return "INVALID", None, f"RECALC_ERROR: {e}"


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
        "created_at": order["created_at"],
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
        "created_at": order["created_at"],
        "closed_at": utc_now(),
        "status": "CLOSED_ORDER",
        "close_reason": reason,
        "close_price": "" if close_price is None else close_price,
    }


def price_inside_entry_zone(order: dict, live_price: float) -> bool:
    low = get_order_entry_zone_low(order)
    high = get_order_entry_zone_high(order)
    return low <= live_price <= high


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
    tp = safe_float(order["tp"])
    sl = safe_float(order["sl"])

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


def open_position_from_order(order: dict, trigger_price: float) -> dict:
    return {
        "position_id": str(uuid.uuid4())[:8],
        "order_id": order["order_id"],
        "symbol": order["symbol"],
        "side": order["side"],
        "entry": get_order_entry_trigger(order),
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
        "entry": position["entry"],
        "sl": position["sl"],
        "tp": position["tp"],
        "opened_at": position["opened_at"],
        "trigger_price": position["trigger_price"],
        "status": position.get("status", "OPEN_POSITION"),
    }


def position_pnl_percent(position: dict, current_price: float) -> float:
    entry = safe_float(position["entry"])
    if entry == 0:
        return 0.0

    if position["side"] == "LONG":
        return ((current_price - entry) / entry) * 100.0
    return ((entry - current_price) / entry) * 100.0


def should_close_position(position: dict, live_price: float):
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


def close_position_row(position: dict, reason: str, close_price: float, score_after_close: int) -> dict:
    pnl_pct = position_pnl_percent(position, close_price)
    return {
        "position_id": position["position_id"],
        "order_id": position["order_id"],
        "symbol": position["symbol"],
        "side": position["side"],
        "entry": position["entry"],
        "sl": position["sl"],
        "tp": position["tp"],
        "opened_at": position["opened_at"],
        "closed_at": utc_now(),
        "close_reason": reason,
        "close_price": close_price,
        "pnl_pct": round(pnl_pct, 4),
        "score_after_close": score_after_close,
        "status": "CLOSED_POSITION",
    }


def order_expired(order: dict) -> bool:
    created_at = parse_utc(order.get("created_at", ""))
    if created_at is None:
        return False
    return datetime.now(UTC) - created_at >= timedelta(hours=ORDER_EXPIRY_HOURS)


def symbol_has_existing_state(symbol: str, open_orders: List[dict], open_positions: List[dict]) -> bool:
    for row in open_orders:
        if row.get("symbol") == symbol:
            return True
    for row in open_positions:
        if row.get("symbol") == symbol:
            return True
    return False


def update_open_orders(precision_map: Dict[str, int]) -> None:
    with file_lock("engine_open_orders.lock"):
        open_orders = read_csv(OPEN_ORDERS_CSV)
        open_positions = read_csv(OPEN_POSITIONS_CSV)

        if not open_orders:
            print(f"[{utc_now()}] no open orders to update", flush=True)
            return

        updated_orders = []

        for order in open_orders:
            symbol = order["symbol"]
            precision = precision_map.get(symbol, 4)

            if symbol_has_existing_state(symbol, [], open_positions):
                closed = close_order_row(order, "BLOCKED_BY_EXISTING_STATE")
                append_csv(CLOSED_ORDERS_CSV, closed, CLOSED_ORDER_HEADERS)
                log_event("ORDER_CLOSED", symbol, order["side"], "reason=BLOCKED_BY_EXISTING_STATE")
                print(f"[{utc_now()}] close open order {symbol} reason=BLOCKED_BY_EXISTING_STATE", flush=True)
                continue

            if order_expired(order):
                closed = close_order_row(order, "ORDER_EXPIRED")
                append_csv(CLOSED_ORDERS_CSV, closed, CLOSED_ORDER_HEADERS)
                log_event("ORDER_CLOSED", symbol, order["side"], "reason=ORDER_EXPIRED")
                print(f"[{utc_now()}] close open order {symbol} reason=ORDER_EXPIRED", flush=True)
                continue

            if not RECALCULATE_OPEN_ORDERS:
                updated_orders.append(order)
                continue

            status, recalculated, reason = recalculate_order_levels(order, precision)

            if status != "UPDATED" or recalculated is None:
                closed = close_order_row(order, reason)
                append_csv(CLOSED_ORDERS_CSV, closed, CLOSED_ORDER_HEADERS)
                log_event("ORDER_INVALIDATED", symbol, order["side"], f"reason={reason}")
                print(f"[{utc_now()}] invalidate open order {symbol} reason={reason}", flush=True)
                continue

            updated_orders.append(recalculated)
            print(
                f"[{utc_now()}] updated order {symbol} side={recalculated['side']} "
                f"zone=({get_order_entry_zone_low(recalculated)},{get_order_entry_zone_high(recalculated)}) "
                f"trigger={get_order_entry_trigger(recalculated)} sl={recalculated['sl']} tp={recalculated['tp']}",
                flush=True,
            )

        rewrite_csv(OPEN_ORDERS_CSV, updated_orders, OPEN_ORDER_HEADERS)


def generate_new_orders() -> None:
    with file_lock("engine_generation.lock"):
        print(f"[{utc_now()}] ===== HOURLY ORDER CYCLE START =====", flush=True)

        existing_open_orders = read_csv(OPEN_ORDERS_CSV)
        precision_map_existing = build_price_precision_map([row["symbol"] for row in existing_open_orders]) if existing_open_orders else {}
        update_open_orders(precision_map_existing)

        open_orders = read_csv(OPEN_ORDERS_CSV)
        open_positions = read_csv(OPEN_POSITIONS_CSV)

        mode, settings = get_mode_settings()
        max_pending_orders = settings.get("MAX_PENDING_ORDERS", 4)
        min_signal_score = settings.get("MIN_SIGNAL_SCORE", MIN_SIGNAL_SCORE)

        print(f"[{utc_now()}] MODE: {mode}", flush=True)

        if len(open_orders) >= max_pending_orders:
            print(f"[{utc_now()}] max pending orders reached: {len(open_orders)}", flush=True)
            print(f"[{utc_now()}] ===== HOURLY ORDER CYCLE END =====", flush=True)
            return

        symbols = get_top_volume_symbols(TOP_N)
        print(f"[{utc_now()}] top symbols count: {len(symbols)}", flush=True)

        precision_map = build_price_precision_map(symbols)
        current_open_orders = read_csv(OPEN_ORDERS_CSV)
        current_open_positions = read_csv(OPEN_POSITIONS_CSV)

        for symbol in symbols:
            if len(current_open_orders) >= max_pending_orders:
                break

            if BLOCK_EXISTING_SYMBOL_STATE and symbol_has_existing_state(symbol, current_open_orders, current_open_positions):
                print(f"[{utc_now()}] SKIP {symbol} reason=BLOCKED_BY_EXISTING_STATE", flush=True)
                continue

            precision = precision_map.get(symbol, 4)
            order, reason = build_order(symbol, precision, min_score_override=min_signal_score, debug=True)

            if not order:
                print(f"[{utc_now()}] CANDIDATE FAIL {symbol} reason={reason}", flush=True)
                continue

            append_csv(OPEN_ORDERS_CSV, order, OPEN_ORDER_HEADERS)
            append_csv(HISTORY_ORDERS_CSV, history_order_row(order), HISTORY_ORDER_HEADERS)

            current_open_orders.append(order)

            log_event(
                "NEW_ORDER",
                symbol,
                order["side"],
                (
                    f"zone=({get_order_entry_zone_low(order)},{get_order_entry_zone_high(order)}) "
                    f"trigger={get_order_entry_trigger(order)} sl={order['sl']} tp={order['tp']} "
                    f"rr={order['rr']} score={order.get('score', '')} "
                    f"tf={order.get('tf_context', '')}"
                ),
            )

            print(
                f"[{utc_now()}] NEW ORDER {symbol} side={order['side']} "
                f"zone=({get_order_entry_zone_low(order)},{get_order_entry_zone_high(order)}) "
                f"trigger={get_order_entry_trigger(order)} sl={order['sl']} tp={order['tp']} "
                f"rr={order['rr']} score={order.get('score', '')} "
                f"reason={reason}",
                flush=True,
            )

        final_open_orders = read_csv(OPEN_ORDERS_CSV)
        final_open_positions = read_csv(OPEN_POSITIONS_CSV)

        print(f"[{utc_now()}] total open orders after generation: {len(final_open_orders)}", flush=True)
        print(
            f"[{utc_now()}] existing open orders: {len(final_open_orders)} "
            f"active positions: {len(final_open_positions)}",
            flush=True,
        )
        print(f"[{utc_now()}] ===== HOURLY ORDER CYCLE END =====", flush=True)


def print_snapshot() -> None:
    open_orders = read_csv(OPEN_ORDERS_CSV)
    open_positions = read_csv(OPEN_POSITIONS_CSV)

    print("\n================ ENGINE SNAPSHOT ================")
    print(f"time={utc_now()} score={get_score()}")
    print(f"open_orders={len(open_orders)} open_positions={len(open_positions)}")

    if open_orders:
        print("\nOPEN ORDERS:")
        for o in open_orders[:10]:
            print(
                f"  {o['symbol']:<12} {o['side']:<5} "
                f"zone=({get_order_entry_zone_low(o)},{get_order_entry_zone_high(o)}) "
                f"trigger={get_order_entry_trigger(o)} "
                f"sl={o['sl']} tp={o['tp']} rr={o.get('rr', '')} "
                f"score={o.get('score', '')} live={o.get('live_price', '')} "
                f"touched={get_order_zone_touched(o)}"
            )

    if open_positions:
        print("\nOPEN POSITIONS:")
        for p in open_positions[:10]:
            print(
                f"  {p['symbol']:<12} {p['side']:<5} entry={p['entry']} "
                f"sl={p['sl']} tp={p['tp']} live={p.get('live_price', '')} "
                f"pnl={p.get('pnl_pct', '')}%"
            )

    print("=================================================\n")


def run_hourly_cycle():
    initialize_csvs()
    generate_new_orders()

def main():
    initialize_csvs()
    print(f"[{utc_now()}] order engine started", flush=True)

    while True:
        try:
            generate_new_orders()
            print_snapshot()
            sleep_seconds = seconds_until_next_quarter()
            print(f"[{utc_now()}] next run in {sleep_seconds}s", flush=True)
            time.sleep(sleep_seconds)
        except KeyboardInterrupt:
            print("Stopped by user.", flush=True)
            break
        except Exception as e:
            print(f"[{utc_now()}] LOOP ERROR: {e}", flush=True)
            time.sleep(POSITION_CHECK_INTERVAL)


def seconds_until_next_quarter():
    now = datetime.now(UTC)
    next_minute = ((now.minute // 15) + 1) * 15
    if next_minute == 60:
        target = now.replace(minute=0, second=0, microsecond=0) + timedelta(hours=1)
    else:
        target = now.replace(minute=next_minute, second=0, microsecond=0)
    return max(1, int((target - now).total_seconds()))


if __name__ == "__main__":
        main()