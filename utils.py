import csv
import json
import math
import os
import threading
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from config import CONFIG


_LOCK = threading.Lock()


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def utc_now_str() -> str:
    return utc_now().strftime("%Y-%m-%d %H:%M:%S UTC")


def ensure_dirs() -> None:
    os.makedirs(CONFIG.ENGINE.DATA_DIR, exist_ok=True)
    os.makedirs(CONFIG.ENGINE.LOG_DIR, exist_ok=True)


def log_message(message: str, logfile: Optional[str] = None) -> None:
    ensure_dirs()
    line = f"[{utc_now_str()}] {message}"
    print(line)

    target = logfile or CONFIG.FILES.ENGINE_LOG_FILE
    with _LOCK:
        with open(target, "a", encoding="utf-8") as f:
            f.write(line + "\n")


def safe_float(value: Any, default: float = 0.0) -> float:
    try:
        if value is None or value == "":
            return default
        return float(value)
    except Exception:
        return default


def safe_int(value: Any, default: int = 0) -> int:
    try:
        if value is None or value == "":
            return default
        return int(float(value))
    except Exception:
        return default


def round_step(value: float, step: float) -> float:
    if step <= 0:
        return value
    return math.floor(value / step) * step


def round_tick(value: float, tick: float) -> float:
    if tick <= 0:
        return value
    return math.floor(value / tick) * tick


def pct_change(entry: float, price: float, side: str) -> float:
    if entry <= 0:
        return 0.0
    if side.upper() == "LONG":
        return ((price - entry) / entry) * 100.0
    return ((entry - price) / entry) * 100.0


def price_distance_pct(a: float, b: float) -> float:
    if a <= 0:
        return 0.0
    return abs((b - a) / a) * 100.0


def estimate_round_trip_cost_pct(use_limit_entry: bool = True) -> float:
    entry_fee = CONFIG.TRADE.MAKER_FEE_PCT if use_limit_entry else CONFIG.TRADE.TAKER_FEE_PCT
    exit_fee = CONFIG.TRADE.TAKER_FEE_PCT
    return entry_fee + exit_fee + CONFIG.TRADE.ROUND_TRIP_SLIPPAGE_PCT


def expected_net_pnl_pct(entry: float, tp: float, side: str, use_limit_entry: bool = True) -> float:
    gross = pct_change(entry, tp, side)
    cost = estimate_round_trip_cost_pct(use_limit_entry=use_limit_entry)
    return gross - cost


def stop_net_loss_pct(entry: float, sl: float, side: str, use_limit_entry: bool = True) -> float:
    gross = pct_change(entry, sl, side)
    cost = estimate_round_trip_cost_pct(use_limit_entry=use_limit_entry)
    return gross - cost


def compute_rr(entry: float, sl: float, tp: float, side: str) -> float:
    risk = abs(entry - sl)
    reward = abs(tp - entry)
    if risk <= 0:
        return 0.0
    return reward / risk


def file_exists(path: str) -> bool:
    return os.path.exists(path) and os.path.isfile(path)


def read_json(path: str, default: Any) -> Any:
    if not file_exists(path):
        return default
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return default


def write_json(path: str, data: Any) -> None:
    ensure_dirs()
    with _LOCK:
        with open(path, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)


def read_csv(path: str) -> List[Dict[str, Any]]:
    if not file_exists(path):
        return []
    try:
        with open(path, "r", encoding="utf-8", newline="") as f:
            rows = list(csv.DictReader(f))
            return rows
    except Exception:
        return []


def write_csv(path: str, rows: List[Dict[str, Any]], fieldnames: List[str]) -> None:
    ensure_dirs()
    with _LOCK:
        with open(path, "w", encoding="utf-8", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            for row in rows:
                writer.writerow({k: row.get(k, "") for k in fieldnames})


def append_csv_row(path: str, row: Dict[str, Any], fieldnames: List[str]) -> None:
    ensure_dirs()
    exists = file_exists(path)
    with _LOCK:
        with open(path, "a", encoding="utf-8", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            if not exists:
                writer.writeheader()
            writer.writerow({k: row.get(k, "") for k in fieldnames})


def order_fieldnames() -> List[str]:
    return [
        "order_id", "symbol", "side",
        "entry_zone_low", "entry_zone_high", "entry_trigger",
        "sl", "tp", "rr", "score",
        "tf_context", "setup_type", "setup_reason",
        "created_at", "updated_at", "expires_at",
        "status", "live_price",
        "zone_touched", "alarm_touched_sent", "alarm_near_trigger_sent",
        "last_alarm_at", "expected_net_pnl_pct",
        "stop_net_loss_pct", "volume_24h_usdt", "spread_pct",
        "funding_rate_pct",
    ]


def position_fieldnames() -> List[str]:
    return [
        "position_id", "order_id", "symbol", "side",
        "entry", "qty", "sl", "tp", "rr", "score",
        "tf_context", "setup_type", "setup_reason",
        "opened_at", "updated_at", "status", "live_price",
        "pnl_pct", "net_pnl_pct", "net_pnl_usdt", "fees_usdt",
        "sl_order_id", "tp_order_id", "protection_armed",
        "partial_taken", "break_even_armed", "highest_price", "lowest_price",
    ]


def new_order_id(symbol: str, side: str) -> str:
    stamp = utc_now().strftime("%H%M%S%f")
    return f"{symbol}-{side}-{stamp}"[-32:]


def new_position_id(symbol: str, side: str) -> str:
    stamp = utc_now().strftime("%H%M%S%f")
    return f"pos-{symbol}-{side}-{stamp}"[-32:]


def price_in_zone(live_price: float, side: str, zone_low: float, zone_high: float) -> bool:
    try:
        live_price = float(live_price)
        zone_low = float(zone_low)
        zone_high = float(zone_high)
    except (TypeError, ValueError):
        return False

    low = min(zone_low, zone_high)
    high = max(zone_low, zone_high)

    side = str(side).upper()
    if side not in {"LONG", "SHORT"}:
        return False

    return low <= live_price <= high


def normalize_side(side: str) -> str:
    return (side or "").upper().strip()


def clamp(value: float, min_value: float, max_value: float) -> float:
    return max(min_value, min(value, max_value))