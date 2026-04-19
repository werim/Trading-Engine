import csv
import json
import math
import os
import tempfile
import time
from datetime import datetime, timezone
from decimal import Decimal, ROUND_DOWN
from typing import Any, Dict, List, Optional


def safe_float(value: Any, default: float = 0.0) -> float:
    try:
        if value is None or value == "":
            return default
        return float(value)
    except (TypeError, ValueError):
        return default


def safe_int(value: Any, default: int = 0) -> int:
    try:
        if value is None or value == "":
            return default
        return int(value)
    except (TypeError, ValueError):
        return default


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def utc_now_str() -> str:
    return utc_now().strftime("%Y-%m-%d %H:%M:%S UTC")


def utc_ts_ms() -> int:
    return int(time.time() * 1000)


def pct_diff(a: float, b: float) -> float:
    if b == 0:
        return 0.0
    return ((a - b) / b) * 100.0


def calc_rr(entry: float, sl: float, tp: float, side: str) -> float:
    risk = abs(entry - sl)
    reward = abs(tp - entry)
    if risk <= 0:
        return 0.0
    return reward / risk


def calc_progress_r(entry: float, sl: float, live_price: float, side: str) -> float:
    risk = abs(entry - sl)
    if risk <= 0:
        return 0.0

    if side == "LONG":
        return (live_price - entry) / risk
    return (entry - live_price) / risk


def decimal_places_from_step(step: float) -> int:
    d = Decimal(str(step)).normalize()
    return max(0, -d.as_tuple().exponent)


def round_price_to_tick(price: float, tick_size: float) -> float:
    if tick_size <= 0:
        return price
    return float((Decimal(str(price)) / Decimal(str(tick_size))).quantize(
        Decimal("1"), rounding=ROUND_DOWN
    ) * Decimal(str(tick_size)))


def floor_qty_to_step(qty: float, step_size: float) -> float:
    if step_size <= 0:
        return qty
    return float((Decimal(str(qty)) / Decimal(str(step_size))).quantize(
        Decimal("1"), rounding=ROUND_DOWN
    ) * Decimal(str(step_size)))


def round_qty_to_step(qty: float, step_size: float) -> float:
    return floor_qty_to_step(qty, step_size)


def validate_min_notional(price: float, qty: float, min_notional: float) -> bool:
    return (price * qty) >= min_notional


def make_order_id() -> str:
    return f"{utc_ts_ms()}"


def make_position_id(symbol: str, side: str) -> str:
    return f"pos-{symbol}-{side}-{utc_ts_ms()}"


def make_client_order_id(symbol: str, side: str, setup_type: str, entry: float) -> str:
    entry_key = str(round(entry, 8)).replace(".", "")
    return f"{symbol[:8]}-{side[:1]}-{setup_type[:4]}-{entry_key[-8:]}-{utc_ts_ms()}"[:36]


def is_same_order_intent(a: Dict[str, Any], b: Dict[str, Any], tolerance_ratio: float = 0.001) -> bool:
    if a.get("symbol") != b.get("symbol"):
        return False
    if a.get("side") != b.get("side"):
        return False
    if a.get("setup_type") != b.get("setup_type"):
        return False

    ea = safe_float(a.get("entry_trigger"))
    eb = safe_float(b.get("entry_trigger"))
    tol = max(abs(ea) * tolerance_ratio, 1e-8)
    return abs(ea - eb) <= tol


def normalize_status(value: str) -> str:
    return (value or "").strip().upper()


def is_order_expired(order: Dict[str, Any]) -> bool:
    expires_at = order.get("expires_at")
    if not expires_at:
        return False
    try:
        expiry = datetime.strptime(expires_at, "%Y-%m-%d %H:%M:%S UTC").replace(tzinfo=timezone.utc)
        return utc_now() >= expiry
    except ValueError:
        return False


def ensure_parent_dir(path: str) -> None:
    os.makedirs(os.path.dirname(path), exist_ok=True)


def read_csv(path: str) -> List[Dict[str, Any]]:
    if not os.path.exists(path):
        return []
    with open(path, "r", newline="", encoding="utf-8") as f:
        return list(csv.DictReader(f))


def write_csv_atomic(path: str, rows: List[Dict[str, Any]], fieldnames: List[str]) -> None:
    ensure_parent_dir(path)
    fd, temp_path = tempfile.mkstemp(prefix="tmp_", suffix=".csv", dir=os.path.dirname(path))
    os.close(fd)
    try:
        with open(temp_path, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            for row in rows:
                writer.writerow(row)
        os.replace(temp_path, path)
    finally:
        if os.path.exists(temp_path):
            try:
                os.remove(temp_path)
            except OSError:
                pass


def append_csv_row_atomic(path: str, row: Dict[str, Any], fieldnames: List[str]) -> None:
    rows = read_csv(path)
    rows.append(row)
    write_csv_atomic(path, rows, fieldnames)


def read_json(path: str, default: Optional[Any] = None) -> Any:
    if not os.path.exists(path):
        return default
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def write_json_atomic(path: str, data: Any) -> None:
    ensure_parent_dir(path)
    fd, temp_path = tempfile.mkstemp(prefix="tmp_", suffix=".json", dir=os.path.dirname(path))
    os.close(fd)
    try:
        with open(temp_path, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=2, ensure_ascii=False)
        os.replace(temp_path, path)
    finally:
        if os.path.exists(temp_path):
            try:
                os.remove(temp_path)
            except OSError:
                pass