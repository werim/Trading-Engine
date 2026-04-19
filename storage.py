from __future__ import annotations

import csv
import os
import tempfile
import time
from contextlib import contextmanager
from typing import Any, Dict, Iterable, List, Optional

from config import CONFIG
from utils import read_csv, safe_float, utc_now_str, write_csv_atomic


# =========================================================
# CSV FIELD DEFINITIONS
# =========================================================

OPEN_ORDER_FIELDS = [
    "order_id",
    "client_order_id",
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
    "updated_at",
    "expires_at",
    "status",
    "live_price",
    "exchange_order_id",
    "exchange_status",
    "order_type",
    "submitted_qty",
    "executed_qty",
    "avg_fill_price",
    "zone_touched",
    "alarm_touched_sent",
    "alarm_near_trigger_sent",
    "last_alarm_at",
    "expected_net_pnl_pct",
    "stop_net_loss_pct",
    "volume_24h_usdt",
    "spread_pct",
    "funding_rate_pct",
]

OPEN_POSITION_FIELDS = [
    "position_id",
    "order_id",
    "symbol",
    "side",
    "entry",
    "qty",
    "sl",
    "tp",
    "rr",
    "score",
    "tf_context",
    "setup_type",
    "setup_reason",
    "opened_at",
    "updated_at",
    "status",
    "live_price",
    "pnl_pct",
    "net_pnl_pct",
    "net_pnl_usdt",
    "fees_usdt",
    "sl_order_id",
    "tp_order_id",
    "protection_armed",
    "partial_taken",
    "break_even_armed",
    "trailing_active",
    "highest_price",
    "lowest_price",
    "initial_qty",
    "initial_risk",
]

CLOSED_POSITION_FIELDS = OPEN_POSITION_FIELDS + [
    "closed_at",
    "close_reason",
    "close_price",
]

FILL_FIELDS = [
    "symbol",
    "side",
    "qty",
    "price",
    "order_id",
    "exchange_order_id",
    "filled_at",
]

EQUITY_FIELDS = [
    "timestamp",
    "equity_usdt",
    "balance_usdt",
    "open_pnl_usdt",
    "closed_pnl_usdt",
    "fees_usdt",
    "open_positions",
    "note",
]


# =========================================================
# LOCKS
# =========================================================

LOCK_DIR = "data/.locks"


def _ensure_dir(path: str) -> None:
    os.makedirs(os.path.dirname(path), exist_ok=True)


def _ensure_lock_dir() -> None:
    os.makedirs(LOCK_DIR, exist_ok=True)


def _lock_path(name: str) -> str:
    _ensure_lock_dir()
    return os.path.join(LOCK_DIR, f"{name}.lock")


@contextmanager
def file_lock(name: str, timeout: float = 10.0, poll_interval: float = 0.1):
    """
    Basit cross-process lock.
    os.O_EXCL ile lock dosyası oluşturur.
    stale lock temizliği için mtime kontrolü eklenebilir.
    """
    lockfile = _lock_path(name)
    start = time.time()
    fd: Optional[int] = None

    while True:
        try:
            fd = os.open(lockfile, os.O_CREAT | os.O_EXCL | os.O_WRONLY)
            os.write(fd, str(os.getpid()).encode("utf-8"))
            break
        except FileExistsError:
            if (time.time() - start) > timeout:
                raise TimeoutError(f"Could not acquire lock: {name}")
            time.sleep(poll_interval)

    try:
        yield
    finally:
        try:
            if fd is not None:
                os.close(fd)
        finally:
            try:
                if os.path.exists(lockfile):
                    os.remove(lockfile)
            except OSError:
                pass


# =========================================================
# GENERIC CSV HELPERS
# =========================================================

def _normalize_row(row: Dict[str, Any], fieldnames: List[str]) -> Dict[str, Any]:
    return {field: row.get(field, "") for field in fieldnames}


def _load_rows(path: str) -> List[Dict[str, Any]]:
    return read_csv(path)


def _save_rows(path: str, rows: List[Dict[str, Any]], fieldnames: List[str], lock_name: str) -> None:
    with file_lock(lock_name):
        normalized = [_normalize_row(r, fieldnames) for r in rows]
        write_csv_atomic(path, normalized, fieldnames)


def _append_row(path: str, row: Dict[str, Any], fieldnames: List[str], lock_name: str) -> None:
    with file_lock(lock_name):
        rows = _load_rows(path)
        rows.append(_normalize_row(row, fieldnames))
        write_csv_atomic(path, rows, fieldnames)


def _replace_rows(
    path: str,
    rows: List[Dict[str, Any]],
    fieldnames: List[str],
    lock_name: str,
) -> None:
    _save_rows(path, rows, fieldnames, lock_name)


def _upsert_rows_by_key(
    path: str,
    new_rows: List[Dict[str, Any]],
    key_field: str,
    fieldnames: List[str],
    lock_name: str,
) -> None:
    with file_lock(lock_name):
        existing = _load_rows(path)
        index = {row.get(key_field): row for row in existing if row.get(key_field)}

        for row in new_rows:
            row_key = row.get(key_field)
            if not row_key:
                continue
            index[row_key] = _normalize_row(row, fieldnames)

        merged = list(index.values())
        write_csv_atomic(path, merged, fieldnames)


# =========================================================
# OPEN ORDERS
# =========================================================

def load_open_orders() -> List[Dict[str, Any]]:
    return _load_rows(CONFIG.FILES.OPEN_ORDERS_CSV)


def save_open_orders(rows: List[Dict[str, Any]]) -> None:
    _save_rows(
        CONFIG.FILES.OPEN_ORDERS_CSV,
        rows,
        OPEN_ORDER_FIELDS,
        "open_orders",
    )


def upsert_open_orders(rows: List[Dict[str, Any]]) -> None:
    _upsert_rows_by_key(
        CONFIG.FILES.OPEN_ORDERS_CSV,
        rows,
        "order_id",
        OPEN_ORDER_FIELDS,
        "open_orders",
    )


def find_open_order(order_id: str) -> Optional[Dict[str, Any]]:
    for row in load_open_orders():
        if row.get("order_id") == order_id:
            return row
    return None


def replace_open_order(updated_row: Dict[str, Any]) -> None:
    with file_lock("open_orders"):
        rows = load_open_orders()
        replaced = False
        updated_id = updated_row.get("order_id")

        for i, row in enumerate(rows):
            if row.get("order_id") == updated_id:
                rows[i] = _normalize_row(updated_row, OPEN_ORDER_FIELDS)
                replaced = True
                break

        if not replaced:
            rows.append(_normalize_row(updated_row, OPEN_ORDER_FIELDS))

        write_csv_atomic(CONFIG.FILES.OPEN_ORDERS_CSV, rows, OPEN_ORDER_FIELDS)


def remove_open_order(order_id: str) -> None:
    with file_lock("open_orders"):
        rows = load_open_orders()
        rows = [r for r in rows if r.get("order_id") != order_id]
        write_csv_atomic(CONFIG.FILES.OPEN_ORDERS_CSV, rows, OPEN_ORDER_FIELDS)


# =========================================================
# OPEN POSITIONS
# =========================================================

def load_open_positions() -> List[Dict[str, Any]]:
    return _load_rows(CONFIG.FILES.OPEN_POSITIONS_CSV)


def save_open_positions(rows: List[Dict[str, Any]]) -> None:
    _save_rows(
        CONFIG.FILES.OPEN_POSITIONS_CSV,
        rows,
        OPEN_POSITION_FIELDS,
        "open_positions",
    )


def upsert_open_positions(rows: List[Dict[str, Any]]) -> None:
    _upsert_rows_by_key(
        CONFIG.FILES.OPEN_POSITIONS_CSV,
        rows,
        "position_id",
        OPEN_POSITION_FIELDS,
        "open_positions",
    )


def find_open_position(position_id: str) -> Optional[Dict[str, Any]]:
    for row in load_open_positions():
        if row.get("position_id") == position_id:
            return row
    return None


def replace_open_position(updated_row: Dict[str, Any]) -> None:
    with file_lock("open_positions"):
        rows = load_open_positions()
        replaced = False
        updated_id = updated_row.get("position_id")

        for i, row in enumerate(rows):
            if row.get("position_id") == updated_id:
                rows[i] = _normalize_row(updated_row, OPEN_POSITION_FIELDS)
                replaced = True
                break

        if not replaced:
            rows.append(_normalize_row(updated_row, OPEN_POSITION_FIELDS))

        write_csv_atomic(CONFIG.FILES.OPEN_POSITIONS_CSV, rows, OPEN_POSITION_FIELDS)


def remove_open_position(position_id: str) -> None:
    with file_lock("open_positions"):
        rows = load_open_positions()
        rows = [r for r in rows if r.get("position_id") != position_id]
        write_csv_atomic(CONFIG.FILES.OPEN_POSITIONS_CSV, rows, OPEN_POSITION_FIELDS)


# =========================================================
# CLOSED POSITIONS
# =========================================================

def load_closed_positions() -> List[Dict[str, Any]]:
    return _load_rows(CONFIG.FILES.CLOSED_POSITIONS_CSV)


def append_closed_position(row: Dict[str, Any]) -> None:
    _append_row(
        CONFIG.FILES.CLOSED_POSITIONS_CSV,
        row,
        CLOSED_POSITION_FIELDS,
        "closed_positions",
    )


# =========================================================
# FILLS
# =========================================================

def load_fills() -> List[Dict[str, Any]]:
    return _load_rows(CONFIG.FILES.FILLS_CSV)


def append_fill(row: Dict[str, Any]) -> None:
    _append_row(
        CONFIG.FILES.FILLS_CSV,
        row,
        FILL_FIELDS,
        "fills",
    )


# =========================================================
# EQUITY
# =========================================================

def load_equity_rows() -> List[Dict[str, Any]]:
    return _load_rows(CONFIG.FILES.EQUITY_CSV)


def append_equity_snapshot(
    equity_usdt: float,
    balance_usdt: float,
    open_pnl_usdt: float,
    closed_pnl_usdt: float,
    fees_usdt: float,
    open_positions: int,
    note: str = "",
) -> None:
    row = {
        "timestamp": utc_now_str(),
        "equity_usdt": round(equity_usdt, 8),
        "balance_usdt": round(balance_usdt, 8),
        "open_pnl_usdt": round(open_pnl_usdt, 8),
        "closed_pnl_usdt": round(closed_pnl_usdt, 8),
        "fees_usdt": round(fees_usdt, 8),
        "open_positions": int(open_positions),
        "note": note,
    }
    _append_row(
        CONFIG.FILES.EQUITY_CSV,
        row,
        EQUITY_FIELDS,
        "equity",
    )


def get_latest_equity() -> Optional[Dict[str, Any]]:
    rows = load_equity_rows()
    if not rows:
        return None
    return rows[-1]


def sum_closed_pnl_usdt() -> float:
    total = 0.0
    for row in load_closed_positions():
        total += safe_float(row.get("net_pnl_usdt"))
    return total


def sum_closed_fees_usdt() -> float:
    total = 0.0
    for row in load_closed_positions():
        total += safe_float(row.get("fees_usdt"))
    return total


def sum_open_pnl_usdt() -> float:
    total = 0.0
    for row in load_open_positions():
        total += safe_float(row.get("net_pnl_usdt"))
    return total


def build_equity_snapshot_from_state(
    balance_usdt: float,
    note: str = "",
) -> Dict[str, Any]:
    open_positions = load_open_positions()
    open_pnl = sum(safe_float(p.get("net_pnl_usdt")) for p in open_positions)
    closed_pnl = sum_closed_pnl_usdt()
    fees = sum_closed_fees_usdt() + sum(safe_float(p.get("fees_usdt")) for p in open_positions)

    equity = balance_usdt + open_pnl

    return {
        "equity_usdt": equity,
        "balance_usdt": balance_usdt,
        "open_pnl_usdt": open_pnl,
        "closed_pnl_usdt": closed_pnl,
        "fees_usdt": fees,
        "open_positions": len(open_positions),
        "note": note,
    }


def append_equity_snapshot_from_state(balance_usdt: float, note: str = "") -> None:
    snap = build_equity_snapshot_from_state(balance_usdt=balance_usdt, note=note)
    append_equity_snapshot(
        equity_usdt=snap["equity_usdt"],
        balance_usdt=snap["balance_usdt"],
        open_pnl_usdt=snap["open_pnl_usdt"],
        closed_pnl_usdt=snap["closed_pnl_usdt"],
        fees_usdt=snap["fees_usdt"],
        open_positions=snap["open_positions"],
        note=snap["note"],
    )


# =========================================================
# HIGHER LEVEL HELPERS
# =========================================================

def move_open_position_to_closed(
    position: Dict[str, Any],
    close_reason: str,
    close_price: float,
) -> None:
    """
    Tek transaction gibi davranan yardımcı:
    - open_positions'tan sil
    - closed_positions'a ekle
    - equity snapshot güncellemeye hazır hale getir
    """
    closed_row = dict(position)
    closed_row["closed_at"] = utc_now_str()
    closed_row["close_reason"] = close_reason
    closed_row["close_price"] = close_price
    closed_row["status"] = "CLOSED"

    # Deadlock önlemek için lock sırası sabit
    with file_lock("open_positions"):
        open_rows = load_open_positions()
        open_rows = [r for r in open_rows if r.get("position_id") != position.get("position_id")]
        write_csv_atomic(CONFIG.FILES.OPEN_POSITIONS_CSV, open_rows, OPEN_POSITION_FIELDS)

    with file_lock("closed_positions"):
        closed_rows = load_closed_positions()
        closed_rows.append(_normalize_row(closed_row, CLOSED_POSITION_FIELDS))
        write_csv_atomic(CONFIG.FILES.CLOSED_POSITIONS_CSV, closed_rows, CLOSED_POSITION_FIELDS)


def mark_order_status(order_id: str, new_status: str, exchange_status: str = "") -> None:
    with file_lock("open_orders"):
        rows = load_open_orders()
        changed = False

        for row in rows:
            if row.get("order_id") != order_id:
                continue
            row["status"] = new_status
            row["updated_at"] = utc_now_str()
            if exchange_status:
                row["exchange_status"] = exchange_status
            changed = True
            break

        if changed:
            write_csv_atomic(CONFIG.FILES.OPEN_ORDERS_CSV, rows, OPEN_ORDER_FIELDS)


def prune_final_open_orders(keep_final_rows: bool = True) -> None:
    """
    İstersen open_orders.csv içinde final statüleri tut,
    istersen temizle. Starter sürümde opsiyonel.
    """
    if keep_final_rows:
        return

    final_statuses = {"FILLED", "CANCELLED", "EXPIRED", "REJECTED", "FAILED"}
    with file_lock("open_orders"):
        rows = load_open_orders()
        rows = [r for r in rows if r.get("status") not in final_statuses]
        write_csv_atomic(CONFIG.FILES.OPEN_ORDERS_CSV, rows, OPEN_ORDER_FIELDS)


# =========================================================
# INITIALIZE EMPTY FILES
# =========================================================

def _ensure_csv_exists(path: str, fieldnames: List[str]) -> None:
    if os.path.exists(path):
        return
    _ensure_dir(path)
    write_csv_atomic(path, [], fieldnames)


def initialize_storage() -> None:
    _ensure_csv_exists(CONFIG.FILES.OPEN_ORDERS_CSV, OPEN_ORDER_FIELDS)
    _ensure_csv_exists(CONFIG.FILES.OPEN_POSITIONS_CSV, OPEN_POSITION_FIELDS)
    _ensure_csv_exists(CONFIG.FILES.CLOSED_POSITIONS_CSV, CLOSED_POSITION_FIELDS)
    _ensure_csv_exists(CONFIG.FILES.FILLS_CSV, FILL_FIELDS)
    _ensure_csv_exists(CONFIG.FILES.EQUITY_CSV, EQUITY_FIELDS)