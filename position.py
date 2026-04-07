#position.py
import time
import uuid
from contextlib import contextmanager
from datetime import datetime, UTC
from typing import Optional

import fcntl
import config

from utils import (
    initialize_csvs,
    read_csv,
    rewrite_csv,
    append_csv,
    get_live_price,
    add_score,
    get_score,
    log_event,
    utc_now,
    close_order_row,
    close_position_row,
    history_position_row,
    position_pnl_percent,
    should_close_position,
)


PRICE_POLL_SECONDS = getattr(config, "PRICE_POLL_SECONDS", 5)
POSITION_CHECK_INTERVAL = getattr(config, "POSITION_CHECK_INTERVAL", PRICE_POLL_SECONDS)

OPEN_ORDERS_CSV = getattr(config, "OPEN_ORDERS_CSV", "open_orders.csv")
CLOSED_ORDERS_CSV = getattr(config, "CLOSED_ORDERS_CSV", "closed_orders.csv")
OPEN_POSITIONS_CSV = getattr(config, "OPEN_POSITIONS_CSV", "open_positions.csv")
CLOSED_POSITIONS_CSV = getattr(config, "CLOSED_POSITIONS_CSV", "closed_positions.csv")
HISTORY_POSITIONS_CSV = getattr(config, "HISTORY_POSITIONS_CSV", "history_positions.csv")

CANCEL_IF_TP_PASSED_BEFORE_FILL = getattr(config, "CANCEL_IF_TP_PASSED_BEFORE_FILL", True)


# Local headers only. Do NOT import these from utils.
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


@contextmanager
def file_lock(lock_name: str = "engine.lock"):
    with open(lock_name, "w") as f:
        fcntl.flock(f, fcntl.LOCK_EX)
        try:
            yield
        finally:
            fcntl.flock(f, fcntl.LOCK_UN)


def safe_float(value, default: float = 0.0) -> float:
    try:
        if value in (None, "", "None"):
            return default
        return float(value)
    except Exception:
        return default


# ---------------------------------------------------------
# Backward compatibility helpers
# ---------------------------------------------------------

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


# ---------------------------------------------------------
# Zone / trigger logic
# ---------------------------------------------------------

def price_inside_entry_zone(order: dict, live_price: float) -> bool:
    low = get_order_entry_zone_low(order)
    high = get_order_entry_zone_high(order)
    return low <= live_price <= high


def resolve_entry(order: dict, live_price: float) -> float:
    if "entry" in order and str(order["entry"]).strip() != "":
        return float(order["entry"])

    zone_low = float(order["entry_zone_low"])
    zone_high = float(order["entry_zone_high"])

    if zone_low <= live_price <= zone_high:
        return live_price

    return (zone_low + zone_high) / 2.0


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


# ---------------------------------------------------------
# Order processing
# ---------------------------------------------------------

def process_open_orders():
    with file_lock("engine_orders.lock"):
        open_orders = read_csv(OPEN_ORDERS_CSV)
        open_positions = read_csv(OPEN_POSITIONS_CSV)

        if not open_orders:
            return

        remaining_orders = []
        active_symbols = {p["symbol"] for p in open_positions}

        for order in open_orders:
            symbol = order["symbol"]
            side = order["side"]

            if symbol in active_symbols:
                closed = close_order_row(order, "BLOCKED_BY_EXISTING_STATE")
                append_csv(CLOSED_ORDERS_CSV, closed, CLOSED_ORDER_HEADERS)
                log_event("ORDER_CLOSED", symbol, side, "reason=BLOCKED_BY_EXISTING_STATE")
                continue

            try:
                live_price = float(get_live_price(symbol))
                if live_price <= 0:
                    remaining_orders.append(order)
                    continue

                order["live_price"] = live_price

                cancel_reason = should_cancel_before_fill(order, live_price)
                if cancel_reason:
                    closed = close_order_row(order, cancel_reason, live_price)
                    append_csv(CLOSED_ORDERS_CSV, closed, CLOSED_ORDER_HEADERS)
                    log_event("ORDER_CLOSED", symbol, side, f"reason={cancel_reason}")
                    print(f"[{utc_now()}] CLOSE ORDER {symbol} reason={cancel_reason} live={live_price}", flush=True)
                    continue

                in_zone = price_inside_entry_zone(order, live_price)
                if in_zone and not get_order_zone_touched(order):
                    set_order_zone_touched(order, True)

                touched = get_order_zone_touched(order)
                triggered = should_trigger_entry(order, live_price)

                print(
                    f"[{utc_now()}] CHECK ORDER {symbol} {side} "
                    f"zone=({get_order_entry_zone_low(order)},{get_order_entry_zone_high(order)}) "
                    f"trigger={get_order_entry_trigger(order)} live={live_price} "
                    f"in_zone={in_zone} touched={touched} triggered={triggered}",
                    flush=True,
                )

                if touched and triggered:
                    closed_order = close_order_row(order, "ENTRY_TRIGGERED", live_price)
                    append_csv(CLOSED_ORDERS_CSV, closed_order, CLOSED_ORDER_HEADERS)

                    position = open_position_from_order(order, live_price)
                    open_positions.append(position)

                    append_csv(
                        HISTORY_POSITIONS_CSV,
                        history_position_row(position),
                        HISTORY_POSITION_HEADERS,
                    )

                    active_symbols.add(symbol)

                    log_event(
                        "ENTRY_TRIGGERED",
                        symbol,
                        side,
                        (
                            f"zone=({get_order_entry_zone_low(order)},{get_order_entry_zone_high(order)}) "
                            f"trigger={get_order_entry_trigger(order)} fill={live_price}"
                        ),
                    )

                    print(
                        f"[{utc_now()}] OPEN POSITION {symbol} {side} "
                        f"trigger={get_order_entry_trigger(order)} fill={live_price}",
                        flush=True,
                    )
                else:
                    remaining_orders.append(order)

            except Exception as e:
                print(f"[{utc_now()}] ORDER PROCESS ERROR {symbol}: {e}", flush=True)
                remaining_orders.append(order)

        rewrite_csv(OPEN_ORDERS_CSV, remaining_orders, OPEN_ORDER_HEADERS)
        rewrite_csv(OPEN_POSITIONS_CSV, open_positions, OPEN_POSITION_HEADERS)


# ---------------------------------------------------------
# Position processing
# ---------------------------------------------------------

def process_open_positions():
    with file_lock("engine_positions.lock"):
        open_positions = read_csv(OPEN_POSITIONS_CSV)
        if not open_positions:
            return

        remaining_positions = []

        for pos in open_positions:
            symbol = pos["symbol"]

            try:
                live_price = float(get_live_price(symbol))
                pnl_pct = round(position_pnl_percent(pos, live_price), 4)

                pos["live_price"] = live_price
                pos["pnl_pct"] = pnl_pct

                close_reason = should_close_position(pos, live_price)

                print(
                    f"[{utc_now()}] CHECK POSITION {symbol} {pos['side']} "
                    f"entry={pos['entry']} sl={pos['sl']} tp={pos['tp']} "
                    f"live={live_price} pnl={pnl_pct}% close_reason={close_reason}",
                    flush=True,
                )

                if close_reason is None:
                    remaining_positions.append(pos)
                    continue

                score_after_close = add_score(1 if close_reason == "TP" else -1)
                closed_pos = close_position_row(pos, close_reason, live_price, score_after_close)
                append_csv(CLOSED_POSITIONS_CSV, closed_pos, CLOSED_POSITION_HEADERS)

                log_event(
                    "POSITION_CLOSED",
                    symbol,
                    pos["side"],
                    (
                        f"reason={close_reason} entry={pos['entry']} close={live_price} "
                        f"pnl={pnl_pct} score={score_after_close}"
                    ),
                )

                print(
                    f"[{utc_now()}] CLOSE POSITION {symbol} {close_reason} "
                    f"entry={pos['entry']} close={live_price} pnl={pnl_pct}%",
                    flush=True,
                )

            except Exception as e:
                print(f"[{utc_now()}] POSITION PROCESS ERROR {symbol}: {e}", flush=True)
                remaining_positions.append(pos)

        rewrite_csv(OPEN_POSITIONS_CSV, remaining_positions, OPEN_POSITION_HEADERS)


# ---------------------------------------------------------
# Snapshot
# ---------------------------------------------------------

def print_snapshot():
    open_orders = read_csv(OPEN_ORDERS_CSV)
    open_positions = read_csv(OPEN_POSITIONS_CSV)

    print("\n================ POSITION SNAPSHOT ================")
    print(f"time={utc_now()} score={get_score()}")
    print(f"open_orders={len(open_orders)} open_positions={len(open_positions)}")

    if open_orders:
        print("\nOPEN ORDERS:")
        for o in open_orders[:10]:
            print(
                f"  {o['symbol']:<12} {o['side']:<5} "
                f"zone=({get_order_entry_zone_low(o)},{get_order_entry_zone_high(o)}) "
                f"trigger={get_order_entry_trigger(o)} "
                f"sl={o.get('sl', '')} tp={o.get('tp', '')} "
                f"rr={o.get('rr', '')} score={o.get('score', '')} "
                f"tf={o.get('tf_context', '')} live={o.get('live_price', '')} "
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

    print("===================================================\n")


def main():
    initialize_csvs()
    print(f"[{utc_now()}] Position engine started", flush=True)

    while True:
        try:
            process_open_orders()
            process_open_positions()
            print_snapshot()
            time.sleep(PRICE_POLL_SECONDS)

        except KeyboardInterrupt:
            print("Stopped by user.", flush=True)
            break

        except Exception as e:
            print(f"[{utc_now()}] LOOP ERROR: {e}", flush=True)
            time.sleep(POSITION_CHECK_INTERVAL)


if __name__ == "__main__":
    main()