from __future__ import annotations

from typing import Any, Dict, List, Optional, Tuple

import adaptive
import binance
import market
import storage
from config import CONFIG
from notifier import notify_position_closed, notify_position_opened
from utils import (
    calc_progress_r,
    make_position_id,
    normalize_status,
    safe_float,
    utc_now_str,
)


OPEN_POSITION_STATUSES = {
    "OPEN_POSITION",
    "PROTECTION_PENDING",
    "PROTECTION_ARMED",
    "PARTIAL_TP_DONE",
    "BREAK_EVEN_ARMED",
    "TRAILING_ACTIVE",
}

FINAL_POSITION_STATUSES = {
    "CLOSED",
    "CANCELLED",
}


def opposite_side(position_side: str) -> str:
    return "SELL" if position_side == "LONG" else "BUY"


def position_is_open(position: Dict[str, Any]) -> bool:
    return normalize_status(position.get("status")) in OPEN_POSITION_STATUSES


def build_position_from_filled_order(order: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    symbol = order.get("symbol", "")
    side = order.get("side", "")
    entry = safe_float(order.get("avg_fill_price")) or safe_float(order.get("entry_trigger"))
    qty = safe_float(order.get("executed_qty")) or safe_float(order.get("submitted_qty"))
    sl = safe_float(order.get("sl"))
    tp = safe_float(order.get("tp"))

    if not symbol or not side or entry <= 0 or qty <= 0 or sl <= 0 or tp <= 0:
        return None

    now = utc_now_str()
    initial_risk = abs(entry - sl) * qty

    return {
        "position_id": make_position_id(symbol, side),
        "order_id": order["order_id"],
        "symbol": symbol,
        "side": side,
        "entry": entry,
        "qty": qty,
        "sl": sl,
        "tp": tp,
        "rr": safe_float(order.get("rr")),
        "score": order.get("score", 0),
        "tf_context": order.get("tf_context", ""),
        "setup_type": order.get("setup_type", ""),
        "setup_reason": order.get("setup_reason", ""),
        "opened_at": now,
        "updated_at": now,
        "status": "PROTECTION_PENDING",
        "live_price": entry,
        "pnl_pct": 0.0,
        "net_pnl_pct": 0.0,
        "net_pnl_usdt": 0.0,
        "fees_usdt": 0.0,
        "sl_order_id": "",
        "tp_order_id": "",
        "protection_armed": 0,
        "partial_taken": 0,
        "break_even_armed": 0,
        "trailing_active": 0,
        "highest_price": entry,
        "lowest_price": entry,
        "initial_qty": qty,
        "initial_risk": initial_risk,
    }


def sync_filled_orders_to_positions() -> None:
    open_orders = storage.load_open_orders()
    open_positions = storage.load_open_positions()

    existing_order_ids = {p.get("order_id") for p in open_positions}
    changed_positions = False

    for order in open_orders:
        if normalize_status(order.get("status")) != "FILLED":
            continue
        if order.get("order_id") in existing_order_ids:
            continue

        position = build_position_from_filled_order(order)
        if not position:
            continue

        notify_position_opened(position)
        open_positions.append(position)
        existing_order_ids.add(order.get("order_id"))
        changed_positions = True

    if changed_positions:
        storage.save_open_positions(open_positions)


def reconcile_position_with_exchange(position: Dict[str, Any]) -> Dict[str, Any]:
    if CONFIG.ENGINE.EXECUTION_MODE != "REAL":
        return position

    try:
        exchange_positions = binance.get_position_risk(position["symbol"])
    except Exception:
        return position

    if not exchange_positions:
        return position

    symbol = position["symbol"]
    side = position["side"]
    matched = None

    for p in exchange_positions:
        if p.get("symbol") != symbol:
            continue
        amt = safe_float(p.get("positionAmt"))
        if side == "LONG" and amt > 0:
            matched = p
            break
        if side == "SHORT" and amt < 0:
            matched = p
            break

    if matched is None:
        if position_is_open(position):
            position["status"] = "CLOSED"
            position["updated_at"] = utc_now_str()
        return position

    exchange_qty = abs(safe_float(matched.get("positionAmt")))
    if exchange_qty > 0:
        position["qty"] = exchange_qty
        entry_price = safe_float(matched.get("entryPrice"))
        if entry_price > 0:
            position["entry"] = entry_price

    position["updated_at"] = utc_now_str()
    return position


def arm_protection_orders(position: Dict[str, Any]) -> Dict[str, Any]:
    if str(position.get("protection_armed")) == "1":
        return position

    symbol = position["symbol"]
    exit_side = opposite_side(position["side"])
    qty = safe_float(position["qty"])
    sl = safe_float(position["sl"])
    tp = safe_float(position["tp"])

    if qty <= 0 or sl <= 0 or tp <= 0:
        return position

    if CONFIG.ENGINE.EXECUTION_MODE == "PAPER":
        position["sl_order_id"] = "paper-sl"
        position["tp_order_id"] = "paper-tp"
        position["protection_armed"] = 1
        position["status"] = "PROTECTION_ARMED"
        position["updated_at"] = utc_now_str()
        return position

    try:
        sl_resp = binance.place_stop_loss(
            symbol=symbol,
            side=exit_side,
            stop_price=sl,
            qty=qty,
            reduce_only=True,
        )
        tp_resp = binance.place_take_profit(
            symbol=symbol,
            side=exit_side,
            stop_price=tp,
            qty=qty,
            reduce_only=True,
        )
        position["sl_order_id"] = sl_resp.get("orderId", "")
        position["tp_order_id"] = tp_resp.get("orderId", "")
        position["protection_armed"] = 1
        position["status"] = "PROTECTION_ARMED"
    except Exception:
        position["status"] = "PROTECTION_PENDING"

    position["updated_at"] = utc_now_str()
    return position


def update_position_live_metrics(position: Dict[str, Any], market_ctx: Dict[str, Any]) -> Dict[str, Any]:
    live = safe_float(market_ctx.get("last_price"))
    entry = safe_float(position.get("entry"))
    qty = safe_float(position.get("qty"))
    side = position.get("side", "")

    if live <= 0 or entry <= 0 or qty <= 0:
        return position

    position["live_price"] = live

    if side == "LONG":
        pnl_pct = ((live - entry) / entry) * 100.0
        gross_pnl_usdt = (live - entry) * qty
        position["highest_price"] = max(safe_float(position.get("highest_price")), live)
        low = safe_float(position.get("lowest_price"))
        position["lowest_price"] = live if low <= 0 else min(low, live)
    else:
        pnl_pct = ((entry - live) / entry) * 100.0
        gross_pnl_usdt = (entry - live) * qty
        position["lowest_price"] = min(safe_float(position.get("lowest_price")) or live, live)
        position["highest_price"] = max(safe_float(position.get("highest_price")), live)

    fees_usdt = abs(entry * qty) * 0.0004 + abs(live * qty) * 0.0004
    net_pnl_usdt = gross_pnl_usdt - fees_usdt
    notional = entry * qty
    net_pnl_pct = (net_pnl_usdt / notional) * 100.0 if notional > 0 else 0.0

    position["pnl_pct"] = pnl_pct
    position["fees_usdt"] = fees_usdt
    position["net_pnl_usdt"] = net_pnl_usdt
    position["net_pnl_pct"] = net_pnl_pct
    position["updated_at"] = utc_now_str()
    return position


def maybe_take_partial(position: Dict[str, Any]) -> Dict[str, Any]:
    if str(position.get("partial_taken")) == "1":
        return position

    progress_r = calc_progress_r(
        entry=safe_float(position["entry"]),
        sl=safe_float(position["sl"]),
        live_price=safe_float(position["live_price"]),
        side=position["side"],
    )
    if progress_r < CONFIG.TRADE.PARTIAL_TP_AT_R:
        return position

    current_qty = safe_float(position["qty"])
    if current_qty <= 0:
        return position

    close_qty = current_qty * CONFIG.TRADE.PARTIAL_CLOSE_RATIO
    remaining_qty = current_qty - close_qty
    if remaining_qty <= 0:
        return position

    position["qty"] = remaining_qty
    position["partial_taken"] = 1
    position["status"] = "PARTIAL_TP_DONE"
    position["updated_at"] = utc_now_str()
    return position


def maybe_move_to_break_even(position: Dict[str, Any]) -> Dict[str, Any]:
    if str(position.get("break_even_armed")) == "1":
        return position

    progress_r = calc_progress_r(
        entry=safe_float(position["entry"]),
        sl=safe_float(position["sl"]),
        live_price=safe_float(position["live_price"]),
        side=position["side"],
    )
    if progress_r < CONFIG.TRADE.BREAK_EVEN_TRIGGER_R:
        return position

    position["sl"] = safe_float(position["entry"])
    position["break_even_armed"] = 1
    position["status"] = "BREAK_EVEN_ARMED"
    position["updated_at"] = utc_now_str()
    return position


def maybe_trail_stop(position: Dict[str, Any]) -> Dict[str, Any]:
    if not CONFIG.TRADE.ENABLE_TRAILING:
        return position

    entry = safe_float(position["entry"])
    current_sl = safe_float(position["sl"])
    progress_r = calc_progress_r(
        entry=entry,
        sl=current_sl,
        live_price=safe_float(position["live_price"]),
        side=position["side"],
    )
    if progress_r < CONFIG.TRADE.TRAIL_AFTER_R:
        return position

    initial_risk_per_unit = abs(entry - safe_float(position.get("sl")))
    if initial_risk_per_unit <= 0:
        initial_risk_per_unit = abs(entry - safe_float(position.get("tp"))) / max(safe_float(position.get("rr")), 1.0)
    if initial_risk_per_unit <= 0:
        return position

    live = safe_float(position["live_price"])
    trail_buffer = initial_risk_per_unit * 0.5
    if position["side"] == "LONG":
        new_sl = max(current_sl, live - trail_buffer)
    else:
        new_sl = min(current_sl, live + trail_buffer)

    if new_sl == current_sl:
        return position

    position["sl"] = new_sl
    position["trailing_active"] = 1
    position["status"] = "TRAILING_ACTIVE"
    position["updated_at"] = utc_now_str()
    return position


def should_close_position(position: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    live = safe_float(position.get("live_price"))
    sl = safe_float(position.get("sl"))
    tp = safe_float(position.get("tp"))
    side = position.get("side", "")

    if live <= 0 or sl <= 0 or tp <= 0:
        return None

    if side == "LONG":
        if live <= sl:
            return {"reason": "SL_HIT", "price": live}
        if live >= tp:
            return {"reason": "TP_HIT", "price": live}
    else:
        if live >= sl:
            return {"reason": "SL_HIT", "price": live}
        if live <= tp:
            return {"reason": "TP_HIT", "price": live}

    return None


def finalize_closed_position(position: Dict[str, Any], close_reason: str, close_price: float) -> None:
    storage.move_open_position_to_closed(position, close_reason, close_price)
    adaptive.record_closed_position(position, close_reason, close_price)


def remove_linked_open_order_if_needed(position: Dict[str, Any]) -> None:
    open_orders = storage.load_open_orders()
    changed = False

    for order in open_orders:
        if order.get("order_id") != position.get("order_id"):
            continue
        if normalize_status(order.get("status")) != "FILLED":
            order["status"] = "FILLED"
            order["updated_at"] = utc_now_str()
            changed = True

    if changed:
        storage.save_open_orders(open_orders)


def on_position_closed(position: Dict[str, Any], close_reason: str, close_price: float) -> None:
    notify_position_closed(position, close_reason, close_price)

    balance_usdt = 1000.0
    if CONFIG.ENGINE.EXECUTION_MODE == "REAL":
        try:
            balance_usdt = binance.get_available_balance("USDT")
        except Exception:
            pass

    storage.append_equity_snapshot_from_state(
        balance_usdt=balance_usdt,
        note=f"{position['symbol']} {close_reason}",
    )


def process_single_position(position: Dict[str, Any], market_ctx: Dict[str, Any]) -> Tuple[Optional[Dict[str, Any]], Optional[Dict[str, Any]]]:
    if normalize_status(position.get("status")) in FINAL_POSITION_STATUSES:
        return None, None

    position = reconcile_position_with_exchange(position)
    if normalize_status(position.get("status")) == "CLOSED":
        return None, {
            "reason": "EXCHANGE_CLOSED",
            "price": safe_float(position.get("live_price")) or safe_float(position.get("entry")),
        }

    position = arm_protection_orders(position)
    position = update_position_live_metrics(position, market_ctx)
    position = maybe_take_partial(position)
    position = maybe_move_to_break_even(position)
    position = maybe_trail_stop(position)

    close_signal = should_close_position(position)
    if close_signal:
        return None, close_signal

    if normalize_status(position.get("status")) not in OPEN_POSITION_STATUSES:
        position["status"] = "OPEN_POSITION"

    return position, None


def process_positions_once() -> None:
    sync_filled_orders_to_positions()

    positions = storage.load_open_positions()
    still_open: List[Dict[str, Any]] = []

    for position in positions:
        symbol = position.get("symbol")
        if not symbol:
            continue

        try:
            market_ctx = market.build_market_context(symbol)
            updated_position, close_signal = process_single_position(position, market_ctx)
        except Exception:
            still_open.append(position)
            continue

        if close_signal:
            close_price = safe_float(close_signal["price"])
            close_reason = close_signal["reason"]
            finalize_closed_position(position, close_reason=close_reason, close_price=close_price)
            remove_linked_open_order_if_needed(position)
            on_position_closed(position, close_reason=close_reason, close_price=close_price)
            continue

        if updated_position is not None:
            still_open.append(updated_position)

    storage.save_open_positions(still_open)


if __name__ == "__main__":
    process_positions_once()
