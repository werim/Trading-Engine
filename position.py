#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from __future__ import annotations

import os
import time
from typing import Any, Dict, List, Optional

from env import load_env
load_env()

from config import CONFIG
from utils import (
    append_closed_position,
    append_open_position,
    fmt_price,
    get_open_positions,
    has_existing_position,
    is_real_mode,
    load_open_orders,
    log_event,
    log_message,
    now_utc,
    safe_get_live_price,
    save_open_positions,
    to_float,
    write_open_orders,
)
from telegram_alert import (
    build_open_position_message,
    build_sl_message,
    build_tp_message,
    send_telegram_message,
)
from binance_real import BinanceFuturesClient


client = BinanceFuturesClient(
    api_key=os.getenv("BINANCE_API_KEY", "").strip(),
    api_secret=os.getenv("BINANCE_API_SECRET", "").strip(),
    testnet=os.getenv("BINANCE_TESTNET", "true").strip().lower() == "true",
)

POSITION_LOG_FILE = CONFIG.TRADE.POSITION_LOG_FILE
WORKING_TYPE = getattr(CONFIG.TRADE, "WORKING_TYPE", "CONTRACT_PRICE")

ENTRY_FEE_RATE = getattr(CONFIG.TRADE, "ENTRY_FEE_RATE", 0.0004)
EXIT_FEE_RATE = getattr(CONFIG.TRADE, "EXIT_FEE_RATE", 0.0004)


def calc_fees_usdt(entry: float, exit_price: float, qty: float) -> float:
    entry_notional = entry * qty
    exit_notional = exit_price * qty
    return (entry_notional * ENTRY_FEE_RATE) + (exit_notional * EXIT_FEE_RATE)


def calc_real_pnl_usdt(entry: float, exit_price: float, qty: float, side: str) -> float:
    side = str(side).upper()
    if side == "LONG":
        gross = (exit_price - entry) * qty
    else:
        gross = (entry - exit_price) * qty
    return gross - calc_fees_usdt(entry, exit_price, qty)


def calc_real_pnl_pct(entry: float, exit_price: float, side: str) -> float:
    if entry <= 0:
        return 0.0
    side = str(side).upper()
    if side == "LONG":
        gross_pct = (exit_price - entry) / entry
    else:
        gross_pct = (entry - exit_price) / entry
    return (gross_pct - ENTRY_FEE_RATE - EXIT_FEE_RATE) * 100


def get_live_price(symbol: str) -> float:
    price = safe_get_live_price(symbol)
    if price is None:
        log_message(f"LIVE_PRICE_FETCH_FAIL {symbol}", POSITION_LOG_FILE)
        return 0.0
    return float(price)


def get_real_position(symbol: str) -> Optional[Dict[str, Any]]:
    try:
        rows = client.position_risk(symbol=symbol)
        if isinstance(rows, dict):
            rows = [rows]

        for row in rows:
            qty = abs(float(row.get("positionAmt", 0) or 0))
            if qty > 0:
                return row
        return None
    except Exception as e:
        log_message(f"POSITION_RISK_FAIL {symbol} error={e}", POSITION_LOG_FILE)
        return None


def has_open_exchange_entry_order(symbol: str, exchange_order_id: str) -> bool:
    if not exchange_order_id:
        return False

    try:
        orders = client.open_orders(symbol=symbol)
        for order in orders:
            if str(order.get("orderId", "")) == str(exchange_order_id):
                return True
            if str(order.get("algoId", "")) == str(exchange_order_id):
                return True
        return False
    except Exception as e:
        log_message(
            f"OPEN_ORDER_CHECK_FAIL {symbol} exchange_order_id={exchange_order_id} error={e}",
            POSITION_LOG_FILE,
        )
        return False


def place_stop_loss_order(pos: Dict[str, Any]) -> str:
    symbol = pos["symbol"]
    side = str(pos["side"]).upper()
    sl = float(pos["sl"])
    qty = abs(float(pos["qty"]))
    close_side = "SELL" if side == "LONG" else "BUY"

    resp = client.place_stop_market_order(
        symbol=symbol,
        side=close_side,
        trigger_price=sl,
        quantity=qty,
        close_position=None,
        position_side=None,
        working_type=WORKING_TYPE,
    )
    return str(resp.get("algoId") or resp.get("orderId") or "")


def place_take_profit_order(pos: Dict[str, Any]) -> str:
    symbol = pos["symbol"]
    side = str(pos["side"]).upper()
    tp = float(pos["tp"])
    qty = abs(float(pos["qty"]))
    close_side = "SELL" if side == "LONG" else "BUY"

    resp = client.place_limit_order(
        symbol=symbol,
        side=close_side,
        quantity=qty,
        price=tp,
        time_in_force="GTC",
        reduce_only=True,
    )
    return str(resp.get("orderId") or "")


def build_position_from_order(order: Dict[str, Any], real_pos: Dict[str, Any]) -> Dict[str, Any]:
    qty_signed = float(real_pos.get("positionAmt", 0) or 0)
    qty = abs(qty_signed)
    entry = float(real_pos.get("entryPrice", 0) or 0)

    return {
        "position_id": order.get("order_id", ""),
        "order_id": order.get("exchange_order_id", ""),
        "symbol": order["symbol"],
        "side": order["side"],
        "entry": fmt_price(entry),
        "qty": str(qty),
        "sl": order["sl"],
        "tp": order["tp"],
        "rr": order.get("rr", ""),
        "score": order.get("score", "0"),
        "tf_context": order.get("tf_context", ""),
        "setup_type": order.get("setup_type", ""),
        "setup_reason": order.get("setup_reason", ""),
        "opened_at": now_utc(),
        "updated_at": now_utc(),
        "status": "OPEN_POSITION",
        "live_price": fmt_price(entry),
        "pnl_pct": "0.0000",
        "net_pnl_pct": "0.0000",
        "net_pnl_usdt": "0.0000",
        "fees_usdt": "0.0000",
        "sl_order_id": "",
        "tp_order_id": "",
        "protection_armed": "0",
    }


def reconcile_armed_orders() -> None:
    open_orders = load_open_orders()
    changed = False
    remaining_orders: List[Dict[str, Any]] = []

    for order in open_orders:
        status = str(order.get("status", "OPEN_ORDER"))
        if status != "ARMED_ORDER":
            remaining_orders.append(order)
            continue

        symbol = order["symbol"]
        side = order["side"]
        exchange_order_id = str(order.get("exchange_order_id", "")).strip()

        if is_real_mode():
            real_pos = get_real_position(symbol)

            if real_pos is not None:
                if not has_existing_position(symbol):
                    pos = build_position_from_order(order, real_pos)

                    try:
                        sl_id = place_stop_loss_order(pos)
                        tp_id = place_take_profit_order(pos)
                        pos["sl_order_id"] = sl_id
                        pos["tp_order_id"] = tp_id
                        pos["protection_armed"] = "1"
                    except Exception as e:
                        log_message(f"PROTECTION_ARM_FAIL {symbol} {side} error={e}", POSITION_LOG_FILE)

                    append_open_position(pos)

                    log_message(
                        f"ENTRY FILLED {symbol} {side} entry={pos['entry']} qty={pos['qty']} "
                        f"sl={pos['sl']} tp={pos['tp']} exchange_order_id={exchange_order_id}",
                        POSITION_LOG_FILE,
                    )
                    log_event(
                        "ENTRY_FILLED",
                        symbol,
                        side,
                        f"entry={pos['entry']} qty={pos['qty']} sl={pos['sl']} tp={pos['tp']} "
                        f"exchange_order_id={exchange_order_id}",
                        int(pos.get("score", 0)),
                    )

                    try:
                        send_telegram_message(build_open_position_message(pos))
                    except Exception as e:
                        log_message(f"TELEGRAM_OPEN_POSITION_FAIL {symbol} error={e}", POSITION_LOG_FILE)

                    changed = True
                    continue

                changed = True
                continue

            if exchange_order_id and has_open_exchange_entry_order(symbol, exchange_order_id):
                remaining_orders.append(order)
                continue

            order["status"] = "CANCELLED"
            order["updated_at"] = now_utc()
            changed = True

            log_message(
                f"ARMED ORDER GONE {symbol} {side} exchange_order_id={exchange_order_id}",
                POSITION_LOG_FILE,
            )
            log_event(
                "ARMED_ORDER_GONE",
                symbol,
                side,
                f"exchange_order_id={exchange_order_id}",
                int(order.get("score", 0)),
            )
            continue

        live_price = get_live_price(symbol)
        trigger = to_float(order.get("entry_trigger", 0))

        if live_price <= 0 or trigger <= 0:
            remaining_orders.append(order)
            continue

        filled = (side == "LONG" and live_price >= trigger) or (side == "SHORT" and live_price <= trigger)

        if filled:
            pos = {
                "position_id": order.get("order_id", ""),
                "order_id": order.get("exchange_order_id", ""),
                "symbol": symbol,
                "side": side,
                "entry": fmt_price(trigger),
                "qty": "0",
                "sl": order["sl"],
                "tp": order["tp"],
                "rr": order.get("rr", ""),
                "score": order.get("score", "0"),
                "tf_context": order.get("tf_context", ""),
                "setup_type": order.get("setup_type", ""),
                "setup_reason": order.get("setup_reason", ""),
                "opened_at": now_utc(),
                "updated_at": now_utc(),
                "status": "OPEN_POSITION",
                "live_price": fmt_price(live_price),
                "pnl_pct": "0.0000",
                "net_pnl_pct": "0.0000",
                "net_pnl_usdt": "0.0000",
                "fees_usdt": "0.0000",
                "sl_order_id": "",
                "tp_order_id": "",
                "protection_armed": "1",
            }
            append_open_position(pos)

            log_message(
                f"PAPER ENTRY FILLED {symbol} {side} entry={pos['entry']} trigger={trigger}",
                POSITION_LOG_FILE,
            )
            log_event(
                "ENTRY_FILLED",
                symbol,
                side,
                f"entry={pos['entry']} trigger={trigger}",
                int(pos.get("score", 0)),
            )
            changed = True
            continue

        remaining_orders.append(order)

    if changed:
        write_open_orders(remaining_orders)


def archive_position(pos: Dict[str, Any], status: str, live_price: float, pnl_pct: float) -> None:
    entry = float(pos.get("entry", 0) or 0)
    qty = float(pos.get("qty", 0) or 0)

    fees_usdt = calc_fees_usdt(entry, live_price, qty) if qty > 0 else 0.0
    net_pnl_usdt = calc_real_pnl_usdt(entry, live_price, qty, pos["side"]) if qty > 0 else 0.0
    net_pnl_pct = calc_real_pnl_pct(entry, live_price, pos["side"])

    pos["status"] = status
    pos["live_price"] = fmt_price(live_price)
    pos["pnl_pct"] = f"{pnl_pct:.4f}"
    pos["net_pnl_pct"] = f"{net_pnl_pct:.4f}"
    pos["net_pnl_usdt"] = f"{net_pnl_usdt:.4f}"
    pos["fees_usdt"] = f"{fees_usdt:.4f}"
    pos["updated_at"] = now_utc()

    append_closed_position(pos)

    log_message(
        f"{status} {pos['symbol']} {pos['side']} entry={pos['entry']} "
        f"sl={pos['sl']} tp={pos['tp']} live={fmt_price(live_price)} "
        f"gross={pnl_pct:.4f}% net={net_pnl_pct:.4f}% fees={fees_usdt:.4f}USDT",
        POSITION_LOG_FILE,
    )
    log_event(
        status,
        pos["symbol"],
        pos["side"],
        f"entry={pos['entry']} sl={pos['sl']} tp={pos['tp']} "
        f"live={fmt_price(live_price)} gross={pnl_pct:.4f}% net={net_pnl_pct:.4f}%",
        int(pos.get("score", 0)),
    )

    if status == "TP_HIT":
        try:
            send_telegram_message(build_tp_message(pos))
        except Exception as e:
            log_message(f"TELEGRAM_TP_FAIL {pos['symbol']} error={e}", POSITION_LOG_FILE)
    elif status == "SL_HIT":
        try:
            send_telegram_message(build_sl_message(pos))
        except Exception as e:
            log_message(f"TELEGRAM_SL_FAIL {pos['symbol']} error={e}", POSITION_LOG_FILE)


def update_positions() -> None:
    positions = get_open_positions()
    alive: List[Dict[str, Any]] = []

    for pos in positions:
        symbol = pos["symbol"]
        side = str(pos["side"]).upper()

        try:
            entry = float(pos.get("entry", 0) or 0)
            sl = float(pos.get("sl", 0) or 0)
            tp = float(pos.get("tp", 0) or 0)
        except Exception:
            alive.append(pos)
            continue

        if entry <= 0:
            alive.append(pos)
            continue

        live_price = get_live_price(symbol)
        if live_price <= 0:
            alive.append(pos)
            continue

        pos["live_price"] = fmt_price(live_price)
        pos["updated_at"] = now_utc()

        if side == "LONG":
            pnl_pct = ((live_price - entry) / entry) * 100
            tp_hit_by_price = live_price >= tp if tp > 0 else False
            sl_hit_by_price = live_price <= sl if sl > 0 else False
        else:
            pnl_pct = ((entry - live_price) / entry) * 100
            tp_hit_by_price = live_price <= tp if tp > 0 else False
            sl_hit_by_price = live_price >= sl if sl > 0 else False

        pos["pnl_pct"] = f"{pnl_pct:.4f}"

        if is_real_mode():
            real_pos = get_real_position(symbol)

            if real_pos is None:
                if tp_hit_by_price:
                    archive_position(pos, "TP_HIT", live_price, pnl_pct)
                elif sl_hit_by_price:
                    archive_position(pos, "SL_HIT", live_price, pnl_pct)
                else:
                    archive_position(pos, "CLOSED_ON_BINANCE", live_price, pnl_pct)
                continue

            alive.append(pos)
            continue

        if tp_hit_by_price:
            archive_position(pos, "TP_HIT", live_price, pnl_pct)
            continue

        if sl_hit_by_price:
            archive_position(pos, "SL_HIT", live_price, pnl_pct)
            continue

        alive.append(pos)

    save_open_positions(alive)


def run_position_loop() -> None:
    log_message(
        f"===== POSITION LOOP START mode={CONFIG.ENGINE.EXECUTION_MODE} =====",
        POSITION_LOG_FILE,
    )

    while True:
        try:
            reconcile_armed_orders()
        except Exception as e:
            log_message(f"RECONCILE_ARMED_ERROR error={e}", POSITION_LOG_FILE)

        try:
            update_positions()
        except Exception as e:
            log_message(f"POSITION_LOOP_ERROR error={e}", POSITION_LOG_FILE)

        time.sleep(2)


if __name__ == "__main__":
    reconcile_armed_orders()
    update_positions()