# -*- coding: utf-8 -*-
import time
from typing import Any, Dict, List, Optional

from config import CONFIG
from telegram_alert import (
    alert_break_even,
    alert_partial_tp,
    alert_position_closed,
    alert_position_opened,
    alert_position_update,
    alert_trailing_update,
)
from market import get_symbol_meta, get_market_snapshot
from utils import (
    log_message,
    new_position_id,
    order_fieldnames,
    pct_change,
    position_fieldnames,
    price_in_zone,
    read_csv,
    round_step,
    round_tick,
    safe_float,
    utc_now_str,
    write_csv,
)


def get_binance_client():
    """
    Binance client sadece REAL modda gerektiğinde oluşturulur.
    PAPER modda authenticated Binance çağrısı yapılmamalı.
    """
    if CONFIG.ENGINE.EXECUTION_MODE != "REAL":
        return None

    from binance_real import BinanceFuturesClient
    return BinanceFuturesClient()


def load_open_orders() -> List[Dict[str, Any]]:
    return read_csv(CONFIG.FILES.OPEN_ORDERS_CSV)


def save_open_orders(rows: List[Dict[str, Any]]) -> None:
    write_csv(CONFIG.FILES.OPEN_ORDERS_CSV, rows, order_fieldnames())


def load_open_positions() -> List[Dict[str, Any]]:
    return read_csv(CONFIG.FILES.OPEN_POSITIONS_CSV)


def save_open_positions(rows: List[Dict[str, Any]]) -> None:
    write_csv(CONFIG.FILES.OPEN_POSITIONS_CSV, rows, position_fieldnames())


def load_closed_positions() -> List[Dict[str, Any]]:
    return read_csv(CONFIG.FILES.CLOSED_POSITIONS_CSV)


def save_closed_positions(rows: List[Dict[str, Any]]) -> None:
    write_csv(
        CONFIG.FILES.CLOSED_POSITIONS_CSV,
        rows,
        position_fieldnames() + ["closed_at", "close_reason", "close_price"],
    )


def calc_qty(symbol: str, entry: float, symbol_meta: Dict[str, Any]) -> float:
    notional = CONFIG.TRADE.USDT_PER_TRADE * CONFIG.TRADE.LEVERAGE
    raw_qty = notional / entry if entry > 0 else 0.0
    qty = round_step(raw_qty, symbol_meta["qty_step"])
    if qty < symbol_meta["min_qty"]:
        qty = symbol_meta["min_qty"]
    return qty


def side_to_binance(side: str) -> str:
    return "BUY" if side.upper() == "LONG" else "SELL"


def close_side_to_binance(side: str) -> str:
    return "SELL" if side.upper() == "LONG" else "BUY"


def arm_protection(
    client: Any,
    symbol: str,
    side: str,
    qty: float,
    sl: float,
    tp: float,
) -> Dict[str, Any]:
    sl_side = close_side_to_binance(side)
    tp_side = close_side_to_binance(side)

    sl_order = client.place_stop_market(
        symbol=symbol,
        side=sl_side,
        stop_price=sl,
        reduce_only=True,
        quantity=qty,
    )
    tp_order = client.place_take_profit_market(
        symbol=symbol,
        side=tp_side,
        stop_price=tp,
        reduce_only=True,
        quantity=qty,
    )

    return {
        "sl_order_id": str(sl_order.get("orderId", "")),
        "tp_order_id": str(tp_order.get("orderId", "")),
    }


def cancel_existing_protection_if_any(pos: Dict[str, Any]) -> None:
    """
    REAL modda qty değişince eski SL/TP korumalarını iptal edip yeniden kurmak gerekir.
    """
    if CONFIG.ENGINE.EXECUTION_MODE != "REAL":
        return

    client = get_binance_client()
    if client is None:
        return

    symbol = pos["symbol"]
    sl_order_id = str(pos.get("sl_order_id", "")).strip()
    tp_order_id = str(pos.get("tp_order_id", "")).strip()

    try:
        if sl_order_id and sl_order_id != "paper-sl":
            client.cancel_order(symbol=symbol, order_id=int(sl_order_id))
    except Exception as e:
        log_message(
            f"CANCEL_SL_PROTECTION_FAIL {symbol} sl_order_id={sl_order_id} error={e}",
            CONFIG.FILES.POSITION_LOG_FILE,
        )

    try:
        if tp_order_id and tp_order_id != "paper-tp":
            client.cancel_order(symbol=symbol, order_id=int(tp_order_id))
    except Exception as e:
        log_message(
            f"CANCEL_TP_PROTECTION_FAIL {symbol} tp_order_id={tp_order_id} error={e}",
            CONFIG.FILES.POSITION_LOG_FILE,
        )


def rearm_protection_for_position(pos: Dict[str, Any], symbol_meta: Dict[str, Any]) -> None:
    """
    Kalan qty ile korumaları yeniden kur.
    """
    if CONFIG.ENGINE.EXECUTION_MODE != "REAL":
        pos["sl_order_id"] = "paper-sl"
        pos["tp_order_id"] = "paper-tp"
        pos["protection_armed"] = 1
        return

    client = get_binance_client()
    if client is None:
        raise RuntimeError("REAL mode active but Binance client could not be created")

    protection = arm_protection(
        client=client,
        symbol=pos["symbol"],
        side=pos["side"],
        qty=safe_float(pos["qty"]),
        sl=round_tick(safe_float(pos["sl"]), symbol_meta["price_tick"]),
        tp=round_tick(safe_float(pos["tp"]), symbol_meta["price_tick"]),
    )

    pos["sl_order_id"] = protection["sl_order_id"]
    pos["tp_order_id"] = protection["tp_order_id"]
    pos["protection_armed"] = 1 if protection["sl_order_id"] or protection["tp_order_id"] else 0


def execute_partial_close(pos: Dict[str, Any], close_ratio: float, symbol_meta: Dict[str, Any]) -> float:
    """
    Pozisyonun bir kısmını kapatır.
    Dönüş değeri: kapatılan qty
    """
    current_qty = safe_float(pos["qty"])
    if current_qty <= 0:
        return 0.0

    close_ratio = max(0.0, min(close_ratio, 1.0))
    raw_close_qty = current_qty * close_ratio
    close_qty = round_step(raw_close_qty, symbol_meta["qty_step"])

    if close_qty <= 0:
        return 0.0

    # Kalan qty min qty altına düşüyorsa tamamını kapatmaya çalışma, partial atla
    remaining_qty = round_step(current_qty - close_qty, symbol_meta["qty_step"])
    if remaining_qty < symbol_meta["min_qty"]:
        return 0.0

    if CONFIG.ENGINE.EXECUTION_MODE == "REAL":
        client = get_binance_client()
        if client is None:
            raise RuntimeError("REAL mode active but Binance client could not be created")

        client.place_market_order(
            symbol=pos["symbol"],
            side=close_side_to_binance(pos["side"]),
            quantity=close_qty,
            reduce_only=True,
        )

    pos["qty"] = round(remaining_qty, 8)
    pos["updated_at"] = utc_now_str()
    return close_qty


def open_position_from_order(
    order: Dict[str, Any],
    live_price: float,
    symbol_meta: Dict[str, Any],
) -> Dict[str, Any]:
    entry = safe_float(order["entry_trigger"])
    qty = calc_qty(order["symbol"], entry, symbol_meta)

    protection = {"sl_order_id": "", "tp_order_id": ""}

    if CONFIG.ENGINE.EXECUTION_MODE == "REAL":
        client = get_binance_client()
        if client is None:
            raise RuntimeError("REAL mode active but Binance client could not be created")

        client.set_leverage(order["symbol"], CONFIG.TRADE.LEVERAGE)

        if CONFIG.TRADE.USE_LIMIT_ENTRY:
            price = round_tick(entry, symbol_meta["price_tick"])
            client.place_limit_order(
                symbol=order["symbol"],
                side=side_to_binance(order["side"]),
                quantity=qty,
                price=price,
                reduce_only=False,
            )
        else:
            client.place_market_order(
                symbol=order["symbol"],
                side=side_to_binance(order["side"]),
                quantity=qty,
                reduce_only=False,
            )

        protection = arm_protection(
            client=client,
            symbol=order["symbol"],
            side=order["side"],
            qty=qty,
            sl=round_tick(safe_float(order["sl"]), symbol_meta["price_tick"]),
            tp=round_tick(safe_float(order["tp"]), symbol_meta["price_tick"]),
        )
    else:
        protection = {"sl_order_id": "paper-sl", "tp_order_id": "paper-tp"}
        log_message(
            f"PAPER ORDER_TO_POSITION {order['symbol']} {order['side']} entry={entry}",
            CONFIG.FILES.POSITION_LOG_FILE,
        )

    now = utc_now_str()
    return {
        "position_id": new_position_id(order["symbol"], order["side"]),
        "order_id": order["order_id"],
        "symbol": order["symbol"],
        "side": order["side"],
        "entry": round(entry, 8),
        "qty": round(qty, 8),
        "sl": round(safe_float(order["sl"]), 8),
        "tp": round(safe_float(order["tp"]), 8),
        "rr": order["rr"],
        "score": order["score"],
        "tf_context": order["tf_context"],
        "setup_type": order["setup_type"],
        "setup_reason": order["setup_reason"],
        "opened_at": now,
        "updated_at": now,
        "status": "OPEN_POSITION",
        "live_price": round(live_price, 8),
        "pnl_pct": 0.0,
        "net_pnl_pct": 0.0,
        "net_pnl_usdt": 0.0,
        "fees_usdt": 0.0,
        "sl_order_id": protection["sl_order_id"],
        "tp_order_id": protection["tp_order_id"],
        "protection_armed": 1 if protection["sl_order_id"] or protection["tp_order_id"] else 0,
        "partial_taken": 0,
        "break_even_armed": 0,
        "highest_price": round(live_price, 8),
        "lowest_price": round(live_price, 8),
        "initial_qty": round(qty, 8),
        "initial_risk": round(abs(entry - safe_float(order["sl"])), 8),
    }


def process_orders_into_positions() -> None:
    orders = load_open_orders()
    positions = load_open_positions()
    symbol_meta_all = get_symbol_meta()

    active_symbols = {
        p["symbol"]
        for p in positions
        if p.get("status") == "OPEN_POSITION"
    }

    for order in orders:
        if order.get("status") != "OPEN_ORDER":
            continue

        if order["symbol"] in active_symbols:
            continue

        try:
            market = get_market_snapshot(order["symbol"])
            live_price = safe_float(market["price"])
            order["live_price"] = round(live_price, 8)
            order["updated_at"] = utc_now_str()

            zone_low = safe_float(order["entry_zone_low"])
            zone_high = safe_float(order["entry_zone_high"])
            side = str(order["side"]).upper()

            if price_in_zone(live_price, zone_low, zone_high):
                order["zone_touched"] = 1

            if not price_in_zone(live_price, zone_low, zone_high):
                continue

            entry = safe_float(order["entry_trigger"])
            if side == "LONG" and live_price > entry * 1.003:
                continue
            if side == "SHORT" and live_price < entry * 0.997:
                continue

            symbol_meta = symbol_meta_all.get(order["symbol"])
            if not symbol_meta:
                log_message(
                    f"SYMBOL_META_MISSING {order['symbol']}",
                    CONFIG.FILES.POSITION_LOG_FILE,
                )
                continue

            pos = open_position_from_order(order, live_price, symbol_meta)
            positions.append(pos)

            order["status"] = "FILLED_TO_POSITION"
            order["updated_at"] = utc_now_str()

            alert_position_opened(pos)

            log_message(
                f"ORDER_TO_POSITION {order['symbol']} {order['side']} "
                f"entry={pos['entry']} qty={pos['qty']} mode={CONFIG.ENGINE.EXECUTION_MODE}",
                CONFIG.FILES.POSITION_LOG_FILE,
            )

        except Exception as e:
            err = str(e)
            log_message(
                f"ORDER_TO_POSITION_FAIL {order.get('symbol')} "
                f"mode={CONFIG.ENGINE.EXECUTION_MODE} error={err}",
                CONFIG.FILES.POSITION_LOG_FILE,
            )

            if "-2015" in err or "Invalid API-key, IP, or permissions" in err:
                order["status"] = "AUTH_FAILED"
                order["updated_at"] = utc_now_str()
                log_message(
                    f"ORDER_BLOCKED_AUTH {order.get('symbol')} order_id={order.get('order_id')}",
                    CONFIG.FILES.POSITION_LOG_FILE,
                )

    save_open_orders(orders)
    save_open_positions(positions)


def update_positions() -> None:
    positions = load_open_positions()
    closed = load_closed_positions()
    updated_positions: List[Dict[str, Any]] = []
    symbol_meta_all = get_symbol_meta()

    for pos in positions:
        if pos.get("status") != "OPEN_POSITION":
            continue

        try:
            market = get_market_snapshot(pos["symbol"])
            live_price = safe_float(market["price"])
            entry = safe_float(pos["entry"])
            qty = safe_float(pos["qty"])
            side = str(pos["side"]).upper()
            sl = safe_float(pos["sl"])
            tp = safe_float(pos["tp"])
            initial_risk = safe_float(pos.get("initial_risk"))

            if initial_risk <= 0:
                initial_risk = abs(entry - sl)

            gross_pct = pct_change(entry, live_price, side)
            fees_pct = (
                CONFIG.TRADE.MAKER_FEE_PCT
                + CONFIG.TRADE.TAKER_FEE_PCT
                + CONFIG.TRADE.ROUND_TRIP_SLIPPAGE_PCT
            )
            net_pct = gross_pct - fees_pct
            notional = entry * qty
            net_usdt = (net_pct / 100.0) * notional
            fees_usdt = (fees_pct / 100.0) * notional

            pos["live_price"] = round(live_price, 8)
            pos["pnl_pct"] = round(gross_pct, 4)
            pos["net_pnl_pct"] = round(net_pct, 4)
            pos["net_pnl_usdt"] = round(net_usdt, 4)
            pos["fees_usdt"] = round(fees_usdt, 4)
            pos["updated_at"] = utc_now_str()

            pos["highest_price"] = max(safe_float(pos.get("highest_price")), live_price)
            old_lowest = safe_float(pos.get("lowest_price"))
            pos["lowest_price"] = live_price if old_lowest == 0 else min(old_lowest, live_price)

            progress_r = abs(live_price - entry) / initial_risk if initial_risk > 0 else 0.0

            # 1) BREAK EVEN
            if CONFIG.TRADE.ENABLE_TRAILING and not int(float(pos.get("break_even_armed", 0))):
                if progress_r >= CONFIG.TRADE.BREAK_EVEN_TRIGGER_R:
                    pos["sl"] = round(entry, 8)
                    pos["break_even_armed"] = 1
                    log_message(
                        f"BREAK_EVEN_ARMED {pos['symbol']} side={side} entry={entry}",
                        CONFIG.FILES.POSITION_LOG_FILE,
                    )
                    alert_break_even(pos)

                    if CONFIG.ENGINE.EXECUTION_MODE == "REAL":
                        symbol_meta = symbol_meta_all.get(pos["symbol"])
                        if symbol_meta:
                            cancel_existing_protection_if_any(pos)
                            rearm_protection_for_position(pos, symbol_meta)

            # 2) PARTIAL TP
            partial_taken = int(float(pos.get("partial_taken", 0)))
            if partial_taken == 0 and CONFIG.TRADE.PARTIAL_TP_AT_R < 99:
                if progress_r >= CONFIG.TRADE.PARTIAL_TP_AT_R:
                    symbol_meta = symbol_meta_all.get(pos["symbol"])
                    if symbol_meta:
                        closed_qty = execute_partial_close(
                            pos=pos,
                            close_ratio=CONFIG.TRADE.PARTIAL_CLOSE_RATIO,
                            symbol_meta=symbol_meta,
                        )

                        if closed_qty > 0:
                            pos["partial_taken"] = 1

                            log_message(
                                f"PARTIAL_TP_HIT {pos['symbol']} side={side} "
                                f"closed_qty={round(closed_qty, 8)} remaining_qty={pos['qty']} "
                                f"progress_r={round(progress_r, 4)} mode={CONFIG.ENGINE.EXECUTION_MODE}",
                                CONFIG.FILES.POSITION_LOG_FILE,
                            )
                            alert_partial_tp(pos, closed_qty, progress_r)


                            if CONFIG.ENGINE.EXECUTION_MODE == "REAL":
                                cancel_existing_protection_if_any(pos)
                                rearm_protection_for_position(pos, symbol_meta)

            # 3) TRAILING
            if CONFIG.TRADE.ENABLE_TRAILING and progress_r >= CONFIG.TRADE.TRAIL_AFTER_R:
                old_sl = safe_float(pos["sl"])

                if side == "LONG":
                    trail_sl = live_price - (initial_risk * CONFIG.TRADE.TRAIL_FACTOR)
                    if trail_sl > old_sl:
                        pos["sl"] = round(trail_sl, 8)
                else:
                    trail_sl = live_price + (initial_risk * CONFIG.TRADE.TRAIL_FACTOR)
                    if trail_sl < old_sl:
                        pos["sl"] = round(trail_sl, 8)

                if safe_float(pos["sl"]) != old_sl:
                    log_message(
                        f"TRAIL_SL_UPDATE {pos['symbol']} side={side} old_sl={round(old_sl, 8)} "
                        f"new_sl={pos['sl']} progress_r={round(progress_r, 4)}",
                        CONFIG.FILES.POSITION_LOG_FILE,
                    )
                    alert_trailing_update(pos, old_sl, safe_float(pos["sl"]), progress_r)

                    if CONFIG.ENGINE.EXECUTION_MODE == "REAL":
                        symbol_meta = symbol_meta_all.get(pos["symbol"])
                        if symbol_meta:
                            cancel_existing_protection_if_any(pos)
                            rearm_protection_for_position(pos, symbol_meta)

            # 4) EXIT
            close_reason: Optional[str] = None

            if side == "LONG":
                if live_price <= safe_float(pos["sl"]):
                    close_reason = "SL_HIT"
                elif live_price >= tp:
                    close_reason = "TP_HIT"
            else:
                if live_price >= safe_float(pos["sl"]):
                    close_reason = "SL_HIT"
                elif live_price <= tp:
                    close_reason = "TP_HIT"

            if close_reason:
                pos["status"] = "CLOSED"

                closed_row = dict(pos)
                closed_row["closed_at"] = utc_now_str()
                closed_row["close_reason"] = close_reason
                closed_row["close_price"] = round(live_price, 8)
                closed.append(closed_row)

                log_message(
                    f"POSITION_CLOSED {pos['symbol']} reason={close_reason} "
                    f"net_pnl_pct={pos['net_pnl_pct']} mode={CONFIG.ENGINE.EXECUTION_MODE}",
                    CONFIG.FILES.POSITION_LOG_FILE,
                )
                alert_position_closed(pos, close_reason)
            else:
                updated_positions.append(pos)

        except Exception as e:
            log_message(
                f"POSITION_UPDATE_FAIL {pos.get('symbol')} error={e}",
                CONFIG.FILES.POSITION_LOG_FILE,
            )
            updated_positions.append(pos)

    save_open_positions(updated_positions)
    save_closed_positions(closed)


def notify_live_positions() -> None:
    positions = load_open_positions()
    for pos in positions:
        try:
            alert_position_update(pos)
        except Exception:
            pass


def run_position_loop() -> None:
    log_message(
        f"===== POSITION LOOP START mode={CONFIG.ENGINE.EXECUTION_MODE} =====",
        CONFIG.FILES.POSITION_LOG_FILE,
    )

    while True:
        try:
            process_orders_into_positions()
        except Exception as e:
            log_message(
                f"PROCESS_ORDERS_ERROR mode={CONFIG.ENGINE.EXECUTION_MODE} error={e}",
                CONFIG.FILES.POSITION_LOG_FILE,
            )

        try:
            update_positions()
        except Exception as e:
            log_message(
                f"POSITION_LOOP_ERROR mode={CONFIG.ENGINE.EXECUTION_MODE} error={e}",
                CONFIG.FILES.POSITION_LOG_FILE,
            )

        time.sleep(CONFIG.TRADE.POSITION_LOOP_SECONDS)


if __name__ == "__main__":
    run_position_loop()