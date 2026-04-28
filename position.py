# -*- coding: utf-8 -*-
import time
from typing import Any, Dict, List, Optional, Tuple
from decimal import Decimal, ROUND_DOWN

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
    round_step_size,
    round_tick,
    safe_float,
    utc_now_str,
    write_csv,
)


# =========================================================
# BASIC IO
# =========================================================

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


def save_closed_orders(rows: List[Dict[str, Any]]) -> None:
    write_csv(CONFIG.FILES.CLOSED_ORDERS_CSV, rows, order_fieldnames())


def move_nopen_orders(rows: List[Dict[str, Any]]) -> None:
    open_rows = []
    closed_rows = []

    for row in rows:
        status = row.get("status")

        if status == "OPEN_ORDER":
            open_rows.append(row)
        else:
            closed_rows.append(row)

    # Save only OPEN_ORDER rows back to open_orders.csv
    write_csv(CONFIG.FILES.OPEN_ORDERS_CSV, open_rows, order_fieldnames())

    # Append non-open orders to closed_orders.csv
    if closed_rows:
        existing_closed = read_csv(CONFIG.FILES.CLOSED_ORDERS_CSV)
        all_closed = existing_closed + closed_rows

        write_csv(CONFIG.FILES.CLOSED_ORDERS_CSV, all_closed, order_fieldnames())


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


# =========================================================
# CONFIG SAFE GETTERS
# =========================================================

def _cfg_trade(name: str, default: Any) -> Any:
    return getattr(CONFIG.TRADE, name, default)


def _entry_fill_timeout_seconds() -> int:
    return int(_cfg_trade("ENTRY_FILL_TIMEOUT_SECONDS", 12))


def _entry_fill_poll_seconds() -> float:
    return float(_cfg_trade("ENTRY_FILL_POLL_SECONDS", 1.0))


def _rearm_sleep_seconds() -> float:
    return float(_cfg_trade("PROTECTION_REARM_SLEEP_SECONDS", 0.25))


def _close_reconcile_sleep_seconds() -> float:
    return float(_cfg_trade("CLOSE_RECONCILE_SLEEP_SECONDS", 0.8))


# =========================================================
# PRECISION HELPERS
# =========================================================

def floor_to_step(value: float, step: float) -> float:
    if step <= 0:
        return float(value)

    value_dec = Decimal(str(value))
    step_dec = Decimal(str(step))
    floored = (value_dec / step_dec).to_integral_value(rounding=ROUND_DOWN) * step_dec
    return float(floored)


def format_by_step(value: float, step: float) -> str:
    if step <= 0:
        return format(Decimal(str(value)), "f")

    value_dec = Decimal(str(value))
    step_dec = Decimal(str(step))
    quantized = value_dec.quantize(step_dec, rounding=ROUND_DOWN)
    return format(quantized, "f")


def _normalize_qty(value: float, symbol_meta: Dict[str, Any]) -> float:
    step = _qty_step_from_meta(symbol_meta)
    min_qty = _min_qty_from_meta(symbol_meta)

    qty = floor_to_step(value, step) if step > 0 else float(value)
    if min_qty and qty < min_qty:
        return 0.0
    return qty


def _normalize_price(value: float, symbol_meta: Dict[str, Any]) -> float:
    tick = _price_tick_from_meta(symbol_meta)
    return floor_to_step(value, tick) if tick > 0 else float(value)


def _qty_to_exchange_str(value: float, symbol_meta: Dict[str, Any]) -> str:
    step = _qty_step_from_meta(symbol_meta)
    qty = _normalize_qty(value, symbol_meta)
    return format_by_step(qty, step) if step > 0 else str(qty)


def _price_to_exchange_str(value: float, symbol_meta: Dict[str, Any]) -> str:
    tick = _price_tick_from_meta(symbol_meta)
    price = _normalize_price(value, symbol_meta)
    return format_by_step(price, tick) if tick > 0 else str(price)


# =========================================================
# COMMON HELPERS
# =========================================================

def calc_qty(symbol: str, entry: float, symbol_meta: Dict[str, Any]) -> float:
    notional = CONFIG.TRADE.USDT_PER_TRADE * CONFIG.TRADE.LEVERAGE
    raw_qty = notional / entry if entry > 0 else 0.0
    qty = _normalize_qty(raw_qty, symbol_meta)
    return qty


def side_to_binance(side: str) -> str:
    return "BUY" if str(side).upper() == "LONG" else "SELL"


def close_side_to_binance(side: str) -> str:
    return "SELL" if str(side).upper() == "LONG" else "BUY"


def _price_tick_from_meta(symbol_meta: Dict[str, Any]) -> float:
    direct = safe_float(
        symbol_meta.get("price_tick")
        or symbol_meta.get("tickSize")
        or symbol_meta.get("priceTick")
        or 0.0
    )
    if direct > 0:
        return direct

    for f in symbol_meta.get("filters", []):
        if f.get("filterType") == "PRICE_FILTER":
            return safe_float(f.get("tickSize"))

    return 0.0


def _qty_step_from_meta(symbol_meta: Dict[str, Any]) -> float:
    direct = safe_float(
        symbol_meta.get("qty_step")
        or symbol_meta.get("stepSize")
        or symbol_meta.get("step_size")
        or 0.0
    )
    if direct > 0:
        return direct

    for f in symbol_meta.get("filters", []):
        if f.get("filterType") == "LOT_SIZE":
            return safe_float(f.get("stepSize"))

    return 0.0


def _min_qty_from_meta(symbol_meta: Dict[str, Any]) -> float:
    direct = safe_float(symbol_meta.get("min_qty") or symbol_meta.get("minQty") or 0.0)
    if direct > 0:
        return direct

    for f in symbol_meta.get("filters", []):
        if f.get("filterType") == "LOT_SIZE":
            return safe_float(f.get("minQty"))

    return 0.0


def _normalize_binance_status(v: Any) -> str:
    return str(v or "").upper().strip()


def _response_order_id(resp: Any) -> str:
    if isinstance(resp, dict):
        return str(resp.get("orderId", "") or resp.get("order_id", "") or "").strip()
    return ""


def _response_avg_price(resp: Any) -> float:
    if not isinstance(resp, dict):
        return 0.0

    candidates = [
        resp.get("avgPrice"),
        resp.get("avg_price"),
        resp.get("averagePrice"),
        resp.get("price"),
        resp.get("ap"),
    ]
    for c in candidates:
        v = safe_float(c)
        if v > 0:
            return v
    return 0.0


def _response_executed_qty(resp: Any) -> float:
    if not isinstance(resp, dict):
        return 0.0

    candidates = [
        resp.get("executedQty"),
        resp.get("executed_qty"),
        resp.get("cumQty"),
        resp.get("cum_qty"),
        resp.get("origQty"),
        resp.get("quantity"),
        resp.get("qty"),
    ]
    for c in candidates:
        v = safe_float(c)
        if v > 0:
            return v
    return 0.0


def _safe_client_call(client: Any, method_names: List[str], **kwargs) -> Any:
    for name in method_names:
        fn = getattr(client, name, None)
        if callable(fn):
            return fn(**kwargs)
    return None


# =========================================================
# EXCHANGE RECONCILIATION HELPERS
# =========================================================

def _get_order_status(client: Any, symbol: str, order_id: str) -> Optional[Dict[str, Any]]:
    if not order_id:
        return None

    order_id_int: Any = order_id
    try:
        order_id_int = int(order_id)
    except Exception:
        pass

    resp = _safe_client_call(
        client,
        ["get_order", "get_order_status", "query_order", "fetch_order"],
        symbol=symbol,
        order_id=order_id_int,
    )
    if isinstance(resp, dict):
        return resp
    return None


def _extract_position_qty_from_resp(resp: Any, symbol: str) -> Optional[float]:
    if resp is None:
        return None

    if isinstance(resp, (int, float, str)):
        return abs(safe_float(resp))

    if isinstance(resp, dict):
        if resp.get("symbol") == symbol:
            for key in ["positionAmt", "position_amt", "qty", "quantity", "position_amount"]:
                if key in resp:
                    return abs(safe_float(resp.get(key)))

        if "positionAmt" in resp:
            return abs(safe_float(resp.get("positionAmt")))

        if "qty" in resp:
            return abs(safe_float(resp.get("qty")))

    if isinstance(resp, list):
        for item in resp:
            if not isinstance(item, dict):
                continue
            if item.get("symbol") != symbol:
                continue
            for key in ["positionAmt", "position_amt", "qty", "quantity", "position_amount"]:
                if key in item:
                    return abs(safe_float(item.get(key)))
    return None


def _extract_position_entry_from_resp(resp: Any, symbol: str) -> Optional[float]:
    if resp is None:
        return None

    if isinstance(resp, dict):
        if resp.get("symbol") == symbol:
            for key in ["entryPrice", "entry_price", "avgPrice", "avg_price"]:
                if key in resp:
                    v = safe_float(resp.get(key))
                    if v > 0:
                        return v

        for key in ["entryPrice", "entry_price", "avgPrice", "avg_price"]:
            if key in resp:
                v = safe_float(resp.get(key))
                if v > 0:
                    return v

    if isinstance(resp, list):
        for item in resp:
            if not isinstance(item, dict):
                continue
            if item.get("symbol") != symbol:
                continue
            for key in ["entryPrice", "entry_price", "avgPrice", "avg_price"]:
                if key in item:
                    v = safe_float(item.get(key))
                    if v > 0:
                        return v
    return None


def _get_exchange_position_qty(client: Any, symbol: str) -> Optional[float]:
    candidates = [
        (["get_position_qty", "get_position_amt", "fetch_position_qty"], {"symbol": symbol}),
        (["get_position_information", "get_position_info", "position_information"], {"symbol": symbol}),
        (["get_position_risk", "position_risk"], {"symbol": symbol}),
    ]

    for method_names, kwargs in candidates:
        try:
            resp = _safe_client_call(client, method_names, **kwargs)
            qty = _extract_position_qty_from_resp(resp, symbol)
            if qty is not None:
                return qty
        except Exception:
            continue

    return None


def _get_exchange_position_snapshot(client: Any, symbol: str) -> Dict[str, float]:
    snapshot = {"qty": 0.0, "entry": 0.0}
    candidates = [
        (["get_position_risk", "position_risk"], {}),
        (["get_position_information", "get_position_info", "position_information"], {"symbol": symbol}),
    ]
    for method_names, kwargs in candidates:
        try:
            resp = _safe_client_call(client, method_names, **kwargs)
            qty = _extract_position_qty_from_resp(resp, symbol)
            entry = _extract_position_entry_from_resp(resp, symbol)
            if qty is not None:
                snapshot["qty"] = qty
            if entry is not None:
                snapshot["entry"] = entry
            if snapshot["qty"] > 0 or snapshot["entry"] > 0:
                return snapshot
        except Exception:
            continue
    return snapshot


def _infer_close_reason_from_price(side: str, live_price: float, sl: float, tp: float) -> str:
    side = str(side).upper()
    if side == "LONG":
        if live_price <= sl:
            return "SL_HIT"
        if live_price >= tp:
            return "TP_HIT"
    else:
        if live_price >= sl:
            return "SL_HIT"
        if live_price <= tp:
            return "TP_HIT"
    return "CLOSED_ON_BINANCE"


def _infer_close_reason_from_orders(client: Any, pos: Dict[str, Any], fallback_reason: str) -> str:
    symbol = pos["symbol"]
    sl_order_id = str(pos.get("sl_order_id", "")).strip()
    tp_order_id = str(pos.get("tp_order_id", "")).strip()
    sl_status = _get_order_status(client, symbol, sl_order_id) if sl_order_id else None
    tp_status = _get_order_status(client, symbol, tp_order_id) if tp_order_id else None

    if isinstance(sl_status, dict) and _normalize_binance_status(sl_status.get("status")) == "FILLED":
        return "SL_HIT"
    if isinstance(tp_status, dict) and _normalize_binance_status(tp_status.get("status")) == "FILLED":
        return "TP_HIT"
    if fallback_reason == "CLOSED_ON_BINANCE":
        return "UNKNOWN_CLOSE"
    return fallback_reason


def _sync_position_qty_from_exchange(pos: Dict[str, Any]) -> Optional[float]:
    if CONFIG.ENGINE.EXECUTION_MODE != "REAL":
        return safe_float(pos.get("qty"))

    client = get_binance_client()
    if client is None:
        return None

    qty = _get_exchange_position_qty(client, pos["symbol"])
    if qty is None:
        return None

    pos["qty"] = round(qty, 8)
    return qty


def _validate_realtime_protection(client: Any, pos: Dict[str, Any], symbol_meta: Dict[str, Any]) -> bool:
    if CONFIG.ENGINE.EXECUTION_MODE != "REAL":
        return True

    symbol = pos["symbol"]
    current_qty = safe_float(pos.get("qty"))
    if current_qty <= 0:
        pos["protection_armed"] = 0
        return True

    sl_order_id = str(pos.get("sl_order_id", "")).strip()
    tp_order_id = str(pos.get("tp_order_id", "")).strip()
    if not sl_order_id or not tp_order_id:
        _safe_rearm_after_change(pos, symbol_meta)
        return True

    sl_status = _get_order_status(client, symbol, sl_order_id)
    tp_status = _get_order_status(client, symbol, tp_order_id)

    def _is_active(order_resp: Optional[Dict[str, Any]]) -> bool:
        if not isinstance(order_resp, dict):
            return False
        return _normalize_binance_status(order_resp.get("status")) in {"NEW", "PARTIALLY_FILLED"}

    sl_ok = _is_active(sl_status)
    tp_ok = _is_active(tp_status)
    if not sl_ok or not tp_ok:
        _safe_rearm_after_change(pos, symbol_meta)
        return True

    sl_exec = _response_executed_qty(sl_status)
    tp_exec = _response_executed_qty(tp_status)
    if sl_exec > 0 or tp_exec > 0:
        _safe_rearm_after_change(pos, symbol_meta)

    pos["protection_armed"] = 1
    return True


def _dedupe_positions(positions: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    deduped: List[Dict[str, Any]] = []
    seen = set()
    for pos in positions:
        if pos.get("status") != "OPEN_POSITION":
            deduped.append(pos)
            continue
        key = (str(pos.get("symbol", "")).upper(), str(pos.get("side", "")).upper())
        if key in seen:
            log_message(
                f"DUPLICATE_OPEN_POSITION_DROPPED symbol={key[0]} side={key[1]} position_id={pos.get('position_id')}",
                CONFIG.FILES.POSITION_LOG_FILE,
            )
            continue
        seen.add(key)
        deduped.append(pos)
    return deduped


# =========================================================
# PROTECTION
# =========================================================

def arm_protection(
    client: Any,
    symbol: str,
    side: str,
    qty: float,
    sl: float,
    tp: float,
    symbol_meta: Dict[str, Any],
) -> Dict[str, Any]:
    sl_side = close_side_to_binance(side)
    tp_side = close_side_to_binance(side)

    qty_s = _qty_to_exchange_str(qty, symbol_meta)
    sl_s = _price_to_exchange_str(sl, symbol_meta)
    tp_s = _price_to_exchange_str(tp, symbol_meta)

    if safe_float(qty_s) <= 0:
        raise RuntimeError(f"PROTECTION_QTY_INVALID {symbol} qty={qty_s}")

    sl_order = client.place_stop_market(
        symbol=symbol,
        side=sl_side,
        stop_price=sl_s,
        reduce_only=True,
        quantity=qty_s,
    )
    tp_order = client.place_take_profit_market(
        symbol=symbol,
        side=tp_side,
        stop_price=tp_s,
        reduce_only=True,
        quantity=qty_s,
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

    current_qty = _normalize_qty(safe_float(pos.get("qty")), symbol_meta)
    if current_qty <= 0:
        pos["sl_order_id"] = ""
        pos["tp_order_id"] = ""
        pos["protection_armed"] = 0
        return

    protection = arm_protection(
        client=client,
        symbol=pos["symbol"],
        side=pos["side"],
        qty=current_qty,
        sl=safe_float(pos["sl"]),
        tp=safe_float(pos["tp"]),
        symbol_meta=symbol_meta,
    )

    pos["sl_order_id"] = protection["sl_order_id"]
    pos["tp_order_id"] = protection["tp_order_id"]
    pos["protection_armed"] = 1 if protection["sl_order_id"] or protection["tp_order_id"] else 0


def _safe_rearm_after_change(pos: Dict[str, Any], symbol_meta: Dict[str, Any]) -> None:
    if CONFIG.ENGINE.EXECUTION_MODE != "REAL":
        return

    try:
        cancel_existing_protection_if_any(pos)
        time.sleep(_rearm_sleep_seconds())
        rearm_protection_for_position(pos, symbol_meta)
    except Exception as e:
        pos["protection_armed"] = 0
        log_message(
            f"PROTECTION_REARM_FAIL {pos['symbol']} error={e}",
            CONFIG.FILES.POSITION_LOG_FILE,
        )
        raise


# =========================================================
# ENTRY EXECUTION
# =========================================================

def _flatten_partial_entry_if_any(
    client: Any,
    symbol: str,
    side: str,
    partial_qty: float,
    symbol_meta: Dict[str, Any],
) -> None:
    if partial_qty <= 0:
        return

    close_qty = _normalize_qty(partial_qty, symbol_meta)
    if close_qty <= 0:
        return

    client.place_market_order(
        symbol=symbol,
        side=close_side_to_binance(side),
        quantity=_qty_to_exchange_str(close_qty, symbol_meta),
        reduce_only=True,
    )


def _wait_for_fill(
    client: Any,
    symbol: str,
    order_id: str,
    expected_qty: float,
) -> Tuple[bool, float, float, Dict[str, Any]]:
    """
    Dönüş:
      filled, avg_price, executed_qty, raw_status
    """
    timeout = _entry_fill_timeout_seconds()
    poll = _entry_fill_poll_seconds()

    last_status: Dict[str, Any] = {}
    started = time.time()

    while time.time() - started <= timeout:
        status = _get_order_status(client, symbol, order_id)
        if isinstance(status, dict):
            last_status = status
            st = _normalize_binance_status(status.get("status"))
            exec_qty = _response_executed_qty(status)
            avg_price = _response_avg_price(status)

            if st == "FILLED":
                if exec_qty <= 0:
                    exec_qty = expected_qty
                return True, avg_price, exec_qty, last_status

            if exec_qty >= expected_qty > 0:
                return True, avg_price, exec_qty, last_status

            if st in {"CANCELED", "EXPIRED", "REJECTED"}:
                return False, avg_price, exec_qty, last_status

        time.sleep(poll)

    exec_qty = _response_executed_qty(last_status)
    avg_price = _response_avg_price(last_status)
    if exec_qty >= expected_qty > 0:
        return True, avg_price, exec_qty, last_status

    return False, avg_price, exec_qty, last_status


def _submit_entry_and_confirm(
    order: Dict[str, Any],
    symbol_meta: Dict[str, Any],
) -> Optional[Dict[str, Any]]:
    """
    REAL modda gerçek fill teyidi alınmadan pozisyon açılmaz.

    Dönüş:
      {
        "entry_price": float,
        "filled_qty": float,
        "entry_order_id": str,
      }
    veya None
    """
    client = get_binance_client()
    if client is None:
        raise RuntimeError("REAL mode active but Binance client could not be created")

    symbol = order["symbol"]
    side = order["side"]
    desired_entry = safe_float(order["entry_trigger"])

    qty = calc_qty(symbol, desired_entry, symbol_meta)
    if qty <= 0:
        raise RuntimeError(f"CALCULATED_QTY_INVALID {symbol} qty={qty}")

    entry_price = _normalize_price(desired_entry, symbol_meta)

    qty_s = _qty_to_exchange_str(qty, symbol_meta)
    price_s = _price_to_exchange_str(entry_price, symbol_meta)

    log_message(
        f"ENTRY_PRECISION_PREP {symbol} side={side} qty={qty_s} price={price_s} "
        f"qty_step={_qty_step_from_meta(symbol_meta)} tick={_price_tick_from_meta(symbol_meta)}",
        CONFIG.FILES.POSITION_LOG_FILE,
    )

    client.set_leverage(symbol, CONFIG.TRADE.LEVERAGE)
    use_limit_entry = bool(getattr(CONFIG.TRADE, "USE_LIMIT_ENTRY", False))

    if use_limit_entry:
        limit_resp = client.place_limit_order(
            symbol=symbol,
            side=side_to_binance(side),
            quantity=qty_s,
            price=price_s,
            reduce_only=False,
        )

        entry_order_id = _response_order_id(limit_resp)
        if not entry_order_id:
            raise RuntimeError(f"LIMIT_ENTRY_NO_ORDER_ID {symbol}")

        filled, avg_price, executed_qty, last_status = _wait_for_fill(
            client=client,
            symbol=symbol,
            order_id=entry_order_id,
            expected_qty=safe_float(qty_s),
        )

        if filled:
            fill_price = avg_price if avg_price > 0 else safe_float(price_s)
            filled_qty = executed_qty if executed_qty > 0 else safe_float(qty_s)
            return {
                "entry_price": round(fill_price, 8),
                "filled_qty": round(filled_qty, 8),
                "entry_order_id": entry_order_id,
            }

        try:
            client.cancel_order(symbol=symbol, order_id=int(entry_order_id))
        except Exception as e:
            log_message(
                f"ENTRY_CANCEL_FAIL {symbol} entry_order_id={entry_order_id} error={e}",
                CONFIG.FILES.POSITION_LOG_FILE,
            )

        partial_qty = _response_executed_qty(last_status)
        if partial_qty > 0:
            log_message(
                f"ENTRY_PARTIAL_FILL_DETECTED {symbol} partial_qty={partial_qty} -> flattening",
                CONFIG.FILES.POSITION_LOG_FILE,
            )
            _flatten_partial_entry_if_any(client, symbol, side, partial_qty, symbol_meta)

        return None

    market_resp = client.place_market_order(
        symbol=symbol,
        side=side_to_binance(side),
        quantity=qty_s,
        reduce_only=False,
    )

    entry_order_id = _response_order_id(market_resp)
    avg_price = _response_avg_price(market_resp)
    executed_qty = _response_executed_qty(market_resp)

    if entry_order_id:
        status = _get_order_status(client, symbol, entry_order_id)
        if isinstance(status, dict):
            avg_price_q = _response_avg_price(status)
            executed_qty_q = _response_executed_qty(status)
            if avg_price_q > 0:
                avg_price = avg_price_q
            if executed_qty_q > 0:
                executed_qty = executed_qty_q

    if executed_qty <= 0:
        reconciled_qty = _get_exchange_position_qty(client, symbol)
        if reconciled_qty is not None and reconciled_qty > 0:
            executed_qty = reconciled_qty

    if executed_qty <= 0:
        raise RuntimeError(f"MARKET_ENTRY_NOT_CONFIRMED {symbol}")

    if avg_price <= 0:
        market = get_market_snapshot(symbol)
        avg_price = safe_float(market.get("price")) if market else desired_entry

    return {
        "entry_price": round(avg_price, 8),
        "filled_qty": round(executed_qty, 8),
        "entry_order_id": entry_order_id,
    }


# =========================================================
# POSITION OPEN / CLOSE BUILDERS
# =========================================================

def open_position_from_order(
    order: Dict[str, Any],
    live_price: float,
    symbol_meta: Dict[str, Any],
) -> Optional[Dict[str, Any]]:
    protection = {"sl_order_id": "", "tp_order_id": ""}

    if CONFIG.ENGINE.EXECUTION_MODE == "REAL":
        entry_fill = _submit_entry_and_confirm(order, symbol_meta)
        if entry_fill is None:
            return None

        entry = _normalize_price(safe_float(entry_fill["entry_price"]), symbol_meta)
        qty = _normalize_qty(safe_float(entry_fill["filled_qty"]), symbol_meta)

        if qty <= 0:
            raise RuntimeError(f"ENTRY_FILLED_QTY_INVALID {order['symbol']} qty={qty}")

        protection = arm_protection(
            client=get_binance_client(),
            symbol=order["symbol"],
            side=order["side"],
            qty=qty,
            sl=safe_float(order["sl"]),
            tp=safe_float(order["tp"]),
            symbol_meta=symbol_meta,
        )

    else:
        entry = safe_float(order["entry_trigger"])
        qty = calc_qty(order["symbol"], entry, symbol_meta)
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


def build_closed_row_from_order(
    order: Dict[str, Any],
    live_price: float,
    symbol_meta: Dict[str, Any],
    close_reason: str,
) -> Dict[str, Any]:
    """
    Entry görüldü ama sistem loop anında pozisyona taşıyamadan TP/SL tarafına gitmişse,
    PAPER benzeri sentetik kayıt düş.
    REAL modda bu fonksiyon sadece çok istisnai fallback içindir.
    """
    entry = safe_float(order["entry_trigger"])
    qty = calc_qty(order["symbol"], entry, symbol_meta)
    side = str(order["side"]).upper()
    sl = safe_float(order["sl"])

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
    now = utc_now_str()

    return {
        "position_id": new_position_id(order["symbol"], order["side"]),
        "order_id": order["order_id"],
        "symbol": order["symbol"],
        "side": order["side"],
        "entry": round(entry, 8),
        "qty": round(qty, 8),
        "sl": round(sl, 8),
        "tp": round(safe_float(order["tp"]), 8),
        "rr": order["rr"],
        "score": order["score"],
        "tf_context": order["tf_context"],
        "setup_type": order["setup_type"],
        "setup_reason": order["setup_reason"],
        "opened_at": now,
        "updated_at": now,
        "status": "CLOSED",
        "live_price": round(live_price, 8),
        "pnl_pct": round(gross_pct, 4),
        "net_pnl_pct": round(net_pct, 4),
        "net_pnl_usdt": round(net_usdt, 4),
        "fees_usdt": round(fees_usdt, 4),
        "sl_order_id": "paper-sl" if CONFIG.ENGINE.EXECUTION_MODE != "REAL" else "",
        "tp_order_id": "paper-tp" if CONFIG.ENGINE.EXECUTION_MODE != "REAL" else "",
        "protection_armed": 0,
        "partial_taken": 0,
        "break_even_armed": 0,
        "highest_price": round(live_price, 8),
        "lowest_price": round(live_price, 8),
        "initial_qty": round(qty, 8),
        "initial_risk": round(abs(entry - sl), 8),
        "closed_at": now,
        "close_reason": close_reason,
        "close_price": round(live_price, 8),
    }


def _build_closed_row_from_position(
    pos: Dict[str, Any],
    live_price: float,
    close_reason: str,
) -> Dict[str, Any]:
    closed_row = dict(pos)
    closed_row["status"] = "CLOSED"
    closed_row["updated_at"] = utc_now_str()
    closed_row["closed_at"] = utc_now_str()
    closed_row["close_reason"] = close_reason
    closed_row["close_price"] = round(live_price, 8)
    return closed_row


# =========================================================
# ORDER -> POSITION LOGIC
# =========================================================

def entry_seen(order: Dict[str, Any], live_price: float) -> bool:
    """
    Order'ın artık 'tetiklenmiş/fill olmuş olabilir' kabul edilip edilmeyeceğini belirler.
    PAPER mantığında işe yarar.
    REAL modda asıl güvence gerçek exchange fill teyididir.
    """
    side = str(order["side"]).upper()
    zone_low = safe_float(order["entry_zone_low"])
    zone_high = safe_float(order["entry_zone_high"])
    entry = safe_float(order["entry_trigger"])
    zone_touched = int(float(order.get("zone_touched", 0)))

    in_zone = price_in_zone(live_price, zone_low, zone_high)

    if in_zone:
        return True

    if zone_touched == 1:
        return True

    if side == "LONG" and live_price >= entry:
        return True

    if side == "SHORT" and live_price <= entry:
        return True

    return False


def immediate_close_reason_after_entry(order: Dict[str, Any], live_price: float) -> Optional[str]:
    """
    Entry görüldükten sonra fiyat zaten TP veya SL tarafına taşmışsa
    PAPER için sentetik kapatma yap.
    REAL modda gerçek entry fill olmadan synthetic close tercih edilmez.
    """
    if CONFIG.ENGINE.EXECUTION_MODE == "REAL":
        return None

    side = str(order["side"]).upper()
    sl = safe_float(order["sl"])
    tp = safe_float(order["tp"])

    if side == "LONG":
        if live_price <= sl:
            return "SL_HIT"
        if live_price >= tp:
            return "TP_HIT"
    else:
        if live_price >= sl:
            return "SL_HIT"
        if live_price <= tp:
            return "TP_HIT"

    return None


def process_orders_into_positions() -> None:
    if CONFIG.ENGINE.EXECUTION_MODE == "REAL":
        # REAL modda entry emirleri order.py tarafından yönetilir.
        # position.py sadece canlı pozisyonları yönetir.
        return

    orders = load_open_orders()
    positions = load_open_positions()
    closed = load_closed_positions()

    existing_open_order_ids = {
        str(p.get("order_id", ""))
        for p in positions
        if p.get("status") == "OPEN_POSITION"
    }

    existing_closed_order_ids = {
        str(c.get("order_id", ""))
        for c in closed
    }

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

        order_id = str(order.get("order_id", ""))
        if order_id in existing_open_order_ids or order_id in existing_closed_order_ids:
            continue

        try:
            market = get_market_snapshot(order["symbol"])
            if not market:
                log_message(
                    f"ORDER_SKIP_NO_MARKET_DATA {order['symbol']}",
                    CONFIG.FILES.POSITION_LOG_FILE,
                )
                continue

            live_price = safe_float(market["price"])
            order["live_price"] = round(live_price, 8)
            order["updated_at"] = utc_now_str()

            zone_low = safe_float(order["entry_zone_low"])
            zone_high = safe_float(order["entry_zone_high"])

            if price_in_zone(live_price, zone_low, zone_high):
                if int(float(order.get("zone_touched", 0))) != 1:
                    log_message(
                        f"ORDER_ZONE_TOUCHED {order['symbol']} {order['side']} live={round(live_price, 8)}",
                        CONFIG.FILES.POSITION_LOG_FILE,
                    )
                order["zone_touched"] = 1

            if not entry_seen(order, live_price):
                continue

            symbol_meta = get_symbol_meta(order["symbol"])
            if not symbol_meta:
                log_message(
                    f"SYMBOL_META_MISSING {order['symbol']}",
                    CONFIG.FILES.POSITION_LOG_FILE,
                )
                continue

            close_reason = immediate_close_reason_after_entry(order, live_price)
            if close_reason:
                closed_row = build_closed_row_from_order(
                    order=order,
                    live_price=live_price,
                    symbol_meta=symbol_meta,
                    close_reason=close_reason,
                )
                closed.append(closed_row)

                order["status"] = f"DIRECT_{close_reason}"
                order["updated_at"] = utc_now_str()

                log_message(
                    f"ORDER_DIRECT_CLOSE {order['symbol']} {order['side']} "
                    f"reason={close_reason} entry={order['entry_trigger']} live={round(live_price, 8)} "
                    f"mode={CONFIG.ENGINE.EXECUTION_MODE}",
                    CONFIG.FILES.POSITION_LOG_FILE,
                )
                continue

            pos = open_position_from_order(order, live_price, symbol_meta)
            if pos is None:
                continue

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

            if "Precision is over the maximum defined for this asset" in err:
                order["status"] = "PRECISION_FAILED"
                order["updated_at"] = utc_now_str()
                log_message(
                    f"PRECISION_ERROR symbol={order.get('symbol')} order_id={order.get('order_id')} error={err}",
                    CONFIG.FILES.POSITION_LOG_FILE,
                )
                continue

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
    save_closed_positions(closed)


# =========================================================
# PARTIAL CLOSE
# =========================================================

def execute_partial_close(pos: Dict[str, Any], close_ratio: float, symbol_meta: Dict[str, Any]) -> float:
    """
    Pozisyonun bir kısmını kapatır.
    Dönüş: gerçekten kapanan qty
    """
    current_qty = safe_float(pos["qty"])
    if current_qty <= 0:
        return 0.0

    close_ratio = max(0.0, min(close_ratio, 1.0))
    raw_close_qty = current_qty * close_ratio

    close_qty = _normalize_qty(raw_close_qty, symbol_meta)
    if close_qty <= 0:
        return 0.0

    remaining_qty = _normalize_qty(current_qty - close_qty, symbol_meta)
    min_qty = _min_qty_from_meta(symbol_meta)

    if min_qty and remaining_qty < min_qty:
        return 0.0

    if CONFIG.ENGINE.EXECUTION_MODE == "REAL":
        client = get_binance_client()
        if client is None:
            raise RuntimeError("REAL mode active but Binance client could not be created")

        before_qty = _get_exchange_position_qty(client, pos["symbol"])
        if before_qty is None:
            before_qty = current_qty

        client.place_market_order(
            symbol=pos["symbol"],
            side=close_side_to_binance(pos["side"]),
            quantity=_qty_to_exchange_str(close_qty, symbol_meta),
            reduce_only=True,
        )

        time.sleep(_close_reconcile_sleep_seconds())
        after_qty = _get_exchange_position_qty(client, pos["symbol"])

        if after_qty is None:
            after_qty = max(0.0, remaining_qty)

        actually_closed = max(0.0, before_qty - after_qty)
        pos["qty"] = round(after_qty, 8)
        pos["updated_at"] = utc_now_str()
        return round(actually_closed, 8)

    pos["qty"] = round(remaining_qty, 8)
    pos["updated_at"] = utc_now_str()
    return round(close_qty, 8)


# =========================================================
# POSITION UPDATE LOGIC
# =========================================================

def update_positions() -> None:
    positions = _dedupe_positions(load_open_positions())
    closed = load_closed_positions()
    updated_positions: List[Dict[str, Any]] = []

    for pos in positions:
        if pos.get("status") != "OPEN_POSITION":
            continue

        try:
            market = get_market_snapshot(pos["symbol"])
            if not market:
                log_message(
                    f"POSITION_UPDATE_SKIP {pos['symbol']} reason=NO_MARKET_DATA",
                    CONFIG.FILES.POSITION_LOG_FILE,
                )
                updated_positions.append(pos)
                continue

            live_price = safe_float(market["price"])
            entry = safe_float(pos["entry"])
            qty = safe_float(pos["qty"])
            side = str(pos["side"]).upper()
            sl = safe_float(pos["sl"])
            tp = safe_float(pos["tp"])
            initial_risk = safe_float(pos.get("initial_risk"))

            if CONFIG.ENGINE.EXECUTION_MODE == "REAL":
                client = get_binance_client()
                if client is None:
                    raise RuntimeError("REAL mode active but Binance client could not be created")

                snapshot = _get_exchange_position_snapshot(client, pos["symbol"])
                exchange_qty = safe_float(snapshot.get("qty"))
                exchange_entry = safe_float(snapshot.get("entry"))

                if exchange_entry > 0:
                    pos["entry"] = round(exchange_entry, 8)
                    entry = safe_float(pos["entry"])
                if exchange_qty > 0:
                    pos["qty"] = round(exchange_qty, 8)
                    qty = safe_float(pos["qty"])

                if exchange_qty <= 0:
                    price_reason = _infer_close_reason_from_price(side, live_price, sl, tp)
                    close_reason = _infer_close_reason_from_orders(client, pos, price_reason)

                    closed_row = _build_closed_row_from_position(pos, live_price, close_reason)
                    closed.append(closed_row)

                    log_message(
                        f"POSITION_CLOSED_ON_EXCHANGE {pos['symbol']} reason={close_reason} "
                        f"mode={CONFIG.ENGINE.EXECUTION_MODE}",
                        CONFIG.FILES.POSITION_LOG_FILE,
                    )
                    alert_position_closed(pos, close_reason)
                    continue

                symbol_meta = get_symbol_meta(pos["symbol"])
                if symbol_meta:
                    _validate_realtime_protection(client, pos, symbol_meta)

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

            if initial_risk > 0:
                if side == "LONG":
                    progress_r = (live_price - entry) / initial_risk
                else:
                    progress_r = (entry - live_price) / initial_risk
            else:
                progress_r = 0.0

            if CONFIG.TRADE.ENABLE_TRAILING and not int(float(pos.get("break_even_armed", 0))):
                if progress_r >= CONFIG.TRADE.BREAK_EVEN_TRIGGER_R:
                    old_sl = safe_float(pos["sl"])
                    pos["sl"] = round(entry, 8)
                    pos["break_even_armed"] = 1

                    log_message(
                        f"BREAK_EVEN_ARMED {pos['symbol']} side={side} entry={entry}",
                        CONFIG.FILES.POSITION_LOG_FILE,
                    )
                    alert_break_even(pos)

                    if CONFIG.ENGINE.EXECUTION_MODE == "REAL":
                        symbol_meta = get_symbol_meta(pos["symbol"])
                        if symbol_meta:
                            try:
                                _safe_rearm_after_change(pos, symbol_meta)
                            except Exception:
                                pos["sl"] = round(old_sl, 8)
                                pos["break_even_armed"] = 0
                                updated_positions.append(pos)
                                continue

            partial_taken = int(float(pos.get("partial_taken", 0)))
            if partial_taken == 0 and CONFIG.TRADE.PARTIAL_TP_AT_R < 99:
                if progress_r >= CONFIG.TRADE.PARTIAL_TP_AT_R:
                    symbol_meta = get_symbol_meta(pos["symbol"])
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
                                try:
                                    _safe_rearm_after_change(pos, symbol_meta)
                                except Exception:
                                    updated_positions.append(pos)
                                    continue

            if safe_float(pos.get("qty")) <= 0:
                close_reason = "PARTIAL_CLOSE_TO_ZERO"
                closed_row = _build_closed_row_from_position(pos, live_price, close_reason)
                closed.append(closed_row)

                log_message(
                    f"POSITION_CLOSED {pos['symbol']} reason={close_reason} "
                    f"mode={CONFIG.ENGINE.EXECUTION_MODE}",
                    CONFIG.FILES.POSITION_LOG_FILE,
                )
                alert_position_closed(pos, close_reason)
                continue

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
                        symbol_meta = get_symbol_meta(pos["symbol"])
                        if symbol_meta:
                            try:
                                _safe_rearm_after_change(pos, symbol_meta)
                            except Exception:
                                updated_positions.append(pos)
                                continue

            if CONFIG.ENGINE.EXECUTION_MODE == "REAL":
                client = get_binance_client()
                if client is None:
                    raise RuntimeError("REAL mode active but Binance client could not be created")

                exchange_qty = _get_exchange_position_qty(client, pos["symbol"])
                if exchange_qty is not None and exchange_qty <= 0:
                    price_reason = _infer_close_reason_from_price(side, live_price, safe_float(pos["sl"]), tp)
                    close_reason = _infer_close_reason_from_orders(client, pos, price_reason)

                    closed_row = _build_closed_row_from_position(pos, live_price, close_reason)
                    closed.append(closed_row)

                    log_message(
                        f"POSITION_CLOSED_ON_EXCHANGE {pos['symbol']} reason={close_reason} "
                        f"net_pnl_pct={pos['net_pnl_pct']} mode={CONFIG.ENGINE.EXECUTION_MODE}",
                        CONFIG.FILES.POSITION_LOG_FILE,
                    )
                    alert_position_closed(pos, close_reason)
                    continue

                if side == "LONG":
                    touched_sl_tp = live_price <= safe_float(pos["sl"]) or live_price >= tp
                else:
                    touched_sl_tp = live_price >= safe_float(pos["sl"]) or live_price <= tp

                if touched_sl_tp:
                    log_message(
                        f"PROTECTION_PENDING {pos['symbol']} side={side} live={round(live_price, 8)} "
                        f"sl={round(safe_float(pos['sl']), 8)} tp={round(tp, 8)} qty={pos['qty']}",
                        CONFIG.FILES.POSITION_LOG_FILE,
                    )

                updated_positions.append(pos)
                continue

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
                closed_row = _build_closed_row_from_position(pos, live_price, close_reason)
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
                f"POSITION_UPDATE_FAIL {pos.get('symbol')} error={str(e)[:220]}",
                CONFIG.FILES.POSITION_LOG_FILE,
            )
            updated_positions.append(pos)

    save_open_positions(updated_positions)
    save_closed_positions(closed)


# =========================================================
# ALERTS
# =========================================================

def notify_live_positions() -> None:
    positions = load_open_positions()
    for pos in positions:
        try:
            alert_position_update(pos)
        except Exception:
            pass


# =========================================================
# MAIN LOOP
# =========================================================

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