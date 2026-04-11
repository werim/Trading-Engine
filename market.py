from typing import Any, Dict, Optional
from env import load_env
load_env()

import os
import math

from config import CONFIG
from utils import log_message
from binance_real import BinanceFuturesClient

client = BinanceFuturesClient(
    api_key=os.getenv("BINANCE_API_KEY", "").strip(),
    api_secret=os.getenv("BINANCE_API_SECRET", "").strip(),
    testnet=os.getenv("BINANCE_TESTNET", "true").strip().lower() == "true",
)


def is_real_mode() -> bool:
    return CONFIG.ENGINE.EXECUTION_MODE.upper() == "REAL"


def _safe_float(value: Any, default: float = 0.0) -> float:
    try:
        return float(value)
    except Exception:
        return default


def get_symbol_info(symbol: str) -> Dict[str, Any]:
    info = client.exchange_info()
    for s in info.get("symbols", []):
        if s.get("symbol") == symbol:
            return s
    return {}


def get_symbol_step_size(symbol: str) -> float:
    try:
        s = get_symbol_info(symbol)
        for f in s.get("filters", []):
            if f.get("filterType") == "LOT_SIZE":
                return _safe_float(f.get("stepSize"), 0.001)
    except Exception as e:
        log_message(
            f"STEP_SIZE_FETCH_FAIL {symbol} error={e}",
            CONFIG.TRADE.ORDER_LOG_FILE,
        )
    return 0.001


def get_symbol_tick_size(symbol: str) -> float:
    try:
        s = get_symbol_info(symbol)
        for f in s.get("filters", []):
            if f.get("filterType") == "PRICE_FILTER":
                return _safe_float(f.get("tickSize"), 0.0001)
    except Exception as e:
        log_message(
            f"TICK_SIZE_FETCH_FAIL {symbol} error={e}",
            CONFIG.TRADE.ORDER_LOG_FILE,
        )
    return 0.0001


def get_symbol_min_qty(symbol: str) -> float:
    try:
        s = get_symbol_info(symbol)
        for f in s.get("filters", []):
            if f.get("filterType") == "LOT_SIZE":
                return _safe_float(f.get("minQty"), 0.0)
    except Exception as e:
        log_message(
            f"MIN_QTY_FETCH_FAIL {symbol} error={e}",
            CONFIG.TRADE.ORDER_LOG_FILE,
        )
    return 0.0


def round_to_step(value: float, step: float) -> float:
    if step <= 0:
        return value
    return math.floor(value / step) * step


def round_to_tick(value: float, tick: float) -> float:
    if tick <= 0:
        return value
    return round(round(value / tick) * tick, 8)


def normalize_qty(symbol: str, qty: float) -> float:
    step = get_symbol_step_size(symbol)
    min_qty = get_symbol_min_qty(symbol)

    qty = round_to_step(qty, step)
    if qty < min_qty:
        qty = min_qty

    return round(qty, 8)


def normalize_price(symbol: str, price: float) -> float:
    tick = get_symbol_tick_size(symbol)
    return round_to_tick(price, tick)


def get_usdt_balance() -> float:
    try:
        balances = client.balance()
        for item in balances:
            if item.get("asset") == "USDT":
                return _safe_float(item.get("availableBalance") or item.get("balance"), 0.0)
    except Exception as e:
        log_message(
            f"BALANCE_FETCH_FAIL error={e}",
            CONFIG.TRADE.ORDER_LOG_FILE,
        )
    return 0.0


def calc_order_qty(symbol: str, entry_trigger: float, sl: float) -> float:
    balance = get_usdt_balance()
    if balance <= 0:
        balance = 100.0

    risk_pct = getattr(CONFIG.TRADE, "RISK_PER_TRADE_PCT", 0.01)
    risk_amount = balance * risk_pct

    sl_distance = abs(entry_trigger - sl)
    if sl_distance <= 0:
        return 0.0

    raw_qty = risk_amount / sl_distance
    qty = normalize_qty(symbol, raw_qty)

    return qty if qty > 0 else 0.0


def extract_real_entry_price(entry_order: Dict[str, Any]) -> float:
    try:
        avg_price = _safe_float(entry_order.get("avgPrice"), 0.0)
        if avg_price > 0:
            return avg_price

        fills = entry_order.get("fills", [])
        if fills:
            fill_price = _safe_float(fills[0].get("price"), 0.0)
            if fill_price > 0:
                return fill_price

        price = _safe_float(entry_order.get("price"), 0.0)
        if price > 0:
            return price

        stop_price = _safe_float(entry_order.get("stopPrice"), 0.0)
        if stop_price > 0:
            return stop_price
    except Exception:
        pass
    return 0.0


def has_real_open_position(symbol: str) -> bool:
    try:
        data = client.position_risk(symbol=symbol)
        if isinstance(data, dict):
            data = [data]

        for pos in data:
            amt = abs(_safe_float(pos.get("positionAmt"), 0.0))
            if amt > 0:
                return True
    except Exception as e:
        log_message(
            f"BINANCE_POSITION_CHECK_FAIL {symbol} error={e}",
            CONFIG.TRADE.ORDER_LOG_FILE,
        )
    return False


def is_symbol_open_for_trading(symbol: str) -> bool:
    try:
        s = get_symbol_info(symbol)
        return (
            s.get("status") == "TRADING"
            and s.get("contractType") == "PERPETUAL"
            and s.get("quoteAsset") == "USDT"
        )
    except Exception as e:
        log_message(
            f"SYMBOL_STATUS_CHECK_FAIL {symbol} error={e}",
            CONFIG.TRADE.ORDER_LOG_FILE,
        )
    return False


def _entry_order_side(side: str) -> str:
    return "BUY" if side == "LONG" else "SELL"


def _exit_order_side(side: str) -> str:
    return "SELL" if side == "LONG" else "BUY"


def arm_entry_order(candidate: Dict[str, Any]) -> Dict[str, Any]:
    symbol = candidate["symbol"]
    side = candidate["side"]
    entry_trigger = normalize_price(symbol, _safe_float(candidate["entry_trigger"]))
    sl = normalize_price(symbol, _safe_float(candidate["sl"]))

    if not is_real_mode():
        qty = calc_order_qty(symbol, entry_trigger, sl)
        return {
            "mode": "PAPER",
            "arm_order": {"algoId": "paper-arm"},
            "exchange_id": "paper-arm",
            "exchange_order_id": "",
            "exchange_algo_id": "paper-arm",
            "client_order_id": "",
            "qty": qty,
            "entry_price": entry_trigger,
        }

    if not is_symbol_open_for_trading(symbol):
        raise ValueError(f"{symbol} is not open for trading in current environment")

    if has_real_open_position(symbol):
        raise ValueError(f"{symbol} already has an open real position")

    qty = calc_order_qty(symbol, entry_trigger, sl)
    if qty <= 0:
        raise ValueError(f"invalid qty for {symbol}: {qty}")

    order_side = _entry_order_side(side)

    arm_order = client.new_algo_order(
        symbol=symbol,
        side=order_side,
        order_type="STOP_MARKET",
        quantity=qty,
        trigger_price=entry_trigger,
        working_type="MARK_PRICE",
        reduce_only=False,
    )

    exchange_algo_id = str(arm_order.get("algoId", "") or "")
    exchange_order_id = str(arm_order.get("orderId", "") or "")
    client_order_id = str(
        arm_order.get("clientOrderId")
        or arm_order.get("origClientOrderId")
        or ""
    )

    exchange_id = exchange_algo_id or exchange_order_id or client_order_id

    return {
        "exchange_order_id": exchange_order_id,  # varsa normal order id
        "exchange_algo_id": exchange_algo_id,    # algo endpoint id
        "client_order_id": client_order_id,      # client id varsa
        "qty": qty,
        "entry_price": entry_trigger,
        "sl": sl,
    }


def place_sl_tp_orders(symbol: str, side: str, sl: float, tp: float) -> Dict[str, Any]:
    close_side = _exit_order_side(side)
    sl = normalize_price(symbol, sl)
    tp = normalize_price(symbol, tp)

    sl_order = client.new_algo_order(
        symbol=symbol,
        side=close_side,
        order_type="STOP_MARKET",
        trigger_price=sl,
        close_position=True,
        working_type="MARK_PRICE",
    )

    tp_order = client.new_algo_order(
        symbol=symbol,
        side=close_side,
        order_type="TAKE_PROFIT_MARKET",
        trigger_price=tp,
        close_position=True,
        working_type="MARK_PRICE",
    )

    return {
        "sl_order": sl_order,
        "tp_order": tp_order,
        "sl": sl,
        "tp": tp,
    }


def open_market_with_sl_tp(
    symbol: str,
    side: str,
    quantity: float,
    sl: float,
    tp: float,
) -> Dict[str, Any]:
    if quantity <= 0:
        raise ValueError(f"invalid quantity for {symbol}: {quantity}")

    order_side = _entry_order_side(side)
    quantity = normalize_qty(symbol, quantity)

    entry_order = client.new_order(
        symbol=symbol,
        side=order_side,
        order_type="MARKET",
        quantity=quantity,
    )

    exit_orders = place_sl_tp_orders(symbol, side, sl, tp)
    entry_price = extract_real_entry_price(entry_order)

    return {
        "mode": "REAL",
        "entry_order": entry_order,
        "sl_order": exit_orders["sl_order"],
        "tp_order": exit_orders["tp_order"],
        "qty": quantity,
        "entry_price": entry_price,
        "sl": exit_orders["sl"],
        "tp": exit_orders["tp"],
    }


def open_paper_position(
    symbol: str,
    side: str,
    entry: float,
    sl: float,
    tp: float,
    qty: float,
) -> Dict[str, Any]:
    return {
        "mode": "PAPER",
        "entry_order": {"orderId": ""},
        "sl_order": {"algoId": ""},
        "tp_order": {"algoId": ""},
        "qty": qty,
        "entry_price": entry,
        "sl": sl,
        "tp": tp,
        "symbol": symbol,
        "side": side,
    }


def execute_order(candidate: Dict[str, Any]) -> Dict[str, Any]:
    """
    Bu fonksiyon anlık market giriş içindir.
    Zone touched -> trigger mantığı için arm_entry_order() çağrılmalı.
    """
    symbol = candidate["symbol"]
    side = candidate["side"]
    entry_trigger = _safe_float(candidate["entry_trigger"])
    sl = _safe_float(candidate["sl"])
    tp = _safe_float(candidate["tp"])

    if is_real_mode() and not is_symbol_open_for_trading(symbol):
        raise ValueError(f"{symbol} is not open for trading in current environment")

    qty = calc_order_qty(symbol, entry_trigger, sl)
    if qty <= 0:
        raise ValueError(f"invalid qty for {symbol}: {qty}")

    if is_real_mode():
        return open_market_with_sl_tp(symbol, side, qty, sl, tp)

    return open_paper_position(symbol, side, entry_trigger, sl, tp, qty)