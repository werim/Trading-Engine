from __future__ import annotations

import uuid
from typing import Any, Dict, List, Optional
from market import arm_entry_order
from env import load_env
load_env()

from config import CONFIG
from structure import evaluate_symbol
from utils import (
    append_closed_order,
    append_open_order,
    fmt_price,
    get_top_symbols,
    has_existing_position,
    has_open_order_for_symbol,
    load_open_orders,
    log_event,
    log_message,
    now_utc,
    round_price,
    round_qty,
    safe_get_live_price,
    write_open_orders,
)
from binance_real import BinanceFuturesClient


client = BinanceFuturesClient()

ORDER_LOG_FILE = CONFIG.TRADE.ORDER_LOG_FILE

MIN_SCORE = getattr(CONFIG.TRADE, "MIN_SCORE", 2)
RR_MIN = getattr(CONFIG.TRADE, "RR_MIN", 1.6)
ORDER_USDT_SIZE = getattr(CONFIG.TRADE, "ORDER_USDT_SIZE", 100.0)

ENTRY_FEE_RATE = getattr(CONFIG.TRADE, "ENTRY_FEE_RATE", 0.0004)
EXIT_FEE_RATE = getattr(CONFIG.TRADE, "EXIT_FEE_RATE", 0.0004)
EXTRA_COST_PCT = getattr(CONFIG.TRADE, "EXTRA_COST_PCT", 0.0007)

MIN_NET_PROFIT_PCT = getattr(CONFIG.TRADE, "MIN_NET_PROFIT_PCT", 0.0035)
MIN_NET_PROFIT_USDT = getattr(CONFIG.TRADE, "MIN_NET_PROFIT_USDT", 0.35)
MIN_NET_RR = getattr(CONFIG.TRADE, "MIN_NET_RR", 1.35)
NET_PROFIT_MODE = getattr(CONFIG.TRADE, "NET_PROFIT_MODE", True)

REPRICE_THRESHOLD_PCT = getattr(CONFIG.TRADE, "REPRICE_THRESHOLD_PCT", 0.0015)
ENTRY_LONG_BUFFER_PCT = getattr(CONFIG.TRADE, "ENTRY_LONG_BUFFER_PCT", 0.0003)
ENTRY_SHORT_BUFFER_PCT = getattr(CONFIG.TRADE, "ENTRY_SHORT_BUFFER_PCT", 0.0003)

MAX_OPEN_ORDERS = getattr(CONFIG.TRADE, "MAX_OPEN_ORDERS", 30)
ALLOW_REPLACE_ARMED_ORDER = getattr(CONFIG.TRADE, "ALLOW_REPLACE_ARMED_ORDER", True)
ALLOW_SIDE_FLIP_REPLACEMENT = getattr(CONFIG.TRADE, "ALLOW_SIDE_FLIP_REPLACEMENT", True)


def new_local_order_id() -> str:
    return uuid.uuid4().hex[:8]


def to_float(value: Any, default: float = 0.0) -> float:
    try:
        return float(value)
    except Exception:
        return default


def pct_diff(a: float, b: float) -> float:
    if a == 0:
        return 0.0
    return abs(a - b) / abs(a)


def normalize_side(side: str) -> str:
    return str(side or "").upper().strip()


def should_mark_zone_touched(order: Dict[str, Any], live_price: float) -> bool:
    zone_low = to_float(order.get("entry_zone_low"))
    zone_high = to_float(order.get("entry_zone_high"))
    side = normalize_side(order.get("side", ""))

    low = min(zone_low, zone_high)
    high = max(zone_low, zone_high)

    if side == "LONG":
        return live_price <= high
    if side == "SHORT":
        return live_price >= low
    return False


def calc_total_cost_pct() -> float:
    return ENTRY_FEE_RATE + EXIT_FEE_RATE + EXTRA_COST_PCT


def calc_gross_profit_pct(entry: float, tp: float, side: str) -> float:
    side = normalize_side(side)
    if entry <= 0:
        return 0.0
    if side == "LONG":
        return (tp - entry) / entry
    if side == "SHORT":
        return (entry - tp) / entry
    return 0.0


def calc_gross_loss_pct(entry: float, sl: float, side: str) -> float:
    side = normalize_side(side)
    if entry <= 0:
        return 0.0
    if side == "LONG":
        return max((entry - sl) / entry, 0.0)
    if side == "SHORT":
        return max((sl - entry) / entry, 0.0)
    return 0.0


def calc_net_profit_pct(entry: float, tp: float, side: str) -> float:
    return calc_gross_profit_pct(entry, tp, side) - calc_total_cost_pct()


def calc_net_loss_pct(entry: float, sl: float, side: str) -> float:
    return calc_gross_loss_pct(entry, sl, side) + calc_total_cost_pct()


def calc_net_rr(entry: float, sl: float, tp: float, side: str) -> float:
    net_reward = calc_net_profit_pct(entry, tp, side)
    net_risk = calc_net_loss_pct(entry, sl, side)
    if net_risk <= 0:
        return 0.0
    return net_reward / net_risk


def calc_expected_net_profit_usdt(entry: float, tp: float, side: str) -> float:
    return ORDER_USDT_SIZE * calc_net_profit_pct(entry, tp, side)


def enrich_candidate_with_net_metrics(candidate: Dict[str, Any]) -> Dict[str, Any]:
    entry = to_float(candidate.get("entry_trigger", 0))
    sl = to_float(candidate.get("sl", 0))
    tp = to_float(candidate.get("tp", 0))
    side = normalize_side(candidate.get("side", ""))

    candidate["gross_profit_pct"] = calc_gross_profit_pct(entry, tp, side)
    candidate["net_profit_pct"] = calc_net_profit_pct(entry, tp, side)
    candidate["net_loss_pct"] = calc_net_loss_pct(entry, sl, side)
    candidate["net_rr"] = calc_net_rr(entry, sl, tp, side)
    candidate["expected_net_profit_usdt"] = calc_expected_net_profit_usdt(entry, tp, side)
    candidate["total_cost_pct"] = calc_total_cost_pct()
    return candidate


def candidate_passes_net_profit_mode(candidate: Dict[str, Any]) -> bool:
    enrich_candidate_with_net_metrics(candidate)

    if candidate["net_profit_pct"] < MIN_NET_PROFIT_PCT:
        return False
    if candidate["expected_net_profit_usdt"] < MIN_NET_PROFIT_USDT:
        return False
    if candidate["net_rr"] < MIN_NET_RR:
        return False

    return True


def candidate_is_tradeable(candidate: Dict[str, Any]) -> bool:
    if not candidate:
        return False

    score = int(candidate.get("score", 0))
    rr = to_float(candidate.get("rr", 0))
    side = normalize_side(candidate.get("side", ""))

    if side not in ("LONG", "SHORT"):
        return False
    if score < MIN_SCORE:
        return False
    if rr < RR_MIN:
        return False

    for key in ("entry_zone_low", "entry_zone_high", "entry_trigger", "sl", "tp"):
        if key not in candidate:
            return False

    if NET_PROFIT_MODE and not candidate_passes_net_profit_mode(candidate):
        return False

    return True


def candidate_to_order(candidate: Dict[str, Any], live_price: float) -> Dict[str, Any]:
    order = {
        "order_id": new_local_order_id(),
        "symbol": candidate["symbol"],
        "side": normalize_side(candidate["side"]),
        "entry_zone_low": str(candidate["entry_zone_low"]),
        "entry_zone_high": str(candidate["entry_zone_high"]),
        "entry_trigger": str(candidate["entry_trigger"]),
        "sl": str(candidate["sl"]),
        "tp": str(candidate["tp"]),
        "rr": str(candidate.get("rr", "")),
        "score": str(candidate.get("score", 0)),
        "tf_context": candidate.get("tf_context", ""),
        "setup_type": candidate.get("setup_type", ""),
        "setup_reason": candidate.get("setup_reason", ""),
        "created_at": now_utc(),
        "updated_at": now_utc(),
        "expires_at": candidate.get("expires_at", ""),
        "status": "OPEN_ORDER",
        "live_price": str(live_price),
        "zone_touched": "0",
        "alarm_touched_sent": "0",
        "alarm_near_trigger_sent": "0",
        "last_alarm_at": "",
        "exchange_order_placed": "0",
        "exchange_order_id": "",
        "gross_profit_pct": str(candidate.get("gross_profit_pct", "")),
        "net_profit_pct": str(candidate.get("net_profit_pct", "")),
        "net_loss_pct": str(candidate.get("net_loss_pct", "")),
        "net_rr": str(candidate.get("net_rr", "")),
        "expected_net_profit_usdt": str(candidate.get("expected_net_profit_usdt", "")),
        "total_cost_pct": str(candidate.get("total_cost_pct", "")),
    }

    if should_mark_zone_touched(order, live_price):
        order["zone_touched"] = "1"

    return order


def refresh_order_from_candidate(old: Dict[str, Any], fresh: Dict[str, Any], live_price: float) -> Dict[str, Any]:
    old["side"] = normalize_side(fresh["side"])
    old["entry_zone_low"] = str(fresh["entry_zone_low"])
    old["entry_zone_high"] = str(fresh["entry_zone_high"])
    old["entry_trigger"] = str(fresh["entry_trigger"])
    old["sl"] = str(fresh["sl"])
    old["tp"] = str(fresh["tp"])
    old["rr"] = str(fresh.get("rr", ""))
    old["score"] = str(fresh.get("score", 0))
    old["tf_context"] = fresh.get("tf_context", "")
    old["setup_type"] = fresh.get("setup_type", "")
    old["setup_reason"] = fresh.get("setup_reason", "")
    old["updated_at"] = now_utc()
    old["live_price"] = str(live_price)

    old["gross_profit_pct"] = str(fresh.get("gross_profit_pct", ""))
    old["net_profit_pct"] = str(fresh.get("net_profit_pct", ""))
    old["net_loss_pct"] = str(fresh.get("net_loss_pct", ""))
    old["net_rr"] = str(fresh.get("net_rr", ""))
    old["expected_net_profit_usdt"] = str(fresh.get("expected_net_profit_usdt", ""))
    old["total_cost_pct"] = str(fresh.get("total_cost_pct", ""))

    if should_mark_zone_touched(old, live_price):
        old["zone_touched"] = "1"

    return old


def materially_changed(old: Dict[str, Any], fresh: Dict[str, Any]) -> bool:
    if normalize_side(old.get("side", "")) != normalize_side(fresh.get("side", "")):
        return True

    if old.get("setup_reason", "") != fresh.get("setup_reason", ""):
        return True

    if old.get("setup_type", "") != fresh.get("setup_type", ""):
        return True

    if int(old.get("score", 0)) != int(fresh.get("score", 0)):
        return True

    for key in ("entry_zone_low", "entry_zone_high", "entry_trigger", "sl", "tp"):
        old_v = to_float(old.get(key))
        new_v = to_float(fresh.get(key))
        if pct_diff(old_v, new_v) >= REPRICE_THRESHOLD_PCT:
            return True

    if pct_diff(to_float(old.get("rr", 0)), to_float(fresh.get("rr", 0))) >= REPRICE_THRESHOLD_PCT:
        return True

    return False


def should_keep_fresh_candidate(fresh: Optional[Dict[str, Any]]) -> bool:
    return bool(fresh and candidate_is_tradeable(fresh))


def entry_order_params(order: Dict[str, Any]) -> Dict[str, Any]:
    symbol = order["symbol"]
    side = normalize_side(order["side"])
    trigger = to_float(order["entry_trigger"])

    if trigger <= 0:
        raise ValueError(f"invalid trigger for {symbol}: {trigger}")

    if side == "LONG":
        exchange_side = "BUY"
        stop_price = trigger
        limit_price = trigger * (1.0 + ENTRY_LONG_BUFFER_PCT)
    elif side == "SHORT":
        exchange_side = "SELL"
        stop_price = trigger
        limit_price = trigger * (1.0 - ENTRY_SHORT_BUFFER_PCT)
    else:
        raise ValueError(f"invalid side for {symbol}: {side}")

    qty = ORDER_USDT_SIZE / max(trigger, 1e-9)
    qty = round_qty(symbol, qty)
    stop_price = round_price(symbol, stop_price)
    limit_price = round_price(symbol, limit_price)

    if qty <= 0:
        raise ValueError(f"invalid qty for {symbol}: {qty}")

    return {
        "symbol": symbol,
        "side": exchange_side,
        "qty": qty,
        "stop_price": stop_price,
        "limit_price": limit_price,
    }


def place_stop_limit_entry(order: Dict[str, Any]) -> Optional[str]:
    params = entry_order_params(order)

    # Not:
    # Binance Futures için STOP / STOP_LIMIT giriş emri doğru endpoint ile gönderilmelidir.
    # binance_real.py içinde bu methodun algo/uygun futures endpointine gitmesi gerekir.
    resp = client.place_stop_limit_order(
        symbol=params["symbol"],
        side=params["side"],
        quantity=params["qty"],
        stop_price=params["stop_price"],
        price=params["limit_price"],
        time_in_force="GTC",
        reduce_only=False,
        working_type="CONTRACT_PRICE",
    )

    exchange_order_id = str(resp.get("orderId", "") or resp.get("algoId", "")).strip()
    return exchange_order_id or None


def cancel_exchange_entry(order: Dict[str, Any]) -> bool:
    exchange_order_id = str(order.get("exchange_order_id", "")).strip()
    symbol = order["symbol"]

    if not exchange_order_id:
        return True

    try:
        client.cancel_order(symbol=symbol, order_id=int(exchange_order_id))
        return True
    except Exception as e:
        log_message(
            f"CANCEL FAILED {symbol} {order['side']} exchange_order_id={exchange_order_id} error={e}",
            ORDER_LOG_FILE,
        )
        return False


def archive_order(order: Dict[str, Any], final_status: str, reason: str) -> None:
    archived = dict(order)
    archived["status"] = final_status
    archived["updated_at"] = now_utc()
    archived["close_reason"] = reason
    append_closed_order(archived)

    log_message(
        f"{final_status} {archived['symbol']} {archived['side']} reason={reason} "
        f"trigger={archived.get('entry_trigger')} live={archived.get('live_price')}",
        ORDER_LOG_FILE,
    )
    log_event(
        final_status,
        archived["symbol"],
        archived["side"],
        f"reason={reason} trigger={archived.get('entry_trigger')} live={archived.get('live_price')}",
        int(archived.get("score", 0)),
    )


def replace_with_fresh_order(
    old: Dict[str, Any],
    fresh: Dict[str, Any],
    live_price: float,
    next_open_orders: List[Dict[str, Any]],
    reason: str,
) -> None:
    if str(old.get("status")) == "ARMED_ORDER" and str(old.get("exchange_order_placed", "0")) == "1":
        cancel_exchange_entry(old)

    archive_order(old, "REPLACED", reason)

    new_order = candidate_to_order(fresh, live_price)
    next_open_orders.append(new_order)

    log_message(
        f"NEW_ORDER {new_order['symbol']} {new_order['side']} "
        f"zone=({new_order['entry_zone_low']},{new_order['entry_zone_high']}) "
        f"trigger={new_order['entry_trigger']} sl={new_order['sl']} tp={new_order['tp']} "
        f"rr={new_order['rr']} score={new_order['score']} "
        f"live={fmt_price(live_price)} touched={new_order['zone_touched']}",
        ORDER_LOG_FILE,
    )


def process_open_orders() -> None:
    open_orders = load_open_orders()
    next_open_orders: List[Dict[str, Any]] = []

    for old in open_orders:
        status = str(old.get("status", "OPEN_ORDER"))
        symbol = old["symbol"]

        if status not in ("OPEN_ORDER", "ARMED_ORDER"):
            next_open_orders.append(old)
            continue

        if has_existing_position(symbol):
            if status == "ARMED_ORDER" and str(old.get("exchange_order_placed", "0")) == "1":
                cancel_exchange_entry(old)
            archive_order(old, "CANCELLED", "BLOCKED_BY_EXISTING_POSITION")
            continue

        live_price = safe_get_live_price(symbol)
        if live_price is None:
            next_open_orders.append(old)
            continue

        old["live_price"] = str(live_price)

        fresh = evaluate_symbol(symbol)
        if not should_keep_fresh_candidate(fresh):
            if status == "ARMED_ORDER" and str(old.get("exchange_order_placed", "0")) == "1":
                cancel_exchange_entry(old)
            archive_order(old, "CANCELLED", "NO_VALID_CANDIDATE")
            continue

        enrich_candidate_with_net_metrics(fresh)

        fresh_side = normalize_side(fresh["side"])
        old_side = normalize_side(old.get("side", ""))

        if old_side != fresh_side:
            if ALLOW_SIDE_FLIP_REPLACEMENT:
                replace_with_fresh_order(old, fresh, live_price, next_open_orders, f"SIDE_FLIP_{old_side}_TO_{fresh_side}")
            else:
                if status == "ARMED_ORDER" and str(old.get("exchange_order_placed", "0")) == "1":
                    cancel_exchange_entry(old)
                archive_order(old, "CANCELLED", "SIDE_FLIP")
            continue

        changed = materially_changed(old, fresh)

        if changed:
            if status == "ARMED_ORDER" and str(old.get("exchange_order_placed", "0")) == "1":
                if ALLOW_REPLACE_ARMED_ORDER:
                    cancel_exchange_entry(old)
                    old["exchange_order_placed"] = "0"
                    old["exchange_order_id"] = ""
                    old["status"] = "OPEN_ORDER"
                    status = "OPEN_ORDER"
                else:
                    next_open_orders.append(old)
                    continue

            old = refresh_order_from_candidate(old, fresh, live_price)

            log_message(
                f"ORDER UPDATED {symbol} {old['side']} "
                f"zone=({old['entry_zone_low']},{old['entry_zone_high']}) "
                f"trigger={old['entry_trigger']} sl={old['sl']} tp={old['tp']} "
                f"rr={old['rr']} score={old['score']} "
                f"live={fmt_price(live_price)} touched={old['zone_touched']}",
                ORDER_LOG_FILE,
            )

        if should_mark_zone_touched(old, live_price) and str(old.get("zone_touched", "0")) != "1":
            old["zone_touched"] = "1"
            old["updated_at"] = now_utc()

            log_message(
                f"ZONE TOUCHED {symbol} {old['side']} "
                f"zone=({old['entry_zone_low']},{old['entry_zone_high']}) "
                f"trigger={old['entry_trigger']} live={fmt_price(live_price)}",
                ORDER_LOG_FILE,
            )
            log_event(
                "ZONE_TOUCHED",
                symbol,
                old["side"],
                f"zone=({old['entry_zone_low']},{old['entry_zone_high']}) trigger={old['entry_trigger']} "
                f"live={fmt_price(live_price)}",
                int(old.get("score", 0)),
            )

        if (
            str(old.get("status")) == "OPEN_ORDER"
            and str(old.get("zone_touched", "0")) == "1"
            and str(old.get("exchange_order_placed", "0")) != "1"
        ):
            try:
                result = arm_entry_order(old)

                log_message(
                    f"ARMED ORDER{old['symbol']} {old['side']} "
                    f"Quantity={result.get('qty')} Price={result.get('entry_price')}"
                    f"exchange_id={result.get('exchange_id', '')} "
                    f"algo_id={result.get('exchange_algo_id', '')} "
                    f"order_id={result.get('exchange_order_id', '')}",
                    CONFIG.TRADE.ORDER_LOG_FILE,
                )
                exchange_order_id = result.get('exchange_id', '')
                params = entry_order_params(old)

                old["exchange_order_placed"] = "1"
                old["exchange_order_id"] = exchange_order_id or ""
                old["status"] = "ARMED_ORDER"
                old["updated_at"] = now_utc()

                log_event(
                    "ARMED_ORDER",
                    symbol,
                    old["side"],
                    f"stop={result.get('entry_price')} sl={result.get('sl')}"
                    f"Qty={result.get('qty')} exchange_order_id={exchange_order_id}",
                    int(old.get("score", 0)),
                )
            except Exception as e:
                old["exchange_order_placed"] = "0"
                old["exchange_order_id"] = ""
                old["status"] = "OPEN_ORDER"
                old["updated_at"] = now_utc()

                log_message(
                    f"ARM FAILED {symbol} {old['side']} trigger={old['entry_trigger']} error={e}",
                    ORDER_LOG_FILE,
                )
                log_event(
                    "ARM_FAILED",
                    symbol,
                    old["side"],
                    f"trigger={old['entry_trigger']} error={e}",
                    int(old.get("score", 0)),
                )

        next_open_orders.append(old)

    write_open_orders(next_open_orders)


def scan_and_create_candidates() -> None:
    open_orders = load_open_orders()
    existing_symbols = {
        row["symbol"]
        for row in open_orders
        if row.get("status") in ("OPEN_ORDER", "ARMED_ORDER")
    }

    mode = CONFIG.get_mode_settings()
    symbols = get_top_symbols(limit=mode["MAX_SYMBOLS_SCAN"])
    created_count = 0

    for symbol in symbols:
        if created_count >= MAX_OPEN_ORDERS:
            break

        if symbol in existing_symbols:
            continue
        if has_existing_position(symbol):
            continue
        if has_open_order_for_symbol(symbol):
            continue

        live_price = safe_get_live_price(symbol)
        if live_price is None:
            continue

        candidate = evaluate_symbol(symbol)
        if not should_keep_fresh_candidate(candidate):
            continue

        enrich_candidate_with_net_metrics(candidate)

        order = candidate_to_order(candidate, live_price)
        append_open_order(order)
        created_count += 1

        log_message(
            f"NEW_ORDER {order['symbol']} {order['side']} "
            f"zone=({order['entry_zone_low']},{order['entry_zone_high']}) "
            f"trigger={order['entry_trigger']} sl={order['sl']} tp={order['tp']} "
            f"rr={order['rr']} score={order['score']} tf={order['tf_context']} "
            f"live={fmt_price(live_price)} touched={order['zone_touched']}",
            ORDER_LOG_FILE,
        )


def main() -> None:
    log_message("===== ORDER CYCLE START =====", ORDER_LOG_FILE)

    try:
        process_open_orders()
    except Exception as e:
        log_message(f"process_open_orders error={e}", ORDER_LOG_FILE)

    try:
        scan_and_create_candidates()
    except Exception as e:
        log_message(f"scan_and_create_candidates error={e}", ORDER_LOG_FILE)

    log_message("===== ORDER CYCLE END =====", ORDER_LOG_FILE)


if __name__ == "__main__":
    main()