import time
from typing import Any, Dict, List, Set

from config import CONFIG
from market import get_tradeable_symbols, get_market_snapshot
from strategy import get_setup, rank_setups
from telegram_alert import alert_new_order
from utils import (
    log_message,
    new_order_id,
    order_fieldnames,
    read_csv,
    safe_float,
    utc_now_str,
    write_csv,
)


def load_open_orders() -> List[Dict[str, Any]]:
    rows = read_csv(CONFIG.FILES.OPEN_ORDERS_CSV)
    return [r for r in rows if r.get("status") == "OPEN_ORDER"]


def save_open_orders(rows: List[Dict[str, Any]]) -> None:
    write_csv(CONFIG.FILES.OPEN_ORDERS_CSV, rows, order_fieldnames())


def load_open_positions_symbols() -> Set[str]:
    rows = read_csv(CONFIG.FILES.OPEN_POSITIONS_CSV)
    return {r["symbol"] for r in rows if r.get("status") == "OPEN_POSITION"}


def candidate_to_order(setup: Dict[str, Any], live_price: float) -> Dict[str, Any]:
    now = utc_now_str()
    return {
        "order_id": new_order_id(setup["symbol"], setup["side"]),
        "symbol": setup["symbol"],
        "side": setup["side"],
        "entry_zone_low": round(setup["entry_zone_low"], 8),
        "entry_zone_high": round(setup["entry_zone_high"], 8),
        "entry_trigger": round(setup["entry_trigger"], 8),
        "sl": round(setup["sl"], 8),
        "tp": round(setup["tp"], 8),
        "rr": setup["rr"],
        "score": setup["score"],
        "tf_context": setup["tf_context"],
        "setup_type": setup["setup_type"],
        "setup_reason": setup["setup_reason"],
        "created_at": now,
        "updated_at": now,
        "expires_at": "",
        "status": "OPEN_ORDER",
        "live_price": round(live_price, 8),
        "zone_touched": 0,
        "alarm_touched_sent": 0,
        "alarm_near_trigger_sent": 0,
        "last_alarm_at": "",
        "expected_net_pnl_pct": setup["expected_net_pnl_pct"],
        "stop_net_loss_pct": setup["stop_net_loss_pct"],
        "volume_24h_usdt": "",
        "spread_pct": setup["spread_pct"],
        "funding_rate_pct": setup["funding_rate_pct"],
    }


def dedupe_orders(existing: List[Dict[str, Any]], new_orders: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    existing_keys = {(r["symbol"], r["side"], r["setup_type"]) for r in existing if r.get("status") == "OPEN_ORDER"}
    out = existing[:]
    for row in new_orders:
        key = (row["symbol"], row["side"], row["setup_type"])
        if key in existing_keys:
            continue
        existing_keys.add(key)
        out.append(row)
    return out


def generate_orders() -> None:
    existing_orders = load_open_orders()
    open_pos_symbols = load_open_positions_symbols()

    symbols = get_tradeable_symbols()
    setups: List[Dict[str, Any]] = []

    log_message(f"ORDER_SCAN_START symbols={len(symbols)}", CONFIG.FILES.ORDER_LOG_FILE)

    for symbol in symbols:
        try:
            if symbol in open_pos_symbols:
                continue
            if any(r["symbol"] == symbol for r in existing_orders):
                continue

            market = get_market_snapshot(symbol)
            setup = get_setup(symbol, market)
            if not setup:
                continue

            if setup["score"] < CONFIG.TRADE.SCORE_MIN:
                continue
            if setup["expected_net_pnl_pct"] < CONFIG.TRADE.MIN_EXPECTED_NET_PNL_PCT:
                continue

            live_price = market["price"]
            order_row = candidate_to_order(setup, live_price)
            setups.append(order_row)

        except Exception as e:
            log_message(f"ORDER_SCAN_FAIL symbol={symbol} error={e}", CONFIG.FILES.ORDER_LOG_FILE)

    ranked = rank_setups(setups)
    free_slots = max(CONFIG.TRADE.MAX_OPEN_POSITIONS - len(open_pos_symbols), 0)
    selected = ranked[:free_slots]

    merged = dedupe_orders(existing_orders, selected)
    save_open_orders(merged)

    for row in selected:
        log_message(
            f"NEW_ORDER {row['symbol']} {row['side']} trigger={row['entry_trigger']} "
            f"sl={row['sl']} tp={row['tp']} score={row['score']} "
            f"rr={row['rr']} exp_net={row['expected_net_pnl_pct']}",
            CONFIG.FILES.ORDER_LOG_FILE,
        )
        if CONFIG.TRADE.ORDER_ALERT:
            alert_new_order(row)

    log_message(
        f"ORDER_SCAN_DONE candidates={len(setups)} selected={len(selected)} total_open_orders={len(merged)}",
        CONFIG.FILES.ORDER_LOG_FILE,
    )


def run_order_loop() -> None:
    log_message(
        f"===== ORDER LOOP START mode={CONFIG.ENGINE.EXECUTION_MODE} =====",
        CONFIG.FILES.ORDER_LOG_FILE,
    )
    while True:
        try:
            generate_orders()
        except Exception as e:
            log_message(f"ORDER_LOOP_ERROR error={e}", CONFIG.FILES.ORDER_LOG_FILE)
        time.sleep(CONFIG.TRADE.ORDER_LOOP_SECONDS)


if __name__ == "__main__":
    run_order_loop()