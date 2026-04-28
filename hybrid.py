# -*- coding: utf-8 -*-
"""
hybrid.py

Top-N volume hybrid scanner:
- scans futures tradeable symbols from market.py
- builds a FUTURE candidate using strategy.get_setup()
- builds a SPOT candidate from the same analysis when the setup is LONG
- keeps the best candidate per symbol
- reuses order.py reconciliation / cleanup / CSV persistence flow
- REAL submission is applied ONLY to FUTURE candidates

Important:
- This codebase currently exposes only Binance Futures market/execution clients.
- So SPOT orders here are generated and tracked locally, but not sent to Binance
  unless a separate spot execution client is added later.
"""

import time
from typing import Any, Dict, List, Optional, Set, Tuple

from config import CONFIG
from market import client as market_client
from market import get_market_snapshot, get_tradeable_symbols
from order import (
    ORDER_LOG_FILE,
    _get_max_open_orders,
    _is_real_mode,
    _run_real_mode_execution,
    build_candidate_orders,
    candidate_to_order,
    cleanup_order_book,
    load_all_orders,
    load_open_positions_symbols,
    load_position_order_ids,
    reconcile_orders,
    refresh_open_order_market_state,
    save_all_orders,
)
from strategy import get_setup, rank_setups
from telegram_alert import alert_new_order
from utils import log_message, safe_float


def _hybrid_top_n() -> int:
    trade_cfg = getattr(CONFIG, "TRADE", None)
    return int(getattr(trade_cfg, "HYBRID_TOP_N", getattr(trade_cfg, "MAX_SYMBOLS", 30)))


def _score_min() -> int:
    trade_cfg = getattr(CONFIG, "TRADE", None)
    return int(getattr(trade_cfg, "SCORE_MIN", 5))


def _quote_volume_map() -> Dict[str, float]:
    out: Dict[str, float] = {}
    try:
        rows = market_client.get_24h_ticker()
    except Exception as e:
        log_message(f"HYBRID_VOLUME_MAP_FAIL error={e}", ORDER_LOG_FILE)
        return out

    for row in rows:
        symbol = str(row.get("symbol", "")).strip().upper()
        if not symbol:
            continue
        out[symbol] = safe_float(row.get("quoteVolume", 0))
    return out


def _market_type_from_reason(reason: str) -> str:
    value = str(reason or "").strip().upper()
    if value.startswith("SPOT_"):
        return "SPOT"
    return "FUTURE"


def _is_future_row(row: Dict[str, Any]) -> bool:
    reason = str(row.get("setup_reason", "")).strip().upper()
    side = str(row.get("side", "")).strip().upper()

    if reason.startswith("FUTURE_"):
        return True

    # SHORT can only be futures in this hybrid model
    if side == "SHORT":
        return True

    return False


def _spot_candidate_from_setup(setup: Dict[str, Any], market_snapshot: Dict[str, Any], volume_24h: float) -> Optional[Dict[str, Any]]:
    """
    Convert a LONG futures-style setup into a SPOT candidate.
    SHORT is blocked for spot.
    """
    if not setup:
        return None

    if str(setup.get("side", "")).strip().upper() != "LONG":
        return None

    spot = dict(setup)
    original_reason = str(spot.get("setup_reason", "")).strip().upper() or "LONG"
    spot["setup_reason"] = f"SPOT_{original_reason}"
    spot["volume_24h_usdt"] = round(volume_24h, 2)

    # Spot has no funding cost in execution, so we do not penalize it further.
    # Slight tie preference to spot for same LONG structure.
    spot["score"] = int(safe_float(spot.get("score", 0)))
    spot["hybrid_market_type"] = "SPOT"
    spot["hybrid_bias"] = 1
    return spot


def _future_candidate_from_setup(setup: Dict[str, Any], volume_24h: float) -> Optional[Dict[str, Any]]:
    if not setup:
        return None

    fut = dict(setup)
    original_reason = str(fut.get("setup_reason", "")).strip().upper() or str(fut.get("side", "")).strip().upper()
    fut["setup_reason"] = f"FUTURE_{original_reason}"
    fut["volume_24h_usdt"] = round(volume_24h, 2)
    fut["hybrid_market_type"] = "FUTURE"
    fut["hybrid_bias"] = 0
    return fut


def _best_setup_for_symbol(symbol: str, volume_24h_map: Dict[str, float]) -> Optional[Dict[str, Any]]:
    try:
        market = get_market_snapshot(symbol)
    except Exception as e:
        log_message(f"HYBRID_MARKET_FAIL symbol={symbol} error={e}", ORDER_LOG_FILE)
        return None

    if not market:
        return None

    setup = get_setup(symbol, market)
    if not setup:
        return None

    score = int(safe_float(setup.get("score", 0)))
    if score < _score_min():
        log_message(
            f"HYBRID_SKIP_SCORE symbol={symbol} score={score} min={_score_min()}",
            ORDER_LOG_FILE,
        )
        return None

    volume_24h = safe_float(volume_24h_map.get(symbol, 0))
    future_candidate = _future_candidate_from_setup(setup, volume_24h)
    spot_candidate = _spot_candidate_from_setup(setup, market, volume_24h)

    options: List[Dict[str, Any]] = []
    if future_candidate:
        options.append(future_candidate)
    if spot_candidate:
        options.append(spot_candidate)

    if not options:
        return None

    # best score wins; then expected pnl; then rr; then slight bias to spot on exact ties
    options = sorted(
        options,
        key=lambda x: (
            int(safe_float(x.get("score", 0))),
            safe_float(x.get("expected_net_pnl_pct", 0)),
            safe_float(x.get("rr", 0)),
            int(safe_float(x.get("hybrid_bias", 0))),
        ),
        reverse=True,
    )
    chosen = options[0]

    log_message(
        f"HYBRID_PICK symbol={symbol} market_type={chosen.get('hybrid_market_type')} "
        f"side={chosen.get('side')} score={chosen.get('score')} "
        f"exp_net={chosen.get('expected_net_pnl_pct')} rr={chosen.get('rr')} "
        f"reason={chosen.get('setup_reason')}",
        ORDER_LOG_FILE,
    )
    return chosen


def build_hybrid_candidate_orders(
    symbols: List[str],
    all_orders: List[Dict[str, Any]],
    open_pos_symbols: Set[str],
) -> List[Dict[str, Any]]:
    """
    Unlike order.build_candidate_orders(), this deliberately keeps
    only the best SPOT/FUTURE choice per symbol.
    """
    _ = all_orders  # kept for interface symmetry and future extensions
    candidates: List[Dict[str, Any]] = []
    volume_24h_map = _quote_volume_map()

    for symbol in symbols:
        try:
            if symbol in open_pos_symbols:
                continue

            best_setup = _best_setup_for_symbol(symbol, volume_24h_map)
            if not best_setup:
                continue

            live_price = safe_float(best_setup.get("entry_trigger", 0))
            try:
                market = get_market_snapshot(symbol)
                live_price = safe_float(market.get("price", live_price))
            except Exception:
                pass

            row = candidate_to_order(best_setup, live_price)
            candidates.append(row)

        except Exception as e:
            log_message(f"HYBRID_SCAN_FAIL symbol={symbol} error={e}", ORDER_LOG_FILE)

    # rank across symbols after each symbol already picked its own best market type
    return rank_setups(candidates)


def generate_hybrid_orders() -> None:
    open_pos_symbols = load_open_positions_symbols()
    position_order_ids = load_position_order_ids()

    # PRE cleanup
    all_orders = load_all_orders()
    all_orders, pre_cancelled = cleanup_order_book(
        all_orders=all_orders,
        open_pos_symbols=open_pos_symbols,
        position_order_ids=position_order_ids,
        stage="PRE",
    )
    save_all_orders(all_orders)

    # refresh live market state on current open orders
    all_orders = refresh_open_order_market_state(all_orders)
    save_all_orders(all_orders)

    existing_open_orders = [r for r in all_orders if r.get("status") == "OPEN_ORDER"]

    # top-N volume universe from market.py
    symbols = get_tradeable_symbols()[: _hybrid_top_n()]

    log_message(
        f"HYBRID_SCAN_START symbols={len(symbols)} open_orders={len(existing_open_orders)} "
        f"positions={len(open_pos_symbols)} max_open_orders={_get_max_open_orders()} "
        f"top_n={_hybrid_top_n()} pre_cancelled={pre_cancelled}",
        ORDER_LOG_FILE,
    )

    candidate_orders = build_hybrid_candidate_orders(
        symbols=symbols,
        all_orders=all_orders,
        open_pos_symbols=open_pos_symbols,
    )

    updated_rows, selected_new_orders, refreshed_count, cancelled_count = reconcile_orders(
        all_orders=all_orders,
        candidate_orders=candidate_orders,
        open_pos_symbols=open_pos_symbols,
    )

    # POST cleanup
    open_pos_symbols_post = load_open_positions_symbols()
    position_order_ids_post = load_position_order_ids()

    final_rows, post_cancelled = cleanup_order_book(
        all_orders=updated_rows,
        open_pos_symbols=open_pos_symbols_post,
        position_order_ids=position_order_ids_post,
        stage="POST",
    )

    # REAL submit only for FUTURE rows
    if _is_real_mode():
        future_selected_new_orders = [r for r in selected_new_orders if _is_future_row(r)]
        final_rows = _run_real_mode_execution(
            all_rows=final_rows,
            selected_new_orders=future_selected_new_orders,
        )

    save_all_orders(final_rows)

    selected_order_ids = {
        str(r.get("order_id", "")).strip()
        for r in selected_new_orders
        if str(r.get("order_id", "")).strip()
    }

    final_selected_open_rows = [
        r for r in final_rows
        if str(r.get("order_id", "")).strip() in selected_order_ids
        and str(r.get("status", "")).strip() == "OPEN_ORDER"
    ]

    for row in final_selected_open_rows:
        market_type = _market_type_from_reason(str(row.get("setup_reason", "")))
        log_message(
            f"HYBRID_NEW_ORDER {row['symbol']} market_type={market_type} side={row['side']} "
            f"trigger={row['entry_trigger']} sl={row['sl']} tp={row['tp']} "
            f"score={row['score']} rr={row['rr']} exp_net={row['expected_net_pnl_pct']} "
            f"vol24h={row.get('volume_24h_usdt', 0)} zone_touched={row.get('zone_touched', 0)}",
            ORDER_LOG_FILE,
        )
        if getattr(CONFIG.TRADE, "ORDER_ALERT", False):
            alert_new_order(row)

    total_open_orders = sum(1 for r in final_rows if r.get("status") == "OPEN_ORDER")
    future_selected_count = sum(1 for r in selected_new_orders if _is_future_row(r))
    spot_selected_count = len(selected_new_orders) - future_selected_count

    log_message(
        f"HYBRID_SCAN_DONE candidates={len(candidate_orders)} new_selected={len(selected_new_orders)} "
        f"spot_selected={spot_selected_count} future_selected={future_selected_count} "
        f"symbols={len(symbols)} refreshed={refreshed_count} cancelled={cancelled_count} "
        f"post_cancelled={post_cancelled} total_open_orders={total_open_orders}",
        ORDER_LOG_FILE,
    )


def run_hybrid() -> None:
    time.sleep(1.5)
    try:
        generate_hybrid_orders()
    except Exception as e:
        log_message(f"HYBRID_ERROR error={e}", ORDER_LOG_FILE)


if __name__ == "__main__":
    run_hybrid()
