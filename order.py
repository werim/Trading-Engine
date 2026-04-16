# -*- coding: utf-8 -*-
import time
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Set, Tuple

from binance_real import BinanceFuturesClient
from config import CONFIG
from market import get_market_snapshot, get_symbol_meta, get_tradeable_symbols
from strategy import get_setup, rank_setups
from telegram_alert import alert_new_order, alert_position_opened
from utils import (
    log_message,
    new_order_id,
    new_position_id,
    order_fieldnames,
    position_fieldnames,
    read_csv,
    round_step,
    round_tick,
    safe_float,
    utc_now_str,
    write_csv,
)


ORDER_LOG_FILE = CONFIG.FILES.ORDER_LOG_FILE


def _utc_now_dt() -> datetime:
    return datetime.now(timezone.utc)


def _parse_utc_dt(value: str) -> Optional[datetime]:
    if not value:
        return None

    patterns = [
        "%Y-%m-%d %H:%M:%S UTC",
        "%Y-%m-%dT%H:%M:%S.%fZ",
        "%Y-%m-%dT%H:%M:%SZ",
    ]

    for fmt in patterns:
        try:
            dt = datetime.strptime(value, fmt)
            return dt.replace(tzinfo=timezone.utc)
        except Exception:
            continue

    return None


def _dt_to_utc_str(dt: datetime) -> str:
    return dt.astimezone(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")


def _get_order_expiry_hours() -> float:
    trade_cfg = getattr(CONFIG, "TRADE", None)
    return float(getattr(trade_cfg, "ORDER_EXPIRY_HOURS", 1))


def _get_max_open_orders() -> int:
    trade_cfg = getattr(CONFIG, "TRADE", None)
    fallback = int(getattr(trade_cfg, "MAX_OPEN_POSITIONS", 5))
    return int(getattr(trade_cfg, "MAX_OPEN_ORDERS", fallback))


def _make_expires_at() -> str:
    expiry_hours = _get_order_expiry_hours()
    dt = _utc_now_dt() + timedelta(hours=expiry_hours)
    return _dt_to_utc_str(dt)


def _is_real_mode() -> bool:
    return str(CONFIG.ENGINE.EXECUTION_MODE).upper() == "REAL"


def _get_binance_client() -> Optional[BinanceFuturesClient]:
    if not _is_real_mode():
        return None
    return BinanceFuturesClient()


def _side_to_binance(side: str) -> str:
    return "BUY" if str(side).upper() == "LONG" else "SELL"


def _close_side_to_binance(side: str) -> str:
    return "SELL" if str(side).upper() == "LONG" else "BUY"


def _response_order_id(resp: Any) -> str:
    if isinstance(resp, dict):
        return str(resp.get("orderId", "") or resp.get("order_id", "")).strip()
    return ""


def _normalize_order_status(v: Any) -> str:
    return str(v or "").upper().strip()


def _response_avg_price(resp: Any) -> float:
    if not isinstance(resp, dict):
        return 0.0
    for key in ("avgPrice", "avg_price", "price"):
        value = safe_float(resp.get(key))
        if value > 0:
            return value
    return 0.0


def _response_executed_qty(resp: Any) -> float:
    if not isinstance(resp, dict):
        return 0.0
    for key in ("executedQty", "executed_qty", "cumQty", "cum_qty"):
        value = safe_float(resp.get(key))
        if value > 0:
            return value
    return 0.0


def _save_open_positions_rows(rows: List[Dict[str, Any]]) -> None:
    write_csv(CONFIG.FILES.OPEN_POSITIONS_CSV, rows, position_fieldnames())


def load_all_orders() -> List[Dict[str, Any]]:
    return read_csv(CONFIG.FILES.OPEN_ORDERS_CSV)


def load_open_orders() -> List[Dict[str, Any]]:
    rows = load_all_orders()
    return [r for r in rows if r.get("status") == "OPEN_ORDER"]


def save_all_orders(rows: List[Dict[str, Any]]) -> None:
    write_csv(CONFIG.FILES.OPEN_ORDERS_CSV, rows, order_fieldnames())


def load_open_positions_rows() -> List[Dict[str, Any]]:
    return read_csv(CONFIG.FILES.OPEN_POSITIONS_CSV)


def load_closed_positions_rows() -> List[Dict[str, Any]]:
    return read_csv(CONFIG.FILES.CLOSED_POSITIONS_CSV)


def load_open_positions_symbols() -> Set[str]:
    rows = load_open_positions_rows()
    return {
        str(r.get("symbol", "")).strip()
        for r in rows
        if r.get("status") == "OPEN_POSITION" and str(r.get("symbol", "")).strip()
    }


def load_position_order_ids() -> Set[str]:
    ids: Set[str] = set()

    for row in load_open_positions_rows():
        oid = str(row.get("order_id", "")).strip()
        if oid:
            ids.add(oid)

    for row in load_closed_positions_rows():
        oid = str(row.get("order_id", "")).strip()
        if oid:
            ids.add(oid)

    return ids


def order_key(row: Dict[str, Any]) -> Tuple[str, str, str]:
    return (
        str(row.get("symbol", "")).strip(),
        str(row.get("side", "")).strip(),
        str(row.get("setup_type", "")).strip(),
    )


def _row_ts_for_priority(row: Dict[str, Any]) -> datetime:
    """
    Yeni olan kazanmalı.
    created_at öncelikli, yoksa updated_at.
    """
    created_at = _parse_utc_dt(str(row.get("created_at", "")).strip())
    if created_at:
        return created_at

    updated_at = _parse_utc_dt(str(row.get("updated_at", "")).strip())
    if updated_at:
        return updated_at

    return datetime(1970, 1, 1, tzinfo=timezone.utc)


def _row_score(row: Dict[str, Any]) -> int:
    return int(safe_float(row.get("score", 0)))


def cancel_order(row: Dict[str, Any], reason: str) -> Dict[str, Any]:
    row = dict(row)
    row["status"] = "CANCELLED"
    row["updated_at"] = utc_now_str()
    log_message(
        f"ORDER_CANCEL {row.get('symbol')} {row.get('side')} setup={row.get('setup_type')} reason={reason}",
        ORDER_LOG_FILE,
    )
    return row


def is_order_expired(row: Dict[str, Any]) -> bool:
    expires_at = str(row.get("expires_at", "")).strip()
    if not expires_at:
        created_at = _parse_utc_dt(str(row.get("created_at", "")).strip())
        if not created_at:
            return False
        expires_dt = created_at + timedelta(hours=_get_order_expiry_hours())
        return _utc_now_dt() >= expires_dt

    expires_dt = _parse_utc_dt(expires_at)
    if not expires_dt:
        return False

    return _utc_now_dt() >= expires_dt


def candidate_to_order(setup: Dict[str, Any], live_price: float) -> Dict[str, Any]:
    now = utc_now_str()
    return {
        "order_id": new_order_id(setup["symbol"], setup["side"]),
        "symbol": setup["symbol"],
        "side": setup["side"],
        "entry_zone_low": round(safe_float(setup.get("entry_zone_low", 0)), 8),
        "entry_zone_high": round(safe_float(setup.get("entry_zone_high", 0)), 8),
        "entry_trigger": round(safe_float(setup.get("entry_trigger", 0)), 8),
        "sl": round(safe_float(setup.get("sl", 0)), 8),
        "tp": round(safe_float(setup.get("tp", 0)), 8),
        "rr": safe_float(setup.get("rr", 0)),
        "score": int(safe_float(setup.get("score", 0))),
        "tf_context": setup.get("tf_context", ""),
        "setup_type": setup.get("setup_type", ""),
        "setup_reason": setup.get("setup_reason", ""),
        "created_at": now,
        "updated_at": now,
        "expires_at": _make_expires_at(),
        "status": "OPEN_ORDER",
        "live_price": round(safe_float(live_price), 8),
        "exchange_order_id": "",
        "exchange_status": "",
        "order_type": "LIMIT" if bool(getattr(CONFIG.TRADE, "USE_LIMIT_ENTRY", False)) else "MARKET",
        "submitted_qty": 0.0,
        "executed_qty": 0.0,
        "avg_fill_price": 0.0,
        "zone_touched": 0,
        "alarm_touched_sent": 0,
        "alarm_near_trigger_sent": 0,
        "last_alarm_at": "",
        "expected_net_pnl_pct": safe_float(setup.get("expected_net_pnl_pct", 0)),
        "stop_net_loss_pct": safe_float(setup.get("stop_net_loss_pct", 0)),
        "volume_24h_usdt": safe_float(setup.get("volume_24h_usdt", 0)),
        "spread_pct": safe_float(setup.get("spread_pct", 0)),
        "funding_rate_pct": safe_float(setup.get("funding_rate_pct", 0)),
    }


def is_better_order(new_row: Dict[str, Any], old_row: Dict[str, Any]) -> bool:
    new_score = _row_score(new_row)
    old_score = _row_score(old_row)

    if new_score > old_score:
        return True
    if new_score < old_score:
        return False

    new_exp = safe_float(new_row.get("expected_net_pnl_pct", 0))
    old_exp = safe_float(old_row.get("expected_net_pnl_pct", 0))

    if new_exp > old_exp + 1e-9:
        return True
    if new_exp < old_exp - 1e-9:
        return False

    new_vol = safe_float(new_row.get("volume_24h_usdt", 0))
    old_vol = safe_float(old_row.get("volume_24h_usdt", 0))

    if new_vol > old_vol:
        return True
    if new_vol < old_vol:
        return False
    return False
    # Tam eşitlikte yeni olan kazansın
    # return _row_ts_for_priority(new_row) >= _row_ts_for_priority(old_row)


def refresh_existing_order(old_row: Dict[str, Any], new_row: Dict[str, Any]) -> Dict[str, Any]:
    refreshed = dict(old_row)

    fields_to_replace = [
        "entry_zone_low",
        "entry_zone_high",
        "entry_trigger",
        "sl",
        "tp",
        "rr",
        "score",
        "tf_context",
        "setup_reason",
        "setup_type",
        "side",
        "live_price",
        "expected_net_pnl_pct",
        "stop_net_loss_pct",
        "volume_24h_usdt",
        "spread_pct",
        "funding_rate_pct",
        "expires_at",
    ]

    for field in fields_to_replace:
        refreshed[field] = new_row.get(field, refreshed.get(field))

    refreshed["updated_at"] = utc_now_str()
    refreshed["status"] = "OPEN_ORDER"
    return refreshed


def dedupe_candidate_orders(candidate_orders: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """
    Aynı coin için tek candidate bırak.
    Yüksek score kalır, eşitse yeni olan kalır.
    """
    best_by_symbol: Dict[str, Dict[str, Any]] = {}

    for row in candidate_orders:
        symbol = str(row.get("symbol", "")).strip()
        if not symbol:
            continue

        existing = best_by_symbol.get(symbol)
        if existing is None or is_better_order(row, existing):
            best_by_symbol[symbol] = row

    ranked = rank_setups(list(best_by_symbol.values()))
    return ranked


def build_candidate_orders(
    symbols: List[str],
    existing_open_orders: List[Dict[str, Any]],
    open_pos_symbols: Set[str],
) -> List[Dict[str, Any]]:
    candidates: List[Dict[str, Any]] = []

    for symbol in symbols:
        try:
            if symbol in open_pos_symbols:
                continue

            market = get_market_snapshot(symbol)
            if not market:
                continue

            setup = get_setup(symbol, market)

            if not setup:
                continue

            if int(safe_float(setup.get("score", 0))) < CONFIG.TRADE.SCORE_MIN:
                continue

            if safe_float(setup.get("expected_net_pnl_pct", 0)) < CONFIG.TRADE.MIN_EXPECTED_NET_PNL_PCT:
                continue

            live_price = safe_float(market.get("price", 0))
            if live_price <= 0:
                continue

            row = candidate_to_order(setup, live_price)
            candidates.append(row)

        except Exception as e:
            log_message(f"ORDER_SCAN_FAIL symbol={symbol} error={e}", ORDER_LOG_FILE)

    return dedupe_candidate_orders(candidates)


def cleanup_order_book(
    all_orders: List[Dict[str, Any]],
    open_pos_symbols: Set[str],
    position_order_ids: Set[str],
    stage: str,
) -> Tuple[List[Dict[str, Any]], int]:
    """
    Baştaki ve sondaki temizlik:
    - open position varsa open order iptal
    - pozisyonlara geçmiş order_id tekrar OPEN_ORDER olamaz
    - aynı symbol için birden fazla OPEN_ORDER varsa:
      yüksek score kalır, eşitse yeni kalır, eski CANCELLED
    """
    cleaned_rows: List[Dict[str, Any]] = []
    cancelled_count = 0

    open_rows_by_symbol: Dict[str, List[Tuple[int, Dict[str, Any]]]] = {}

    for idx, row in enumerate(all_orders):
        status = str(row.get("status", "")).strip()
        symbol = str(row.get("symbol", "")).strip()
        order_id = str(row.get("order_id", "")).strip()

        if status != "OPEN_ORDER":
            cleaned_rows.append(row)
            continue

        if symbol in open_pos_symbols:
            cleaned_rows.append(cancel_order(row, f"{stage}_HAS_OPEN_POSITION"))
            cancelled_count += 1
            continue

        if order_id and order_id in position_order_ids:
            cleaned_rows.append(cancel_order(row, f"{stage}_FILLED_CONFLICT"))
            cancelled_count += 1
            continue

        open_rows_by_symbol.setdefault(symbol, []).append((idx, row))

    keep_by_original_index: Set[int] = set()
    cancel_by_original_index: Dict[int, Dict[str, Any]] = {}

    for symbol, indexed_rows in open_rows_by_symbol.items():
        if not indexed_rows:
            continue

        # en iyi row'u seç
        best_idx, best_row = indexed_rows[0]
        for idx, row in indexed_rows[1:]:
            if is_better_order(row, best_row):
                best_idx, best_row = idx, row

        keep_by_original_index.add(best_idx)

        for idx, row in indexed_rows:
            if idx == best_idx:
                continue

            reason = f"{stage}_DUPLICATE_SYMBOL_LOST"
            if _row_score(row) == _row_score(best_row):
                reason = f"{stage}_DUPLICATE_SYMBOL_EQUAL_SCORE_OLDER_CANCELLED"

            cancel_by_original_index[idx] = cancel_order(row, reason)
            cancelled_count += 1

    # sırayı koruyarak birleştir
    passthrough_open_rows: Dict[int, Dict[str, Any]] = {}
    for symbol_rows in open_rows_by_symbol.values():
        for idx, row in symbol_rows:
            if idx in keep_by_original_index:
                passthrough_open_rows[idx] = row

    for idx, row in enumerate(all_orders):
        status = str(row.get("status", "")).strip()

        if status != "OPEN_ORDER":
            continue

        if idx in cancel_by_original_index:
            cleaned_rows.append(cancel_by_original_index[idx])
        elif idx in passthrough_open_rows:
            cleaned_rows.append(passthrough_open_rows[idx])

    # stable sort by created/updated order is not needed because we preserved input order
    log_message(
        f"ORDER_CLEANUP stage={stage} cancelled={cancelled_count}",
        ORDER_LOG_FILE,
    )
    return cleaned_rows, cancelled_count


def reconcile_orders(
    all_orders: List[Dict[str, Any]],
    candidate_orders: List[Dict[str, Any]],
    open_pos_symbols: Set[str],
) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]], int, int]:
    """
    Coin başına tek açık order kuralı:
    - mevcut OPEN_ORDER varsa candidate ile kıyaslanır
    - yüksek score kalır
    - eşitse yeni candidate kalır, eski CANCELLED olur
    """
    selected_new_orders: List[Dict[str, Any]] = []
    refreshed_count = 0
    cancelled_count = 0

    output_rows: List[Dict[str, Any]] = []

    max_open_orders = _get_max_open_orders()
    current_open_orders_count = 0

    existing_open_by_symbol: Dict[str, Dict[str, Any]] = {}
    non_open_rows: List[Dict[str, Any]] = []

    for row in all_orders:
        if str(row.get("status", "")).strip() == "OPEN_ORDER":
            symbol = str(row.get("symbol", "")).strip()
            if symbol:
                existing_open_by_symbol[symbol] = row
            else:
                non_open_rows.append(row)
        else:
            non_open_rows.append(row)

    candidate_by_symbol: Dict[str, Dict[str, Any]] = {
        str(r.get("symbol", "")).strip(): r for r in candidate_orders if str(r.get("symbol", "")).strip()
    }

    processed_symbols: Set[str] = set()

    # önce mevcut open order'lar
    for row in all_orders:
        if str(row.get("status", "")).strip() != "OPEN_ORDER":
            continue

        symbol = str(row.get("symbol", "")).strip()
        if not symbol or symbol in processed_symbols:
            continue

        processed_symbols.add(symbol)

        if symbol in open_pos_symbols:
            output_rows.append(cancel_order(row, "HAS_OPEN_POSITION"))
            cancelled_count += 1
            continue

        if is_order_expired(row):
            output_rows.append(cancel_order(row, "EXPIRED"))
            cancelled_count += 1
            continue

        new_candidate = candidate_by_symbol.get(symbol)
        if not new_candidate:
            output_rows.append(cancel_order(row, "SETUP_GONE"))
            cancelled_count += 1
            continue

        if is_better_order(new_candidate, row):
            # yeni daha iyi ya da eşitlikte yeni kazandı
            output_rows.append(cancel_order(row, "REPLACED_BY_BETTER_OR_EQUAL_NEW_SYMBOL_ORDER"))
            output_rows.append(new_candidate)
            selected_new_orders.append(new_candidate)
            cancelled_count += 1
            current_open_orders_count += 1
        else:
            keep_row = dict(row)
            keep_row["live_price"] = new_candidate.get("live_price", keep_row.get("live_price", 0))
            keep_row["updated_at"] = utc_now_str()
            output_rows.append(keep_row)
            refreshed_count += 1
            current_open_orders_count += 1

        candidate_by_symbol.pop(symbol, None)

    # mevcut open order'ı olmayan symbol'lerden yeni ekle
    ranked_remaining = rank_setups(list(candidate_by_symbol.values()))
    free_order_slots = max(max_open_orders - current_open_orders_count, 0)

    for row in ranked_remaining[:free_order_slots]:
        symbol = str(row.get("symbol", "")).strip()
        if not symbol or symbol in open_pos_symbols:
            continue

        output_rows.append(row)
        selected_new_orders.append(row)
        current_open_orders_count += 1

    # non-open rows da en başta ekleyelim
    final_rows = non_open_rows + output_rows
    return final_rows, selected_new_orders, refreshed_count, cancelled_count


def _exchange_open_position_symbols(client: BinanceFuturesClient) -> Set[str]:
    symbols: Set[str] = set()
    try:
        for row in client.get_position_risk():
            symbol = str(row.get("symbol", "")).strip()
            if not symbol:
                continue
            if abs(safe_float(row.get("positionAmt", 0))) > 0:
                symbols.add(symbol)
    except Exception as e:
        log_message(f"REAL_POSITIONS_FETCH_FAIL error={e}", ORDER_LOG_FILE)
    return symbols


def _build_position_from_fill(
    order: Dict[str, Any],
    fill_price: float,
    fill_qty: float,
    sl_order_id: str = "",
    tp_order_id: str = "",
) -> Dict[str, Any]:
    now = utc_now_str()
    return {
        "position_id": new_position_id(order["symbol"], order["side"]),
        "order_id": order["order_id"],
        "symbol": order["symbol"],
        "side": order["side"],
        "entry": round(fill_price, 8),
        "qty": round(fill_qty, 8),
        "sl": round(safe_float(order["sl"]), 8),
        "tp": round(safe_float(order["tp"]), 8),
        "rr": safe_float(order.get("rr", 0)),
        "score": int(safe_float(order.get("score", 0))),
        "tf_context": order.get("tf_context", ""),
        "setup_type": order.get("setup_type", ""),
        "setup_reason": order.get("setup_reason", ""),
        "opened_at": now,
        "updated_at": now,
        "status": "OPEN_POSITION",
        "live_price": round(fill_price, 8),
        "pnl_pct": 0.0,
        "net_pnl_pct": 0.0,
        "net_pnl_usdt": 0.0,
        "fees_usdt": 0.0,
        "sl_order_id": sl_order_id,
        "tp_order_id": tp_order_id,
        "protection_armed": 1 if sl_order_id or tp_order_id else 0,
        "partial_taken": 0,
        "break_even_armed": 0,
        "highest_price": round(fill_price, 8),
        "lowest_price": round(fill_price, 8),
        "initial_qty": round(fill_qty, 8),
        "initial_risk": round(abs(fill_price - safe_float(order["sl"])), 8),
    }


def _arm_initial_protection(
    client: BinanceFuturesClient,
    symbol: str,
    side: str,
    qty: float,
    sl: float,
    tp: float,
) -> Tuple[str, str]:
    sl_resp = client.place_stop_market(
        symbol=symbol,
        side=_close_side_to_binance(side),
        stop_price=sl,
        reduce_only=True,
        quantity=qty,
    )
    tp_resp = client.place_take_profit_market(
        symbol=symbol,
        side=_close_side_to_binance(side),
        stop_price=tp,
        reduce_only=True,
        quantity=qty,
    )
    return _response_order_id(sl_resp), _response_order_id(tp_resp)


def _calc_submitted_qty(order: Dict[str, Any], symbol_meta: Dict[str, Any]) -> float:
    entry = safe_float(order.get("entry_trigger"))
    if entry <= 0:
        return 0.0
    raw_qty = (CONFIG.TRADE.USDT_PER_TRADE * CONFIG.TRADE.LEVERAGE) / entry
    qty_step = safe_float(symbol_meta.get("qty_step") or symbol_meta.get("stepSize"))
    min_qty = safe_float(symbol_meta.get("min_qty") or symbol_meta.get("minQty"))
    qty = round_step(raw_qty, qty_step) if qty_step > 0 else raw_qty
    if min_qty > 0 and qty < min_qty:
        qty = min_qty
    return qty


def _resolve_filled_order(
    client: BinanceFuturesClient,
    order_row: Dict[str, Any],
    status_resp: Dict[str, Any],
) -> Dict[str, Any]:
    positions = load_open_positions_rows()
    if any(
        str(p.get("order_id", "")).strip() == str(order_row.get("order_id", "")).strip()
        and p.get("status") == "OPEN_POSITION"
        for p in positions
    ):
        order_row["status"] = "FILLED_TO_POSITION"
        order_row["updated_at"] = utc_now_str()
        return order_row

    avg_price = _response_avg_price(status_resp)
    executed_qty = _response_executed_qty(status_resp)
    if avg_price <= 0:
        avg_price = safe_float(order_row.get("entry_trigger"))
    if executed_qty <= 0:
        executed_qty = safe_float(order_row.get("submitted_qty"))
    if executed_qty <= 0:
        raise RuntimeError(f"FILLED_WITH_ZERO_QTY symbol={order_row.get('symbol')}")

    sl_order_id, tp_order_id = _arm_initial_protection(
        client=client,
        symbol=order_row["symbol"],
        side=order_row["side"],
        qty=executed_qty,
        sl=safe_float(order_row["sl"]),
        tp=safe_float(order_row["tp"]),
    )

    pos = _build_position_from_fill(
        order=order_row,
        fill_price=avg_price,
        fill_qty=executed_qty,
        sl_order_id=sl_order_id,
        tp_order_id=tp_order_id,
    )
    positions.append(pos)
    _save_open_positions_rows(positions)
    alert_position_opened(pos)

    order_row["status"] = "FILLED_TO_POSITION"
    order_row["executed_qty"] = round(executed_qty, 8)
    order_row["avg_fill_price"] = round(avg_price, 8)
    order_row["exchange_status"] = "FILLED"
    order_row["updated_at"] = utc_now_str()
    return order_row


def _run_real_mode_execution(
    all_rows: List[Dict[str, Any]],
    selected_new_orders: List[Dict[str, Any]],
) -> List[Dict[str, Any]]:
    client = _get_binance_client()
    if client is None:
        return all_rows

    open_orders_by_symbol: Dict[str, Dict[str, Any]] = {}
    try:
        for row in client.get_open_orders():
            symbol = str(row.get("symbol", "")).strip()
            if symbol:
                open_orders_by_symbol[symbol] = row
    except Exception as e:
        log_message(f"REAL_OPEN_ORDERS_FETCH_FAIL error={e}", ORDER_LOG_FILE)

    open_pos_symbols = _exchange_open_position_symbols(client)
    row_by_order_id = {
        str(r.get("order_id", "")).strip(): r for r in all_rows if str(r.get("order_id", "")).strip()
    }

    # 1) Sync existing submitted orders with Binance truth.
    for row in all_rows:
        if row.get("status") != "OPEN_ORDER":
            continue
        exchange_order_id = str(row.get("exchange_order_id", "")).strip()
        if not exchange_order_id:
            continue

        try:
            status_resp = client.get_order(symbol=row["symbol"], order_id=int(exchange_order_id))
            status = _normalize_order_status(status_resp.get("status"))
            row["exchange_status"] = status
            row["executed_qty"] = round(_response_executed_qty(status_resp), 8)
            row["avg_fill_price"] = round(_response_avg_price(status_resp), 8)
            row["updated_at"] = utc_now_str()

            if status == "FILLED":
                row_by_order_id[row["order_id"]] = _resolve_filled_order(client, row, status_resp)
            elif status in {"CANCELED", "EXPIRED", "REJECTED"}:
                row["status"] = status
        except Exception as e:
            log_message(
                f"REAL_ORDER_SYNC_FAIL symbol={row.get('symbol')} local_order_id={row.get('order_id')} error={e}",
                ORDER_LOG_FILE,
            )

    # 2) Submit new candidates only when no open position/order exists.
    for candidate in selected_new_orders:
        local_order_id = str(candidate.get("order_id", "")).strip()
        row = row_by_order_id.get(local_order_id)
        if not row or row.get("status") != "OPEN_ORDER":
            continue
        if str(row.get("exchange_order_id", "")).strip():
            continue

        symbol = str(row.get("symbol", "")).strip()
        if not symbol:
            continue
        if symbol in open_pos_symbols:
            row["status"] = "CANCELLED"
            row["updated_at"] = utc_now_str()
            continue
        if symbol in open_orders_by_symbol:
            row["status"] = "CANCELLED"
            row["updated_at"] = utc_now_str()
            log_message(f"REAL_SKIP_EXISTING_OPEN_ORDER symbol={symbol}", ORDER_LOG_FILE)
            continue

        symbol_meta = get_symbol_meta(symbol)
        if not symbol_meta:
            row["status"] = "CANCELLED"
            row["updated_at"] = utc_now_str()
            continue

        qty = _calc_submitted_qty(row, symbol_meta)
        if qty <= 0:
            row["status"] = "FAILED_QTY"
            row["updated_at"] = utc_now_str()
            continue

        order_type = str(row.get("order_type", "LIMIT")).upper()
        price_tick = safe_float(symbol_meta.get("price_tick") or symbol_meta.get("tickSize"))
        entry_price = safe_float(row.get("entry_trigger"))
        entry_price = round_tick(entry_price, price_tick) if price_tick > 0 else entry_price

        try:
            client.set_leverage(symbol, CONFIG.TRADE.LEVERAGE)
            if order_type == "MARKET":
                resp = client.place_market_order(
                    symbol=symbol,
                    side=_side_to_binance(row["side"]),
                    quantity=qty,
                    reduce_only=False,
                )
            else:
                resp = client.place_limit_order(
                    symbol=symbol,
                    side=_side_to_binance(row["side"]),
                    quantity=qty,
                    price=entry_price,
                    reduce_only=False,
                )

            exchange_order_id = _response_order_id(resp)
            row["exchange_order_id"] = exchange_order_id
            row["submitted_qty"] = round(qty, 8)
            row["exchange_status"] = _normalize_order_status(resp.get("status", "NEW"))
            row["updated_at"] = utc_now_str()

            status_resp = resp
            if exchange_order_id:
                status_resp = client.get_order(symbol=symbol, order_id=int(exchange_order_id))
                row["exchange_status"] = _normalize_order_status(status_resp.get("status"))
                row["executed_qty"] = round(_response_executed_qty(status_resp), 8)
                row["avg_fill_price"] = round(_response_avg_price(status_resp), 8)

                if row["exchange_status"] == "FILLED":
                    row_by_order_id[row["order_id"]] = _resolve_filled_order(client, row, status_resp)
                    open_pos_symbols.add(symbol)
                else:
                    open_orders_by_symbol[symbol] = status_resp

            log_message(
                f"REAL_ENTRY_SUBMITTED symbol={symbol} side={row['side']} qty={row['submitted_qty']} "
                f"exchange_order_id={row.get('exchange_order_id')} status={row.get('exchange_status')}",
                ORDER_LOG_FILE,
            )
        except Exception as e:
            row["status"] = "FAILED_SUBMIT"
            row["updated_at"] = utc_now_str()
            log_message(f"REAL_ENTRY_SUBMIT_FAIL symbol={symbol} error={e}", ORDER_LOG_FILE)

    return all_rows


def generate_orders() -> None:
    open_pos_symbols = load_open_positions_symbols()
    position_order_ids = load_position_order_ids()

    # BAŞTA TEMİZLİK
    all_orders = load_all_orders()
    all_orders, pre_cancelled = cleanup_order_book(
        all_orders=all_orders,
        open_pos_symbols=open_pos_symbols,
        position_order_ids=position_order_ids,
        stage="PRE",
    )
    save_all_orders(all_orders)

    existing_open_orders = [r for r in all_orders if r.get("status") == "OPEN_ORDER"]
    symbols = get_tradeable_symbols()
    log_message(
        f"ORDER_SCAN_START symbols={len(symbols)} open_orders={len(existing_open_orders)} "
        f"positions={len(open_pos_symbols)} max_open_orders={_get_max_open_orders()}"
        f"pre_cancelled={pre_cancelled}",
        ORDER_LOG_FILE,
    )

    candidate_orders = build_candidate_orders(
        symbols=symbols,
        existing_open_orders=existing_open_orders,
        open_pos_symbols=open_pos_symbols,
    )

    updated_rows, selected_new_orders, refreshed_count, cancelled_count = reconcile_orders(
        all_orders=all_orders,
        candidate_orders=candidate_orders,
        open_pos_symbols=open_pos_symbols,
    )

    # SONDA TEMİZLİK
    open_pos_symbols_post = load_open_positions_symbols()
    position_order_ids_post = load_position_order_ids()

    final_rows, post_cancelled = cleanup_order_book(
        all_orders=updated_rows,
        open_pos_symbols=open_pos_symbols_post,
        position_order_ids=position_order_ids_post,
        stage="POST",
    )

    if _is_real_mode():
        final_rows = _run_real_mode_execution(
            all_rows=final_rows,
            selected_new_orders=selected_new_orders,
        )

    save_all_orders(final_rows)

    for row in selected_new_orders:
        if str(row.get("status", "")).strip() != "OPEN_ORDER":
            continue

        log_message(
            f"NEW_ORDER {row['symbol']} {row['side']} trigger={row['entry_trigger']} "
            f"sl={row['sl']} tp={row['tp']} score={row['score']} rr={row['rr']} "
            f"exp_net={row['expected_net_pnl_pct']} vol24h={row['volume_24h_usdt']}",
            ORDER_LOG_FILE,
        )
        if getattr(CONFIG.TRADE, "ORDER_ALERT", False):
            alert_new_order(row)

    total_open_orders = sum(1 for r in final_rows if r.get("status") == "OPEN_ORDER")

    log_message(
        f"ORDER_SCAN_DONE candidates={len(candidate_orders)} new_selected={len(selected_new_orders)} "
        f"symbols={len(symbols)} refreshed={refreshed_count} cancelled={cancelled_count} "
        f"post_cancelled={post_cancelled} total_open_orders={total_open_orders}",
        ORDER_LOG_FILE,
    )


def run_order() -> None:
    time.sleep(1.5)
    """
    log_message(
        f"===== ORDER START mode={CONFIG.ENGINE.EXECUTION_MODE} =====",
        ORDER_LOG_FILE,
    )"""
    try:
        generate_orders()
    except Exception as e:
        log_message(f"ORDER_ERROR error={e}", ORDER_LOG_FILE)


if __name__ == "__main__":
    run_order()
