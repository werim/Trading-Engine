from typing import Any, Dict, List, Tuple

from config import CONFIG
from utils import safe_float


def calc_position_size(
    entry: float,
    sl: float,
    account_balance: float,
    risk_pct: float,
    symbol_meta: Dict[str, Any],
) -> float:
    risk_amount = account_balance * (risk_pct / 100.0)
    unit_risk = abs(entry - sl)
    if entry <= 0 or risk_amount <= 0 or unit_risk <= 0:
        return 0.0

    stop_pct = (unit_risk / entry) * 100.0
    if stop_pct < 0.08:
        return 0.0
    if stop_pct > 4.0:
        return 0.0

    raw_qty = risk_amount / unit_risk
    step_size = safe_float(symbol_meta.get("step_size"), 0.0)
    min_qty = safe_float(symbol_meta.get("min_qty"), 0.0)

    if step_size > 0:
        from utils import floor_qty_to_step
        raw_qty = floor_qty_to_step(raw_qty, step_size)

    min_notional = safe_float(symbol_meta.get("min_notional"), 0.0)
    if raw_qty < min_qty:
        return 0.0
    if min_notional > 0 and (raw_qty * entry) < min_notional:
        return 0.0
    return raw_qty


def check_daily_loss_limit(closed_positions: List[Dict[str, Any]], max_daily_loss_pct: float) -> Tuple[bool, str]:
    # Starter version. Gerçek sistemde bugünün kayıtlarını filtrele.
    return True, "OK"


def check_symbol_cooldown(symbol: str, closed_positions: List[Dict[str, Any]], cooldown_minutes: int) -> Tuple[bool, str]:
    # Starter version. Son kapanış zamanına bakılabilir.
    return True, "OK"


def can_open_new_order(
    candidate: Dict[str, Any],
    open_orders: List[Dict[str, Any]],
    open_positions: List[Dict[str, Any]],
) -> Tuple[bool, str]:
    if len(open_orders) >= CONFIG.TRADE.MAX_OPEN_ORDERS:
        return False, "MAX_OPEN_ORDERS_REACHED"

    if len(open_positions) >= CONFIG.TRADE.MAX_OPEN_POSITIONS:
        return False, "MAX_OPEN_POSITIONS_REACHED"

    symbol = candidate["symbol"]
    for pos in open_positions:
        if pos.get("symbol") == symbol and pos.get("status") == "OPEN_POSITION":
            return False, "SYMBOL_HAS_OPEN_POSITION"

    side = str(candidate.get("side", "")).upper()
    rr = safe_float(candidate.get("rr"))
    if "PULLBACK" in str(candidate.get("setup_type", "")).upper():
        rr_floor = max(1.35, CONFIG.FILTER.MIN_RR * 0.85)
    elif "BREAKOUT" in str(candidate.get("setup_type", "")).upper() or "RECLAIM" in str(candidate.get("setup_type", "")).upper():
        rr_floor = max(1.25, CONFIG.FILTER.MIN_RR * 0.80)
    else:
        rr_floor = max(1.2, CONFIG.FILTER.MIN_RR * 0.75)
    if rr < rr_floor:
        return False, f"RR_BELOW_SETUP_FLOOR:{rr_floor:.2f}"
    if side not in {"LONG", "SHORT"}:
        return False, "INVALID_SIDE"

    return True, "OK"
