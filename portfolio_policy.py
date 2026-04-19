# portfolio_policy.py
from __future__ import annotations

from typing import Any, Dict, List


def apply_portfolio_policy(
    ctx: Dict[str, Any],
    reviewer_result: Dict[str, Any],
    portfolio_state: Dict[str, Any],
) -> Dict[str, Any]:
    """
    portfolio_state örnek:
    {
        "open_positions_count": 1,
        "open_orders_count": 2,
        "max_open_positions": 3,
        "max_open_orders": 5,
        "daily_realized_loss_pct": 1.8,
        "max_daily_loss_pct": 3.0,
        "side_open_counts": {"LONG": 2, "SHORT": 0},
        "symbol_blacklist": [],
        "correlated_long_exposure": 3,
        "correlated_short_exposure": 0,
        "max_correlated_exposure": 3,
    }
    """
    allow = bool(reviewer_result.get("allow", False))
    reasons: List[str] = list(reviewer_result.get("reasons", []))
    flags: List[str] = list(reviewer_result.get("flags", []))
    confidence = float(reviewer_result.get("confidence", 0.0) or 0.0)

    symbol = str(ctx.get("symbol", ""))
    side = str(ctx.get("side", ""))

    open_positions_count = int(portfolio_state.get("open_positions_count", 0) or 0)
    max_open_positions = int(portfolio_state.get("max_open_positions", 999) or 999)

    open_orders_count = int(portfolio_state.get("open_orders_count", 0) or 0)
    max_open_orders = int(portfolio_state.get("max_open_orders", 999) or 999)

    daily_realized_loss_pct = float(portfolio_state.get("daily_realized_loss_pct", 0.0) or 0.0)
    max_daily_loss_pct = float(portfolio_state.get("max_daily_loss_pct", 999.0) or 999.0)

    symbol_blacklist = set(portfolio_state.get("symbol_blacklist", []) or [])
    side_open_counts = portfolio_state.get("side_open_counts", {}) or {}
    correlated_limit = int(portfolio_state.get("max_correlated_exposure", 999) or 999)

    correlated_long_exposure = int(portfolio_state.get("correlated_long_exposure", 0) or 0)
    correlated_short_exposure = int(portfolio_state.get("correlated_short_exposure", 0) or 0)

    if symbol in symbol_blacklist:
        allow = False
        confidence = min(confidence, 0.02)
        reasons.append("symbol blacklist içinde")
        flags.append("BLACKLISTED_SYMBOL")

    if open_positions_count >= max_open_positions:
        allow = False
        confidence = min(confidence, 0.03)
        reasons.append("maksimum açık pozisyon sınırı dolu")
        flags.append("MAX_OPEN_POSITIONS")

    if open_orders_count >= max_open_orders:
        allow = False
        confidence = min(confidence, 0.05)
        reasons.append("maksimum açık order sınırı dolu")
        flags.append("MAX_OPEN_ORDERS")

    if daily_realized_loss_pct >= max_daily_loss_pct:
        allow = False
        confidence = min(confidence, 0.01)
        reasons.append("günlük zarar limiti doldu")
        flags.append("DAILY_LOSS_LIMIT")

    if side == "LONG" and correlated_long_exposure >= correlated_limit:
        allow = False
        confidence = min(confidence, 0.08)
        reasons.append("korelasyonlu long yoğunluğu fazla")
        flags.append("CORRELATED_LONG_LIMIT")

    if side == "SHORT" and correlated_short_exposure >= correlated_limit:
        allow = False
        confidence = min(confidence, 0.08)
        reasons.append("korelasyonlu short yoğunluğu fazla")
        flags.append("CORRELATED_SHORT_LIMIT")

    summary = "portfolio policy passed" if allow else "portfolio policy rejected trade"

    return {
        "allow": allow,
        "confidence": round(confidence, 4),
        "summary": summary,
        "reasons": reasons,
        "flags": flags,
    }