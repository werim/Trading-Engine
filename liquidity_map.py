
from __future__ import annotations

from typing import Any, Dict, List


def _safe_float(value: Any, default: float = 0.0) -> float:
    try:
        if value is None:
            return default
        return float(value)
    except (TypeError, ValueError):
        return default


def _unique_sorted(values: List[float]) -> List[float]:
    cleaned = []
    for value in values:
        if value > 0:
            cleaned.append(round(value, 8))
    return sorted(set(cleaned))


def build_liquidity_map(symbol: str, market_ctx: Dict[str, Any]) -> Dict[str, Any]:
    tf = market_ctx.get("tf", {}) or {}
    one_h = tf.get("1H", {}) or {}
    four_h = tf.get("4H", {}) or {}
    one_d = tf.get("1D", {}) or {}

    last_price = _safe_float(market_ctx.get("last_price"))
    atr = _safe_float(one_h.get("atr"))
    ema20 = _safe_float(one_h.get("ema20"))
    ema50 = _safe_float(one_h.get("ema50"))
    ema200 = _safe_float(one_h.get("ema200"))
    high_1h = _safe_float(one_h.get("recent_high"))
    low_1h = _safe_float(one_h.get("recent_low"))
    high_4h = _safe_float(four_h.get("recent_high"))
    low_4h = _safe_float(four_h.get("recent_low"))
    high_1d = _safe_float(one_d.get("recent_high"))
    low_1d = _safe_float(one_d.get("recent_low"))

    above = _unique_sorted([
        high_1h,
        high_4h,
        high_1d,
        ema20 if ema20 > last_price else 0.0,
        ema50 if ema50 > last_price else 0.0,
        last_price + atr * 0.75,
        last_price + atr * 1.50,
        last_price + atr * 2.25,
    ])

    below = _unique_sorted([
        low_1h,
        low_4h,
        low_1d,
        ema20 if 0 < ema20 < last_price else 0.0,
        ema50 if 0 < ema50 < last_price else 0.0,
        ema200 if 0 < ema200 < last_price else 0.0,
        last_price - atr * 0.75,
        last_price - atr * 1.50,
        last_price - atr * 2.25,
    ])

    trap_long_zone = []
    trap_short_zone = []

    if low_1h > 0 and atr > 0:
        trap_long_zone = [round(low_1h - atr * 0.20, 8), round(low_1h + atr * 0.20, 8)]
    elif atr > 0:
        trap_long_zone = [round(last_price - atr * 1.25, 8), round(last_price - atr * 0.75, 8)]

    if high_1h > 0 and atr > 0:
        trap_short_zone = [round(high_1h - atr * 0.20, 8), round(high_1h + atr * 0.20, 8)]
    elif atr > 0:
        trap_short_zone = [round(last_price + atr * 0.75, 8), round(last_price + atr * 1.25, 8)]

    reclaim_levels = _unique_sorted([ema20, ema50, high_1h, low_1h])

    return {
        "symbol": symbol,
        "price": last_price,
        "atr_1h": atr,
        "resting_liquidity_above": above,
        "resting_liquidity_below": below,
        "reclaim_levels": reclaim_levels,
        "trap_long_zone": trap_long_zone,
        "trap_short_zone": trap_short_zone,
    }
