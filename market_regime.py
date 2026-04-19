
from __future__ import annotations

from typing import Any, Dict, List


def _safe_float(value: Any, default: float = 0.0) -> float:
    try:
        if value is None:
            return default
        return float(value)
    except (TypeError, ValueError):
        return default


def _ema_slope_pct(value_now: float, value_prev: float) -> float:
    if value_prev == 0:
        return 0.0
    return (value_now - value_prev) / abs(value_prev) * 100.0


def _classify_single_tf(tf_ctx: Dict[str, Any]) -> Dict[str, Any]:
    last_price = _safe_float(tf_ctx.get("last_price"))
    ema20 = _safe_float(tf_ctx.get("ema20"))
    ema50 = _safe_float(tf_ctx.get("ema50"))
    ema200 = _safe_float(tf_ctx.get("ema200"))
    atr = _safe_float(tf_ctx.get("atr"))
    volume_ratio = _safe_float(tf_ctx.get("volume_ratio"), 1.0)
    momentum = _safe_float(tf_ctx.get("momentum"))
    ema20_prev = _safe_float(tf_ctx.get("ema20_prev"), ema20)
    ema50_prev = _safe_float(tf_ctx.get("ema50_prev"), ema50)

    trend_up = last_price > ema20 > ema50 > ema200 > 0
    trend_down = 0 < last_price < ema20 < ema50 < ema200
    atr_pct = (atr / last_price * 100.0) if last_price > 0 else 0.0
    ema20_slope = _ema_slope_pct(ema20, ema20_prev)
    ema50_slope = _ema_slope_pct(ema50, ema50_prev)

    regime = "RANGE"
    strength = 0.0

    if trend_up:
        regime = "LONG"
        strength += 3.0
    elif trend_down:
        regime = "SHORT"
        strength += 3.0

    if ema20_slope > 0 and ema50_slope > 0:
        strength += 1.0
    elif ema20_slope < 0 and ema50_slope < 0:
        strength += 1.0

    if atr_pct > 2.0:
        strength += 0.5
    if volume_ratio > 1.3:
        strength += 0.5
    if abs(momentum) > 0.75:
        strength += 0.5

    if atr_pct >= 4.0 and volume_ratio >= 1.8:
        if regime == "LONG":
            regime = "BLOWOFF_TOP"
        elif regime == "SHORT":
            regime = "PANIC_SELL"

    return {
        "regime": regime,
        "strength": round(strength, 4),
        "atr_pct": round(atr_pct, 4),
        "ema20_slope_pct": round(ema20_slope, 4),
        "ema50_slope_pct": round(ema50_slope, 4),
        "volume_ratio": round(volume_ratio, 4),
        "momentum": round(momentum, 4),
    }


def detect_market_regime(symbol: str, market_ctx: Dict[str, Any]) -> Dict[str, Any]:
    tf = market_ctx.get("tf", {}) or {}

    one_h = _classify_single_tf(tf.get("1H", {}) or {})
    four_h = _classify_single_tf(tf.get("4H", {}) or {})
    one_d = _classify_single_tf(tf.get("1D", {}) or {})

    regime_votes: List[str] = [one_h["regime"], four_h["regime"], one_d["regime"]]
    last_price = _safe_float(market_ctx.get("last_price"))

    bull_votes = sum(1 for r in regime_votes if r in {"LONG", "BLOWOFF_TOP"})
    bear_votes = sum(1 for r in regime_votes if r in {"SHORT", "PANIC_SELL"})

    overall = "RANGE"
    if bull_votes >= 2:
        overall = "TREND_LONG"
    elif bear_votes >= 2:
        overall = "TREND_SHORT"

    avg_strength = round((one_h["strength"] + four_h["strength"] + one_d["strength"]) / 3.0, 4)

    return {
        "symbol": symbol,
        "price": last_price,
        "overall_regime": overall,
        "confidence": avg_strength,
        "tf": {
            "1H": one_h,
            "4H": four_h,
            "1D": one_d,
        },
        "bias": "LONG" if overall == "TREND_LONG" else "SHORT" if overall == "TREND_SHORT" else "NEUTRAL",
    }
