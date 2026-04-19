# symbol_context.py
from __future__ import annotations

from typing import Any, Dict, List, Optional


def _safe_float(x: Any, default: float = 0.0) -> float:
    try:
        if x is None:
            return default
        return float(x)
    except Exception:
        return default


def _classify_alignment(side: str, t1: str, t4: str, td: str) -> str:
    aligned = sum(1 for t in (t1, t4, td) if t == side)
    if aligned == 3:
        return "FULL"
    if aligned == 2:
        return "PARTIAL"
    if aligned == 1:
        return "WEAK"
    return "NONE"


def _classify_entry_quality(side: str, entry: float, last: float, zone_low: float, zone_high: float) -> str:
    if entry <= 0 or last <= 0:
        return "UNKNOWN"

    zone_mid = (zone_low + zone_high) / 2.0 if zone_low > 0 and zone_high > 0 else entry

    if side == "LONG":
        if last < zone_low:
            return "EARLY"
        if zone_low <= last <= zone_high:
            return "OPTIMAL"
        if zone_high < last <= entry * 1.003:
            return "LATE"
        return "VERY_LATE"

    if side == "SHORT":
        if last > zone_high:
            return "EARLY"
        if zone_low <= last <= zone_high:
            return "OPTIMAL"
        if entry * 0.997 <= last < zone_low:
            return "LATE"
        return "VERY_LATE"

    return "UNKNOWN"


def _classify_distance_from_trigger(side: str, entry: float, last: float) -> float:
    if entry <= 0 or last <= 0:
        return 999.0
    if side == "LONG":
        return (last - entry) / entry * 100.0
    if side == "SHORT":
        return (entry - last) / entry * 100.0
    return 999.0


def _classify_spread_quality(spread_pct: float) -> str:
    if spread_pct <= 0.04:
        return "GOOD"
    if spread_pct <= 0.10:
        return "OK"
    if spread_pct <= 0.20:
        return "WEAK"
    return "BAD"


def _classify_volume_quality(volume_24h_usdt: float) -> str:
    if volume_24h_usdt >= 5_000_000_000:
        return "A+"
    if volume_24h_usdt >= 1_000_000_000:
        return "A"
    if volume_24h_usdt >= 250_000_000:
        return "B"
    if volume_24h_usdt >= 50_000_000:
        return "C"
    return "D"


def build_symbol_context(
    symbol: str,
    side: str,
    market_ctx: Dict[str, Any],
    tf: Dict[str, Dict[str, Any]],
    candidate: Optional[Dict[str, Any]],
    recent_symbol_stats: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """
    Beklenen candidate alanları:
      entry_zone_low, entry_zone_high, entry_trigger, sl, tp, rr, score, setup_type, setup_reason
    Beklenen tf alanları:
      tf["1H"]["regime"], tf["4H"]["regime"], tf["1D"]["regime"], ema20, ema50
    Beklenen market_ctx alanları:
      last_price, spread_pct, volume_24h_usdt, funding_rate_pct
    """

    recent_symbol_stats = recent_symbol_stats or {}

    last = _safe_float(market_ctx.get("last_price") or market_ctx.get("price"))
    spread_pct = _safe_float(market_ctx.get("spread_pct"))
    volume_24h_usdt = _safe_float(market_ctx.get("volume_24h_usdt"))
    funding_rate_pct = _safe_float(market_ctx.get("funding_rate_pct"))

    t1 = str(tf.get("1H", {}).get("regime", "RANGE"))
    t4 = str(tf.get("4H", {}).get("regime", "RANGE"))
    td = str(tf.get("1D", {}).get("regime", "RANGE"))

    c = candidate or {}
    entry = _safe_float(c.get("entry_trigger"))
    zone_low = _safe_float(c.get("entry_zone_low"))
    zone_high = _safe_float(c.get("entry_zone_high"))
    sl = _safe_float(c.get("sl"))
    tp = _safe_float(c.get("tp"))
    rr = _safe_float(c.get("rr"))
    score = int(c.get("score", 0) or 0)

    alignment = _classify_alignment(side=side, t1=t1, t4=t4, td=td)
    entry_quality = _classify_entry_quality(side, entry, last, zone_low, zone_high)
    trigger_distance_pct = _classify_distance_from_trigger(side, entry, last)
    spread_quality = _classify_spread_quality(spread_pct)
    volume_quality = _classify_volume_quality(volume_24h_usdt)

    same_symbol_open_orders = int(recent_symbol_stats.get("same_symbol_open_orders", 0) or 0)
    same_symbol_open_positions = int(recent_symbol_stats.get("same_symbol_open_positions", 0) or 0)
    recent_attempts = int(recent_symbol_stats.get("recent_attempts", 0) or 0)
    recent_cancellations = int(recent_symbol_stats.get("recent_cancellations", 0) or 0)
    cooldown_active = bool(recent_symbol_stats.get("cooldown_active", False))

    expected_net_pnl_pct = 0.0
    stop_net_loss_pct = 0.0
    if entry > 0 and tp > 0:
        if side == "LONG":
            expected_net_pnl_pct = ((tp - entry) / entry) * 100.0
        else:
            expected_net_pnl_pct = ((entry - tp) / entry) * 100.0
    if entry > 0 and sl > 0:
        if side == "LONG":
            stop_net_loss_pct = ((entry - sl) / entry) * 100.0
        else:
            stop_net_loss_pct = ((sl - entry) / entry) * 100.0

    return {
        "symbol": symbol,
        "side": side,
        "last_price": last,
        "spread_pct": spread_pct,
        "spread_quality": spread_quality,
        "volume_24h_usdt": volume_24h_usdt,
        "volume_quality": volume_quality,
        "funding_rate_pct": funding_rate_pct,
        "trend_1h": t1,
        "trend_4h": t4,
        "trend_1d": td,
        "trend_alignment": alignment,
        "entry_zone_low": zone_low,
        "entry_zone_high": zone_high,
        "entry_trigger": entry,
        "sl": sl,
        "tp": tp,
        "rr": rr,
        "score": score,
        "setup_type": c.get("setup_type", ""),
        "setup_reason": c.get("setup_reason", ""),
        "entry_quality": entry_quality,
        "trigger_distance_pct": trigger_distance_pct,
        "expected_net_pnl_pct": expected_net_pnl_pct,
        "stop_net_loss_pct": stop_net_loss_pct,
        "same_symbol_open_orders": same_symbol_open_orders,
        "same_symbol_open_positions": same_symbol_open_positions,
        "recent_attempts": recent_attempts,
        "recent_cancellations": recent_cancellations,
        "cooldown_active": cooldown_active,
    }