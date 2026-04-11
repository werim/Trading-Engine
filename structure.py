#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from __future__ import annotations

from typing import Any, Dict, List, Optional, Tuple

from config import CONFIG
from utils import (
    atr,
    classify_trend,
    get_klines,
    safe_get_live_price,
    to_float,
)


# =========================================================
# INTERNAL HELPERS
# =========================================================

def _closes(klines: List[Dict[str, Any]]) -> List[float]:
    return [to_float(k["close"]) for k in klines]


def _highs(klines: List[Dict[str, Any]]) -> List[float]:
    return [to_float(k["high"]) for k in klines]


def _lows(klines: List[Dict[str, Any]]) -> List[float]:
    return [to_float(k["low"]) for k in klines]


def _last(klines: List[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
    if not klines:
        return None
    return klines[-1]


def _prev(klines: List[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
    if len(klines) < 2:
        return None
    return klines[-2]


def _swing_high(klines: List[Dict[str, Any]], lookback: int = 20) -> float:
    highs = _highs(klines[-lookback:]) if klines else []
    return max(highs) if highs else 0.0


def _swing_low(klines: List[Dict[str, Any]], lookback: int = 20) -> float:
    lows = _lows(klines[-lookback:]) if klines else []
    return min(lows) if lows else 0.0


def _last_close(klines: List[Dict[str, Any]]) -> float:
    last = _last(klines)
    return to_float(last["close"]) if last else 0.0


def _candle_range(candle: Dict[str, Any]) -> float:
    return max(to_float(candle["high"]) - to_float(candle["low"]), 0.0)


def _normalize_zone(a: float, b: float) -> Tuple[float, float]:
    return (min(a, b), max(a, b))


def _calc_rr(side: str, entry_trigger: float, sl: float, tp: float) -> float:
    side = str(side).upper()

    if side == "LONG":
        risk = entry_trigger - sl
        reward = tp - entry_trigger
    else:
        risk = sl - entry_trigger
        reward = entry_trigger - tp

    if risk <= 0:
        return 0.0
    return reward / risk


def _candidate_valid(side: str, zone_low: float, zone_high: float, trigger: float, sl: float, tp: float) -> bool:
    if zone_low <= 0 or zone_high <= 0 or trigger <= 0 or sl <= 0 or tp <= 0:
        return False

    if zone_low >= zone_high:
        return False

    side = str(side).upper()

    if side == "LONG":
        return sl < trigger < tp
    if side == "SHORT":
        return tp < trigger < sl

    return False


def _score_candidate(
    side: str,
    trend_1h: str,
    trend_4h: str,
    trend_1d: str,
    rr: float,
    setup_type: str,
) -> int:
    score = 0

    if side == trend_1h:
        score += 1
    if side == trend_4h:
        score += 1
    if side == trend_1d:
        score += 1

    if rr >= getattr(CONFIG.TRADE, "RR_DEFAULT", 2.0):
        score += 1

    if setup_type == "BREAKOUT":
        score += 1
    elif setup_type == "MOMENTUM_CONTINUATION":
        score += 1

    return score


def _candidate_dict(
    *,
    symbol: str,
    side: str,
    zone_low: float,
    zone_high: float,
    trigger: float,
    sl: float,
    tp: float,
    rr: float,
    score: int,
    tf_context: str,
    setup_type: str,
    setup_reason: str,
    live_price: float,
) -> Dict[str, Any]:
    return {
        "symbol": symbol,
        "side": side,
        "entry_zone_low": zone_low,
        "entry_zone_high": zone_high,
        "entry_trigger": trigger,
        "sl": sl,
        "tp": tp,
        "rr": rr,
        "score": score,
        "tf_context": tf_context,
        "setup_type": setup_type,
        "setup_reason": setup_reason,
        "live_price": live_price,
    }


# =========================================================
# PULLBACK ZONES
# =========================================================

def _zone_from_pullback_long(
    klines: List[Dict[str, Any]],
    atr_value: float,
) -> Optional[Tuple[float, float, float, float, float]]:
    if len(klines) < 20:
        return None

    last = _last(klines)
    if not last:
        return None

    close = to_float(last["close"])
    recent_high = _swing_high(klines[:-1], lookback=12)
    recent_low = _swing_low(klines[:-1], lookback=12)

    zone_low, zone_high = _normalize_zone(
        close - atr_value * 0.90,
        close - atr_value * 0.30,
    )
    trigger = recent_high + atr_value * 0.05
    sl = min(zone_low - atr_value * 0.25, recent_low - atr_value * 0.08)
    tp = trigger + (trigger - sl) * getattr(CONFIG.TRADE, "RR_DEFAULT", 2.0)

    return zone_low, zone_high, trigger, sl, tp


def _zone_from_pullback_short(
    klines: List[Dict[str, Any]],
    atr_value: float,
) -> Optional[Tuple[float, float, float, float, float]]:
    if len(klines) < 20:
        return None

    last = _last(klines)
    if not last:
        return None

    close = to_float(last["close"])
    recent_high = _swing_high(klines[:-1], lookback=12)
    recent_low = _swing_low(klines[:-1], lookback=12)

    zone_low, zone_high = _normalize_zone(
        close + atr_value * 0.30,
        close + atr_value * 0.90,
    )
    trigger = recent_low - atr_value * 0.05
    sl = max(zone_high + atr_value * 0.25, recent_high + atr_value * 0.08)
    tp = trigger - (sl - trigger) * getattr(CONFIG.TRADE, "RR_DEFAULT", 2.0)

    return zone_low, zone_high, trigger, sl, tp


# =========================================================
# BREAKOUT ZONES
# =========================================================

def _zone_from_breakout_long(
    klines: List[Dict[str, Any]],
    atr_value: float,
) -> Optional[Tuple[float, float, float, float, float]]:
    if len(klines) < 25:
        return None

    last = _last(klines)
    prev = _prev(klines)
    if not last or not prev:
        return None

    last_close = to_float(last["close"])
    prev_high = max(_highs(klines[-15:-1])) if len(klines) >= 16 else to_float(prev["high"])

    # breakout onayı
    if last_close <= prev_high:
        return None

    trigger = last_close + atr_value * 0.02
    zone_low, zone_high = _normalize_zone(
        last_close - atr_value * 0.20,
        last_close + atr_value * 0.10,
    )
    sl = min(to_float(last["low"]) - atr_value * 0.20, trigger - atr_value * 1.10)
    tp = trigger + (trigger - sl) * getattr(CONFIG.TRADE, "RR_DEFAULT", 2.0)

    return zone_low, zone_high, trigger, sl, tp


def _zone_from_breakout_short(
    klines: List[Dict[str, Any]],
    atr_value: float,
) -> Optional[Tuple[float, float, float, float, float]]:
    if len(klines) < 25:
        return None

    last = _last(klines)
    prev = _prev(klines)
    if not last or not prev:
        return None

    last_close = to_float(last["close"])
    prev_low = min(_lows(klines[-15:-1])) if len(klines) >= 16 else to_float(prev["low"])

    # breakout onayı
    if last_close >= prev_low:
        return None

    trigger = last_close - atr_value * 0.02
    zone_low, zone_high = _normalize_zone(
        last_close - atr_value * 0.10,
        last_close + atr_value * 0.20,
    )
    sl = max(to_float(last["high"]) + atr_value * 0.20, trigger + atr_value * 1.10)
    tp = trigger - (sl - trigger) * getattr(CONFIG.TRADE, "RR_DEFAULT", 2.0)

    return zone_low, zone_high, trigger, sl, tp


# =========================================================
# MOMENTUM CONTINUATION ZONES
# =========================================================

def _zone_from_momentum_long(
    klines: List[Dict[str, Any]],
    atr_value: float,
) -> Optional[Tuple[float, float, float, float, float]]:
    if len(klines) < 20:
        return None

    last = _last(klines)
    prev = _prev(klines)
    if not last or not prev:
        return None

    close = to_float(last["close"])
    open_ = to_float(last["open"])
    prev_close = to_float(prev["close"])

    # güçlü yeşil ve ivmeli kapanış
    if not (close > open_ and close > prev_close):
        return None

    body = abs(close - open_)
    if body < atr_value * 0.35:
        return None

    trigger = close + atr_value * 0.01
    zone_low, zone_high = _normalize_zone(
        close - atr_value * 0.25,
        close - atr_value * 0.05,
    )
    sl = zone_low - atr_value * 0.35
    tp = trigger + (trigger - sl) * getattr(CONFIG.TRADE, "RR_DEFAULT", 2.0)

    return zone_low, zone_high, trigger, sl, tp


def _zone_from_momentum_short(
    klines: List[Dict[str, Any]],
    atr_value: float,
) -> Optional[Tuple[float, float, float, float, float]]:
    if len(klines) < 20:
        return None

    last = _last(klines)
    prev = _prev(klines)
    if not last or not prev:
        return None

    close = to_float(last["close"])
    open_ = to_float(last["open"])
    prev_close = to_float(prev["close"])

    # güçlü kırmızı ve ivmeli kapanış
    if not (close < open_ and close < prev_close):
        return None

    body = abs(close - open_)
    if body < atr_value * 0.35:
        return None

    trigger = close - atr_value * 0.01
    zone_low, zone_high = _normalize_zone(
        close + atr_value * 0.05,
        close + atr_value * 0.25,
    )
    sl = zone_high + atr_value * 0.35
    tp = trigger - (sl - trigger) * getattr(CONFIG.TRADE, "RR_DEFAULT", 2.0)

    return zone_low, zone_high, trigger, sl, tp


# =========================================================
# CANDIDATE BUILDERS
# =========================================================

def _append_candidate(
    candidates: List[Dict[str, Any]],
    *,
    symbol: str,
    side: str,
    trend_1h: str,
    trend_4h: str,
    trend_1d: str,
    tf_context: str,
    setup_type: str,
    setup_reason: str,
    zone: Optional[Tuple[float, float, float, float, float]],
    live_price: float,
) -> None:
    if not zone:
        return

    zone_low, zone_high, trigger, sl, tp = zone

    if not _candidate_valid(side, zone_low, zone_high, trigger, sl, tp):
        return

    rr = _calc_rr(side, trigger, sl, tp)
    score = _score_candidate(
        side=side,
        trend_1h=trend_1h,
        trend_4h=trend_4h,
        trend_1d=trend_1d,
        rr=rr,
        setup_type=setup_type,
    )

    candidates.append(
        _candidate_dict(
            symbol=symbol,
            side=side,
            zone_low=zone_low,
            zone_high=zone_high,
            trigger=trigger,
            sl=sl,
            tp=tp,
            rr=rr,
            score=score,
            tf_context=tf_context,
            setup_type=setup_type,
            setup_reason=setup_reason,
            live_price=live_price,
        )
    )


# =========================================================
# MAIN EVALUATOR
# =========================================================

def evaluate_symbol(symbol: str) -> Optional[Dict[str, Any]]:
    limit = max(200, getattr(CONFIG.TRADE, "KLINES_LIMIT", 200))

    kl_1h = get_klines(symbol, "1h", limit)
    kl_4h = get_klines(symbol, "4h", limit)
    kl_1d = get_klines(symbol, "1d", limit)

    if not kl_1h or not kl_4h or not kl_1d:
        return None

    closes_1h = _closes(kl_1h)
    closes_4h = _closes(kl_4h)
    closes_1d = _closes(kl_1d)

    trend_1h = classify_trend(closes_1h)
    trend_4h = classify_trend(closes_4h)
    trend_1d = classify_trend(closes_1d)

    tf_context = f"1H={trend_1h}|4H={trend_4h}|1D={trend_1d}"

    live_price = safe_get_live_price(symbol)
    if live_price is None:
        return None

    atr_period = getattr(CONFIG.TRADE, "ATR_PERIOD", 14)
    atr_1h = atr(kl_1h, atr_period)
    if atr_1h <= 0:
        return None

    candidates: List[Dict[str, Any]] = []

    # ---------------------------------------------------------
    # LONG side setups
    # ---------------------------------------------------------
    if trend_1h == "LONG" and trend_4h in ("LONG", "RANGE") and trend_1d in ("LONG", "RANGE"):
        _append_candidate(
            candidates,
            symbol=symbol,
            side="LONG",
            trend_1h=trend_1h,
            trend_4h=trend_4h,
            trend_1d=trend_1d,
            tf_context=tf_context,
            setup_type="PULLBACK",
            setup_reason="PULLBACK_LONG",
            zone=_zone_from_pullback_long(kl_1h, atr_1h),
            live_price=live_price,
        )

        _append_candidate(
            candidates,
            symbol=symbol,
            side="LONG",
            trend_1h=trend_1h,
            trend_4h=trend_4h,
            trend_1d=trend_1d,
            tf_context=tf_context,
            setup_type="BREAKOUT",
            setup_reason="BREAKOUT_LONG",
            zone=_zone_from_breakout_long(kl_1h, atr_1h),
            live_price=live_price,
        )

        _append_candidate(
            candidates,
            symbol=symbol,
            side="LONG",
            trend_1h=trend_1h,
            trend_4h=trend_4h,
            trend_1d=trend_1d,
            tf_context=tf_context,
            setup_type="MOMENTUM_CONTINUATION",
            setup_reason="MOMENTUM_LONG",
            zone=_zone_from_momentum_long(kl_1h, atr_1h),
            live_price=live_price,
        )

    # ---------------------------------------------------------
    # SHORT side setups
    # ---------------------------------------------------------
    if trend_1h == "SHORT" and trend_4h in ("SHORT", "RANGE") and trend_1d in ("SHORT", "RANGE"):
        _append_candidate(
            candidates,
            symbol=symbol,
            side="SHORT",
            trend_1h=trend_1h,
            trend_4h=trend_4h,
            trend_1d=trend_1d,
            tf_context=tf_context,
            setup_type="PULLBACK",
            setup_reason="PULLBACK_SHORT",
            zone=_zone_from_pullback_short(kl_1h, atr_1h),
            live_price=live_price,
        )

        _append_candidate(
            candidates,
            symbol=symbol,
            side="SHORT",
            trend_1h=trend_1h,
            trend_4h=trend_4h,
            trend_1d=trend_1d,
            tf_context=tf_context,
            setup_type="BREAKOUT",
            setup_reason="BREAKOUT_SHORT",
            zone=_zone_from_breakout_short(kl_1h, atr_1h),
            live_price=live_price,
        )

        _append_candidate(
            candidates,
            symbol=symbol,
            side="SHORT",
            trend_1h=trend_1h,
            trend_4h=trend_4h,
            trend_1d=trend_1d,
            tf_context=tf_context,
            setup_type="MOMENTUM_CONTINUATION",
            setup_reason="MOMENTUM_SHORT",
            zone=_zone_from_momentum_short(kl_1h, atr_1h),
            live_price=live_price,
        )

    if not candidates:
        return None

    candidates.sort(
        key=lambda x: (
            int(x["score"]),
            float(x["rr"]),
            1 if x["setup_type"] == "BREAKOUT" else 0,
            1 if x["setup_type"] == "MOMENTUM_CONTINUATION" else 0,
        ),
        reverse=True,
    )

    best = candidates[0]

    if int(best["score"]) < getattr(CONFIG.TRADE, "MIN_SCORE", 2):
        return None

    if float(best["rr"]) < getattr(CONFIG.TRADE, "RR_MIN", 1.6):
        return None

    return best