from __future__ import annotations
from event_miner import build_event_key, get_event_stats
from market import _get_market_context_from_local_cache
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Tuple
import csv
import os

import adaptive
import binance
import market
import optimizer
import risk
import storage
import liquidity_map_sniper as liquidity_map
import market_regime
import order_planner_sniper as order_planner
import scenario_engine_sniper as scenario_engine
from config import CONFIG
from logger import get_logger
from notifier import notify_order_created, notify_real_order_submitted
from utils import (
    calc_rr,
    is_order_expired,
    is_same_order_intent,
    make_client_order_id,
    make_order_id,
    normalize_status,
    safe_float,
    utc_now_str,
)

ACTIVE_ORDER_STATUSES = {
    "PLANNED",
    "WATCHING",
    "READY",
    "NEW",
    "PARTIALLY_FILLED",
}

FINAL_ORDER_STATUSES = {
    "FILLED",
    "CANCELLED",
    "EXPIRED",
    "REJECTED",
    "FAILED",
}

EXCHANGE_TO_LOCAL_STATUS = {
    "NEW": "NEW",
    "PARTIALLY_FILLED": "PARTIALLY_FILLED",
    "FILLED": "FILLED",
    "CANCELED": "CANCELLED",
    "EXPIRED": "EXPIRED",
    "REJECTED": "REJECTED",
}

BAD_EVENT_TYPES = {
    "volume_spike",
    "volatility_burst",
}

A_PLUS_EVENT_KEYS = {
    ("price_spike_up", "normal", "sharp_up"),
    ("combined_shock", "high", "mild"),
    ("combined_shock", "high", "sharp_up"),
}

SUPPORTIVE_EVENT_KEYS = {
    ("combined_shock", "extreme", "mild"),
    ("combined_shock", "extreme", "sharp_up"),
    ("price_spike_up", "normal", "mild"),
    ("price_spike_down", "normal", "sharp_down"),
}

MIN_EVENT_SAMPLE_SIZE = 10
MIN_EVENT_WINRATE = 0.60
A_PLUS_TP_MULT = 1.18
SUPPORTIVE_TP_MULT = 1.08
EVENT_SCORE_BONUS_A_PLUS = 4
EVENT_SCORE_BONUS_SUPPORTIVE = 2
EVENT_SCORE_PENALTY_BAD = -4

log = get_logger("order", "logs/order.log")


# ============================================================================
# basic helpers
# ============================================================================

def now_utc() -> str:
    return utc_now_str()


def get_order_status(order: Dict[str, Any]) -> str:
    return normalize_status(order.get("status"))


def is_final_order_status(status: str) -> bool:
    return normalize_status(status) in FINAL_ORDER_STATUSES


def is_active_order_status(status: str) -> bool:
    return normalize_status(status) in ACTIVE_ORDER_STATUSES


def stamp_updated(order: Dict[str, Any]) -> Dict[str, Any]:
    order["updated_at"] = now_utc()
    return order


def build_tf_context(market_ctx: Dict[str, Any]) -> str:
    tf = market_ctx["tf"]
    return f"1H={tf['1H']['regime']}|4H={tf['4H']['regime']}|1D={tf['1D']['regime']}"


def _normalize_event_type(value: Any) -> str:
    return str(value or "").strip().lower()


def _normalize_bucket(value: Any) -> str:
    return str(value or "").strip().lower()


def _market_event_key(event: Dict[str, Any]) -> Tuple[str, str, str]:
    return (
        _normalize_event_type(event.get("event_type")),
        _normalize_bucket(event.get("volume_bucket")),
        _normalize_bucket(event.get("return_bucket")),
    )


def _extract_market_event(market_ctx: Dict[str, Any]) -> Dict[str, Any]:
    raw = market_ctx.get("market_event") or {}
    if not isinstance(raw, dict):
        raw = {}

    return {
        "event_type": _normalize_event_type(raw.get("event_type")),
        "direction": normalize_status(raw.get("direction")),
        "volume_bucket": _normalize_bucket(raw.get("volume_bucket")),
        "return_bucket": _normalize_bucket(raw.get("return_bucket")),
        "winrate": safe_float(raw.get("winrate", 0.0)),
        "sample_size": int(safe_float(raw.get("sample_size", 0))),
        "recent": int(safe_float(raw.get("recent", 0))),
    }


def _candidate_side_matches_event(candidate: Dict[str, Any], event: Dict[str, Any]) -> bool:
    side = normalize_status(candidate.get("side"))
    direction = normalize_status(event.get("direction"))

    if not direction or direction == "NEUTRAL":
        return True
    if side == "LONG" and direction == "UP":
        return True
    if side == "SHORT" and direction == "DOWN":
        return True
    return False


def _apply_tp_multiplier(candidate: Dict[str, Any], tp_mult: float) -> Dict[str, Any]:
    entry = safe_float(candidate.get("entry_trigger"))
    tp = safe_float(candidate.get("tp"))
    side = normalize_status(candidate.get("side"))

    if entry <= 0 or tp <= 0 or tp_mult <= 0:
        return candidate

    if side == "LONG":
        dist = max(tp - entry, 0.0)
        candidate["tp"] = entry + dist * tp_mult
    elif side == "SHORT":
        dist = max(entry - tp, 0.0)
        candidate["tp"] = entry - dist * tp_mult

    candidate["rr"] = calc_rr(
        safe_float(candidate["entry_trigger"]),
        safe_float(candidate["sl"]),
        safe_float(candidate["tp"]),
        side,
    )
    return candidate


def _apply_market_event_intelligence(candidate: Dict[str, Any], market_ctx: Dict[str, Any]) -> Dict[str, Any]:
    event = _extract_market_event(market_ctx)
    event_key = _market_event_key(event)

    candidate["market_event_type"] = event.get("event_type", "")
    candidate["market_event_direction"] = event.get("direction", "")
    candidate["market_event_volume_bucket"] = event.get("volume_bucket", "")
    candidate["market_event_return_bucket"] = event.get("return_bucket", "")
    candidate["market_event_winrate"] = event.get("winrate", 0.0)
    candidate["market_event_sample_size"] = event.get("sample_size", 0)
    candidate["market_event_recent"] = event.get("recent", 0)
    candidate["market_event_key"] = "|".join(event_key)
    candidate["market_event_blocked"] = 0
    candidate["market_event_reason"] = ""

    if not event.get("event_type"):
        candidate["market_event_reason"] = "NO_EVENT"
        return candidate

    if event.get("recent", 0) != 1:
        candidate["market_event_reason"] = "STALE_EVENT"
        return candidate

    if not _candidate_side_matches_event(candidate, event):
        candidate["market_event_blocked"] = 1
        candidate["market_event_reason"] = "EVENT_DIRECTION_MISMATCH"
        return candidate

    if event["event_type"] in BAD_EVENT_TYPES:
        candidate["market_event_blocked"] = 1
        candidate["market_event_reason"] = "BAD_EVENT_TYPE"
        candidate["score"] = max(0, int(candidate.get("score", 0)) + EVENT_SCORE_PENALTY_BAD)
        return candidate

    if event["sample_size"] < MIN_EVENT_SAMPLE_SIZE:
        candidate["market_event_reason"] = "LOW_EVENT_SAMPLE"
        return candidate

    if event["winrate"] < MIN_EVENT_WINRATE:
        candidate["market_event_blocked"] = 1
        candidate["market_event_reason"] = "LOW_EVENT_WINRATE"
        return candidate

    if event_key in A_PLUS_EVENT_KEYS:
        candidate["score"] = int(candidate.get("score", 0)) + EVENT_SCORE_BONUS_A_PLUS
        candidate["market_event_reason"] = "A_PLUS_EVENT"
        candidate = _apply_tp_multiplier(candidate, A_PLUS_TP_MULT)
        return candidate

    if event_key in SUPPORTIVE_EVENT_KEYS:
        candidate["score"] = int(candidate.get("score", 0)) + EVENT_SCORE_BONUS_SUPPORTIVE
        candidate["market_event_reason"] = "SUPPORTIVE_EVENT"
        candidate = _apply_tp_multiplier(candidate, SUPPORTIVE_TP_MULT)
        return candidate

    candidate["market_event_reason"] = "UNSUPPORTED_EVENT"
    return candidate


# ============================================================================
# candidate building
# ============================================================================

def _build_fallback_order_candidate(symbol: str, market_ctx: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    tf = market_ctx["tf"]
    last_price = safe_float(market_ctx["last_price"])
    atr_1h = safe_float(tf["1H"]["atr"])

    regime_1h = tf["1H"]["regime"]
    regime_4h = tf["4H"]["regime"]

    if last_price <= 0 or atr_1h <= 0:
        return None

    side: str
    setup_type: str
    setup_reason: str
    entry_zone_low: float
    entry_zone_high: float
    entry_trigger: float
    sl: float
    tp: float

    if regime_1h == "LONG" and regime_4h == "LONG":
        side = "LONG"
        setup_type = "PULLBACK"
        setup_reason = "PULLBACK_LONG"

        entry_zone_high = last_price - atr_1h * 0.10
        entry_zone_low = last_price - atr_1h * 0.35
        entry_trigger = (entry_zone_low + entry_zone_high) / 2.0
        sl = entry_zone_low - atr_1h * CONFIG.STRATEGY.PULLBACK_SL_ATR_MULT
        tp = entry_trigger + (entry_trigger - sl) * CONFIG.STRATEGY.PULLBACK_RR_MULT

    elif regime_1h == "SHORT" and regime_4h == "SHORT":
        side = "SHORT"
        setup_type = "PULLBACK"
        setup_reason = "PULLBACK_SHORT"

        entry_zone_low = last_price + atr_1h * 0.10
        entry_zone_high = last_price + atr_1h * 0.35
        entry_trigger = (entry_zone_low + entry_zone_high) / 2.0
        sl = entry_zone_high + atr_1h * CONFIG.STRATEGY.PULLBACK_SL_ATR_MULT
        tp = entry_trigger - (sl - entry_trigger) * CONFIG.STRATEGY.PULLBACK_RR_MULT

    else:
        return None

    distance_pct = abs(last_price - entry_trigger) / last_price * 100.0
    if distance_pct > 3.0:
        return None

    rr = calc_rr(entry_trigger, sl, tp, side)
    now = now_utc()

    return {
        "order_id": make_order_id(),
        "client_order_id": "",
        "symbol": symbol,
        "side": side,
        "entry_zone_low": entry_zone_low,
        "entry_zone_high": entry_zone_high,
        "entry_trigger": entry_trigger,
        "sl": sl,
        "tp": tp,
        "rr": rr,
        "score": 0,
        "tf_context": build_tf_context(market_ctx),
        "setup_type": setup_type,
        "setup_reason": setup_reason,
        "scenario_name": "",
        "scenario_probability": 0.0,
        "created_at": now,
        "updated_at": now,
        "expires_at": "",
        "status": "WATCHING",
        "live_price": last_price,
        "exchange_order_id": "",
        "exchange_status": "",
        "order_type": "LIMIT" if CONFIG.TRADE.USE_LIMIT_ENTRY else "MARKET",
        "submitted_qty": 0.0,
        "executed_qty": 0.0,
        "avg_fill_price": 0.0,
        "zone_touched": 0,
        "alarm_touched_sent": 0,
        "alarm_near_trigger_sent": 0,
        "last_alarm_at": "",
        "expected_net_pnl_pct": 0.0,
        "stop_net_loss_pct": 0.0,
        "volume_24h_usdt": market_ctx["volume_24h_usdt"],
        "spread_pct": market_ctx["spread_pct"],
        "funding_rate_pct": market_ctx["funding_rate_pct"],
        "adaptive_score_delta": 0,
        "adaptive_expectancy": 0.0,
        "adaptive_sample_size": 0,
        "adaptive_reason": "",
        "adaptive_blocked": 0,
        "market_event_type": "",
        "market_event_direction": "",
        "market_event_volume_bucket": "",
        "market_event_return_bucket": "",
        "market_event_winrate": 0.0,
        "market_event_sample_size": 0,
        "market_event_recent": 0,
        "market_event_key": "",
        "market_event_blocked": 0,
        "market_event_reason": "",
    }


def _normalize_plans(raw_plans: Any) -> List[Dict[str, Any]]:
    if raw_plans is None:
        return []
    if isinstance(raw_plans, dict):
        return [raw_plans]
    if isinstance(raw_plans, list):
        return [p for p in raw_plans if isinstance(p, dict)]
    return []


def _plan_to_candidate(
    symbol: str,
    market_ctx: Dict[str, Any],
    plan: Dict[str, Any],
) -> Optional[Dict[str, Any]]:
    now = now_utc()
    last_price = safe_float(market_ctx["last_price"])

    side = normalize_status(plan.get("side"))
    if side not in {"LONG", "SHORT"}:
        return None

    entry = safe_float(plan.get("entry_trigger", plan.get("entry")))
    sl = safe_float(plan.get("sl"))
    tp = safe_float(plan.get("tp"))

    if entry <= 0 or sl <= 0 or tp <= 0:
        return None

    rr = safe_float(plan.get("rr"))
    if rr <= 0:
        rr = calc_rr(entry, sl, tp, side)

    entry_zone_low = safe_float(plan.get("entry_zone_low"))
    entry_zone_high = safe_float(plan.get("entry_zone_high"))

    if entry_zone_low <= 0 or entry_zone_high <= 0:
        atr_1h = safe_float(market_ctx.get("tf", {}).get("1H", {}).get("atr", 0.0))
        zone_pad = atr_1h * 0.08 if atr_1h > 0 else max(entry * 0.0015, 1e-8)

        if side == "LONG":
            entry_zone_low = entry - zone_pad
            entry_zone_high = entry + zone_pad * 0.25
        else:
            entry_zone_low = entry - zone_pad * 0.25
            entry_zone_high = entry + zone_pad

    entry_zone_low = safe_float(entry_zone_low)
    entry_zone_high = safe_float(entry_zone_high)

    if entry_zone_low <= 0 or entry_zone_high <= 0:
        return None

    if entry_zone_low > entry_zone_high:
        entry_zone_low, entry_zone_high = entry_zone_high, entry_zone_low

    setup_reason = str(plan.get("setup_reason") or plan.get("scenario_name") or f"{side}_SCENARIO")
    setup_reason_u = normalize_status(setup_reason)
    if "PULLBACK" in setup_reason_u:
        setup_type = f"PULLBACK_{side}"
    elif "RECLAIM" in setup_reason_u:
        setup_type = f"RECLAIM_{side}"
    elif "BREAKOUT" in setup_reason_u or "BREAKDOWN" in setup_reason_u:
        setup_type = f"BREAKOUT_{side}"
    else:
        setup_type = str(plan.get("setup_type") or f"RECLAIM_{side}")
    score_bonus = int(safe_float(plan.get("score_bonus", 0)))
    size_mult = safe_float(plan.get("size_mult", 1.0), 1.0)
    scenario_probability = safe_float(plan.get("scenario_probability", 0.0))

    preferred_order_type = str(plan.get("preferred_order_type") or "").upper()
    if preferred_order_type not in {"LIMIT", "MARKET"}:
        preferred_order_type = "LIMIT" if CONFIG.TRADE.USE_LIMIT_ENTRY else "MARKET"

    return {
        "order_id": make_order_id(),
        "client_order_id": "",
        "symbol": symbol,
        "side": side,
        "entry_zone_low": entry_zone_low,
        "entry_zone_high": entry_zone_high,
        "entry_trigger": entry,
        "sl": sl,
        "tp": tp,
        "rr": rr,
        "score": max(0, score_bonus),
        "tf_context": build_tf_context(market_ctx),
        "setup_type": setup_type,
        "setup_reason": setup_reason,
        "setup_family": str(plan.get("setup_family") or ""),
        "scenario_name": str(plan.get("scenario_name", "")),
        "scenario_probability": scenario_probability,
        "size_mult": size_mult,
        "created_at": now,
        "updated_at": now,
        "expires_at": "",
        "status": "WATCHING",
        "live_price": last_price,
        "exchange_order_id": "",
        "exchange_status": "",
        "order_type": preferred_order_type,
        "submitted_qty": 0.0,
        "executed_qty": 0.0,
        "avg_fill_price": 0.0,
        "zone_touched": 0,
        "alarm_touched_sent": 0,
        "alarm_near_trigger_sent": 0,
        "last_alarm_at": "",
        "expected_net_pnl_pct": 0.0,
        "stop_net_loss_pct": 0.0,
        "volume_24h_usdt": market_ctx["volume_24h_usdt"],
        "spread_pct": market_ctx["spread_pct"],
        "funding_rate_pct": market_ctx["funding_rate_pct"],
        "funding_rate_available": market_ctx.get("funding_rate_available", 1),
        "adaptive_score_delta": 0,
        "adaptive_expectancy": 0.0,
        "adaptive_sample_size": 0,
        "adaptive_reason": "",
        "adaptive_blocked": 0,
        "market_event_type": "",
        "market_event_direction": "",
        "market_event_volume_bucket": "",
        "market_event_return_bucket": "",
        "market_event_winrate": 0.0,
        "market_event_sample_size": 0,
        "market_event_recent": 0,
        "market_event_key": "",
        "market_event_blocked": 0,
        "market_event_reason": "",
    }


def build_order_candidate(symbol: str, market_ctx: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    try:
        regime = market_regime.detect_market_regime(symbol, market_ctx)
        liq = liquidity_map.build_liquidity_map(symbol, market_ctx)
        scenarios = scenario_engine.build_scenarios(symbol, market_ctx, regime, liq)
        raw_plans = order_planner.build_order_plan(symbol, market_ctx, regime, liq, scenarios)
        plans = _normalize_plans(raw_plans)

        if plans:
            plans = sorted(
                plans,
                key=lambda p: (
                    safe_float(p.get("scenario_probability", 0.0)),
                    safe_float(p.get("score_bonus", 0.0)),
                    safe_float(p.get("size_mult", 1.0)),
                ),
                reverse=True,
            )
            for plan in plans:
                candidate = _plan_to_candidate(symbol, market_ctx, plan)
                if candidate:
                    return candidate
    except Exception as exc:
        log.exception("SCENARIO_PLAN_BUILD_ERROR symbol=%s err=%s", symbol, type(exc).__name__)

    return _build_fallback_order_candidate(symbol, market_ctx)


def score_candidate(candidate: Dict[str, Any], market_ctx: Dict[str, Any]) -> int:
    tf = market_ctx["tf"]
    score = int(safe_float(candidate.get("score", 0)))

    if tf["1H"]["regime"] == tf["4H"]["regime"]:
        score += 3
    if tf["4H"]["regime"] == tf["1D"]["regime"]:
        score += 1
    if tf["1H"]["regime"] != tf["4H"]["regime"] and tf["1H"]["regime"] in {"LONG", "SHORT"} and tf["4H"]["regime"] in {"LONG", "SHORT"}:
        score += 1
    if safe_float(candidate["rr"]) >= CONFIG.FILTER.MIN_RR:
        score += 2
    if safe_float(market_ctx["volume_24h_usdt"]) >= CONFIG.FILTER.MIN_24H_VOLUME_USDT:
        score += 1
    if safe_float(market_ctx["spread_pct"]) <= CONFIG.FILTER.MAX_SPREAD_PCT:
        score += 1

    setup_reason = normalize_status(candidate.get("setup_reason"))
    if "BREAKOUT" in setup_reason or "TREND_CONTINUATION" in setup_reason:
        score += 1

    if "PULLBACK" in setup_reason and tf["1H"]["regime"] == "RANGE":
        score -= 1

    return max(0, score)


def estimate_expected_net_pnl_pct(candidate: Dict[str, Any]) -> float:
    entry = safe_float(candidate["entry_trigger"])
    tp = safe_float(candidate["tp"])
    spread_pct = safe_float(candidate["spread_pct"])

    if entry <= 0:
        return 0.0

    gross = abs(tp - entry) / entry * 100.0
    if normalize_status(candidate.get("order_type")) == "LIMIT":
        slippage_pct = CONFIG.TRADE.LIMIT_ENTRY_SLIPPAGE_PCT
    else:
        slippage_pct = CONFIG.TRADE.MARKET_ENTRY_SLIPPAGE_PCT

    rough_fees_slippage = spread_pct + 0.08 + slippage_pct
    return gross - rough_fees_slippage


def estimate_stop_net_loss_pct(candidate: Dict[str, Any]) -> float:
    entry = safe_float(candidate["entry_trigger"])
    sl = safe_float(candidate["sl"])
    spread_pct = safe_float(candidate["spread_pct"])

    if entry <= 0:
        return 0.0

    gross_loss = abs(entry - sl) / entry * 100.0
    if normalize_status(candidate.get("order_type")) == "LIMIT":
        slippage_pct = CONFIG.TRADE.LIMIT_ENTRY_SLIPPAGE_PCT
    else:
        slippage_pct = CONFIG.TRADE.MARKET_ENTRY_SLIPPAGE_PCT

    return gross_loss + spread_pct + 0.08 + slippage_pct


def _is_trend_setup(candidate: Dict[str, Any]) -> bool:
    setup_reason = normalize_status(candidate.get("setup_reason"))
    setup_family = normalize_status(candidate.get("setup_family"))
    if setup_family == "TREND":
        return True
    return any(x in setup_reason for x in {"TREND_", "BREAKOUT", "BREAKDOWN", "PULLBACK"})


def _is_ranging_context(candidate: Dict[str, Any]) -> bool:
    tf_context = str(candidate.get("tf_context", ""))
    return ("1H=RANGE" in tf_context) and ("4H=RANGE" in tf_context)


def passes_order_filters(candidate: Dict[str, Any]) -> Tuple[bool, str]:
    hard_reasons: List[str] = []
    soft_flags: List[str] = []
    score = safe_float(candidate.get("score"))

    rr = safe_float(candidate["rr"])
    spread_pct = safe_float(candidate["spread_pct"])
    volume_24h = safe_float(candidate["volume_24h_usdt"])
    expected_net = safe_float(candidate["expected_net_pnl_pct"])
    funding_abs = abs(safe_float(candidate["funding_rate_pct"]))
    entry = safe_float(candidate["entry_trigger"])
    sl = safe_float(candidate["sl"])
    tp = safe_float(candidate["tp"])
    side = normalize_status(candidate.get("side"))
    stop_pct = (abs(entry - sl) / entry * 100.0) if entry > 0 else 0.0
    tp_pct = (abs(tp - entry) / entry * 100.0) if entry > 0 else 0.0

    if entry <= 0 or sl <= 0 or tp <= 0:
        hard_reasons.append("INVALID_GEOMETRY")
    elif side == "LONG" and not (sl < entry < tp):
        hard_reasons.append("INVALID_STOP_GEOMETRY")
    elif side == "SHORT" and not (tp < entry < sl):
        hard_reasons.append("INVALID_STOP_GEOMETRY")

    if spread_pct > CONFIG.FILTER.MAX_SPREAD_PCT * 1.8:
        hard_reasons.append("ABSURD_SPREAD")
    if volume_24h < CONFIG.FILTER.MIN_24H_VOLUME_USDT * 0.45:
        hard_reasons.append("UNUSABLE_LIQUIDITY")
    if rr < max(1.05, CONFIG.FILTER.MIN_RR * 0.75):
        hard_reasons.append("UNUSABLE_RR")
    if stop_pct < 0.08:
        hard_reasons.append("STOP_TOO_TIGHT")
    if stop_pct > 3.8:
        soft_flags.append("STOP_TOO_WIDE")
    if tp_pct < 0.12:
        hard_reasons.append("TP_TOO_CLOSE")

    if hard_reasons:
        return False, "|".join(hard_reasons)

    if _is_trend_setup(candidate) and _is_ranging_context(candidate):
        score -= 1.5
    if int(candidate.get("adaptive_blocked", 0)) == 1:
        score -= 2
    if int(candidate.get("market_event_blocked", 0)) == 1:
        score -= 3
    if volume_24h < CONFIG.FILTER.MIN_24H_VOLUME_USDT:
        score -= 1
    if spread_pct > CONFIG.FILTER.MAX_SPREAD_PCT:
        score -= 1
    if funding_abs > CONFIG.FILTER.MAX_FUNDING_RATE_PCT:
        score -= 1
    if expected_net < CONFIG.FILTER.MIN_EXPECTED_NET_PNL_PCT:
        score -= 1
    if int(candidate.get("adaptive_sample_size", 0)) > 0 and safe_float(candidate.get("adaptive_expectancy")) < CONFIG.FILTER.MIN_ADAPTIVE_EXPECTANCY:
        score -= 1
    if "STOP_TOO_WIDE" in soft_flags:
        score -= 1

    if soft_flags:
        existing_flags = str(candidate.get("soft_filter_flags", "")).strip()
        merged_flags = [x for x in existing_flags.split("|") if x] + soft_flags
        candidate["soft_filter_flags"] = "|".join(dict.fromkeys(merged_flags))

    candidate["score"] = max(0, int(round(score)))
    if candidate["score"] < CONFIG.FILTER.MIN_SCORE:
        return False, "LOW_SCORE"
    return True, "OK"


def create_virtual_order(candidate: Dict[str, Any]) -> Dict[str, Any]:
    live = safe_float(candidate.get("live_price"))
    low = safe_float(candidate.get("entry_zone_low"))
    high = safe_float(candidate.get("entry_zone_high"))
    trigger = safe_float(candidate.get("entry_trigger"))
    candidate["entry_type"] = "LIMIT_PULLBACK" if "PULLBACK" in normalize_status(candidate.get("setup_reason")) else "BREAKOUT_CONFIRM"
    in_zone = low <= live <= high if low > 0 and high > 0 else False
    near = abs(live - trigger) / trigger <= 0.0018 if trigger > 0 else False
    candidate["status"] = "READY" if (in_zone or near) else "WATCHING"
    candidate["zone_touched"] = 0
    candidate["watch_reason"] = "READY_IMMEDIATE_ZONE" if candidate["status"] == "READY" else "WAITING_ENTRY_ZONE"
    return stamp_updated(candidate)


# ============================================================================
# duplicate handling
# ============================================================================

def should_consider_duplicate(a: Dict[str, Any], b: Dict[str, Any]) -> bool:
    return (
        a.get("symbol") == b.get("symbol")
        and a.get("side") == b.get("side")
        and a.get("setup_type") == b.get("setup_type")
        and is_same_order_intent(a, b)
    )


def choose_order_to_keep(a: Dict[str, Any], b: Dict[str, Any]) -> Tuple[Dict[str, Any], Dict[str, Any]]:
    score_a = safe_float(a.get("score"))
    score_b = safe_float(b.get("score"))

    if score_a > score_b:
        return a, b
    if score_b > score_a:
        return b, a

    return a, b


def mark_order_closed(order: Dict[str, Any], status: str, reason: Optional[str] = None) -> Dict[str, Any]:
    order["status"] = status
    order["close_reason"] = order.get("close_reason") or reason or normalize_status(status)
    order["closed_at"] = order.get("closed_at") or now_utc()
    return stamp_updated(order)


def cleanup_duplicate_open_orders(orders: List[Dict[str, Any]]) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
    active: List[Dict[str, Any]] = []
    closed: List[Dict[str, Any]] = []

    for order in orders:
        status = get_order_status(order)
        if is_final_order_status(status):
            closed.append(order)
            continue

        duplicate_found = False
        for i, kept in enumerate(active):
            if should_consider_duplicate(order, kept):
                winner, loser = choose_order_to_keep(kept, order)
                loser["exchange_status"] = loser.get("exchange_status") or "LOCAL_DUPLICATE_CANCELLED"
                loser = mark_order_closed(loser, "CANCELLED", "CANCELLED")

                closed.append(loser)
                if winner is not kept:
                    active[i] = winner

                duplicate_found = True
                break

        if not duplicate_found:
            active.append(order)

    return active, closed


def check_duplicate_order(
    candidate: Dict[str, Any],
    open_orders: List[Dict[str, Any]],
    open_positions: List[Dict[str, Any]],
) -> Tuple[bool, str]:
    for pos in open_positions:
        if pos.get("symbol") == candidate["symbol"] and normalize_status(pos.get("status")) != "CLOSED":
            return False, "SYMBOL_HAS_OPEN_POSITION"

    for order in open_orders:
        if is_final_order_status(order.get("status")):
            continue
        if should_consider_duplicate(candidate, order):
            return False, "DUPLICATE_OPEN_ORDER"

    return True, "OK"


# ============================================================================
# exchange reconciliation
# ============================================================================

def apply_exchange_status_to_order(order: Dict[str, Any], exchange_status: str) -> Dict[str, Any]:
    local_status = EXCHANGE_TO_LOCAL_STATUS.get(normalize_status(exchange_status))
    if local_status:
        order["status"] = local_status
    return order


def reconcile_exchange_order_status(order: Dict[str, Any]) -> Dict[str, Any]:
    if CONFIG.ENGINE.EXECUTION_MODE != "REAL":
        return order

    exchange_order_id = order.get("exchange_order_id")
    client_order_id = order.get("client_order_id")
    if not exchange_order_id and not client_order_id:
        return order

    try:
        resp = binance.query_order(
            symbol=order["symbol"],
            order_id=exchange_order_id or None,
            client_order_id=client_order_id or None,
        )
    except Exception:
        return order

    if not resp:
        return order

    exchange_status = normalize_status(resp.get("status"))
    if exchange_status:
        order["exchange_status"] = exchange_status
        order = apply_exchange_status_to_order(order, exchange_status)

    order["executed_qty"] = resp.get("executedQty", order.get("executed_qty", 0.0))
    order["avg_fill_price"] = resp.get("avgPrice", order.get("avg_fill_price", 0.0))
    order["exchange_order_id"] = resp.get("orderId", order.get("exchange_order_id", ""))

    return stamp_updated(order)


def cancel_exchange_order_if_needed(order: Dict[str, Any]) -> Dict[str, Any]:
    if CONFIG.ENGINE.EXECUTION_MODE != "REAL":
        return order

    exchange_order_id = order.get("exchange_order_id")
    client_order_id = order.get("client_order_id")
    if not exchange_order_id and not client_order_id:
        return order

    try:
        latest = binance.query_order(
            symbol=order["symbol"],
            order_id=exchange_order_id or None,
            client_order_id=client_order_id or None,
        )
        latest_status = normalize_status((latest or {}).get("status"))
        if latest_status in {"FILLED", "CANCELED", "EXPIRED", "REJECTED"}:
            return reconcile_exchange_order_status(order)
    except Exception:
        pass

    try:
        resp = binance.cancel_order(
            symbol=order["symbol"],
            order_id=exchange_order_id or None,
            client_order_id=client_order_id or None,
        )
        exchange_status = normalize_status(resp.get("status"))
        if exchange_status == "CANCELED":
            order["exchange_status"] = "CANCELED"
            order["status"] = "CANCELLED"
            order = stamp_updated(order)
    except Exception:
        order = reconcile_exchange_order_status(order)

    return order


# ============================================================================
# persistence
# ============================================================================

def _append_rows_to_csv(path: str, rows: List[Dict[str, Any]]) -> None:
    if not rows:
        return

    fieldnames: List[str] = []
    seen = set()

    for row in rows:
        for key in row.keys():
            if key not in seen:
                seen.add(key)
                fieldnames.append(key)

    existing_rows: List[Dict[str, Any]] = []
    existing_ids = set()
    existing_fieldnames: List[str] = []

    if os.path.exists(path) and os.path.getsize(path) > 0:
        with open(path, "r", newline="") as f:
            reader = csv.DictReader(f)
            existing_fieldnames = list(reader.fieldnames or [])
            for row in reader:
                existing_rows.append(row)
                row_id = row.get("order_id") or row.get("position_id")
                if row_id:
                    existing_ids.add(row_id)

    for key in existing_fieldnames:
        if key not in seen:
            seen.add(key)
            fieldnames.append(key)

    new_rows = []
    for row in rows:
        row_id = row.get("order_id") or row.get("position_id")
        if row_id and row_id in existing_ids:
            continue
        new_rows.append(row)

    if not new_rows:
        return

    all_rows = existing_rows + new_rows
    with open(path, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for row in all_rows:
            writer.writerow({k: row.get(k, "") for k in fieldnames})


def append_closed_orders(rows: List[Dict[str, Any]]) -> None:
    append_fn = getattr(storage, "append_closed_orders", None)
    if callable(append_fn):
        append_fn(rows)
        return
    _append_rows_to_csv(CONFIG.FILES.CLOSED_ORDERS_CSV, rows)


def _record_feedback_from_closed_orders(rows: List[Dict[str, Any]]) -> None:
    if not rows:
        return
    try:
        recorded = adaptive.record_closed_orders(rows)
        if recorded:
            log.info("ADAPTIVE_REVIEWS_RECORDED count=%s", recorded)
    except Exception as exc:
        log.exception("ADAPTIVE_RECORD_CLOSED_ORDERS_ERROR err=%s", type(exc).__name__)


# ============================================================================
# optimizer / adaptive
# ============================================================================

def refresh_optimizer_state(force_persist: bool = True) -> Dict[str, Any]:
    try:
        snapshot = optimizer.optimize_from_reviews(persist=force_persist)
        log.info(
            "OPTIMIZER_REFRESH review_count=%s weights=%s",
            snapshot.get("review_count", 0),
            len((snapshot.get("setup_weights") or {})),
        )
        return snapshot
    except Exception as exc:
        log.exception("OPTIMIZER_REFRESH_ERROR err=%s", type(exc).__name__)
        return {}


def load_or_refresh_optimizer_weights(feedback_changed: bool) -> Dict[str, Any]:
    optimizer_snapshot: Dict[str, Any] = {}

    if feedback_changed:
        optimizer_snapshot = refresh_optimizer_state(force_persist=True)
    else:
        try:
            optimizer_weights = optimizer.load_optimizer_weights()
            if not optimizer_weights:
                optimizer_snapshot = refresh_optimizer_state(force_persist=True)
        except Exception:
            optimizer_snapshot = refresh_optimizer_state(force_persist=True)

    return optimizer_snapshot.get("setup_weights") or optimizer.load_optimizer_weights() or {}


def _apply_learning_layers(
    candidate: Dict[str, Any],
    optimizer_weights: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    try:
        candidate = optimizer.apply_optimizer_to_candidate(candidate, weights=optimizer_weights)
    except Exception as exc:
        log.exception(
            "ORDER_OPTIMIZER_APPLY_ERROR symbol=%s err=%s",
            candidate.get("symbol"),
            type(exc).__name__,
        )

    if int(candidate.get("adaptive_sample_size", 0)) == 0:
        try:
            candidate = adaptive.apply_adaptive_scoring(candidate)
        except Exception as exc:
            log.exception(
                "ORDER_ADAPTIVE_APPLY_ERROR symbol=%s err=%s",
                candidate.get("symbol"),
                type(exc).__name__,
            )

    return candidate


# ============================================================================
# open order reconciliation
# ============================================================================

def finalize_order_for_closure(order: Dict[str, Any]) -> Dict[str, Any]:
    status = get_order_status(order)

    if status == "CANCELLED":
        order = cancel_exchange_order_if_needed(order)

    order["closed_at"] = order.get("closed_at") or now_utc()
    order["close_reason"] = order.get("close_reason") or get_order_status(order)
    return order


def reconcile_open_orders() -> bool:
    orders = storage.load_open_orders()
    changed = False
    feedback_changed = False

    still_open: List[Dict[str, Any]] = []
    to_close: List[Dict[str, Any]] = []

    for order in orders:
        old_status = get_order_status(order)

        if is_order_expired(order) and not is_final_order_status(old_status):
            order["status"] = "EXPIRED"
            order = stamp_updated(order)
            changed = True

        before = get_order_status(order)
        order = reconcile_exchange_order_status(order)
        after = get_order_status(order)

        if before != after:
            order = stamp_updated(order)
            changed = True

        if is_final_order_status(order.get("status")):
            to_close.append(finalize_order_for_closure(order))
        else:
            still_open.append(order)

    deduped_open, duplicate_closed = cleanup_duplicate_open_orders(still_open)
    if duplicate_closed:
        changed = True
        for order in duplicate_closed:
            to_close.append(finalize_order_for_closure(order))

    if changed or deduped_open != orders:
        storage.save_open_orders(deduped_open)

    if to_close:
        append_closed_orders(to_close)
        _record_feedback_from_closed_orders(to_close)
        feedback_changed = True

    return feedback_changed


# ============================================================================
# virtual market state
# ============================================================================

def zone_is_touched(order: Dict[str, Any], live_price: float) -> bool:
    low = safe_float(order["entry_zone_low"])
    high = safe_float(order["entry_zone_high"])
    if low > high:
        low, high = high, low
    return low <= live_price <= high


def near_trigger(order: Dict[str, Any], live_price: float, ratio: float = 0.0015) -> bool:
    trigger = safe_float(order["entry_trigger"])
    if trigger <= 0:
        return False
    return abs(live_price - trigger) / trigger <= ratio


def update_virtual_order_market_state(order: Dict[str, Any], market_ctx: Dict[str, Any]) -> Dict[str, Any]:
    live_price = safe_float(market_ctx["last_price"])
    live_high = max(
        live_price,
        safe_float(market_ctx.get("last_1m_high", 0.0)),
        safe_float(market_ctx.get("candle_high", 0.0)),
        safe_float(market_ctx.get("high", 0.0)),
    )
    low_candidates = [
        live_price,
        safe_float(market_ctx.get("last_1m_low", 0.0)),
        safe_float(market_ctx.get("candle_low", 0.0)),
        safe_float(market_ctx.get("low", 0.0)),
    ]
    low_candidates = [v for v in low_candidates if v > 0]
    live_low = min(low_candidates) if low_candidates else live_price
    order["live_price"] = live_price
    order["live_high"] = live_high
    order["live_low"] = live_low
    status = get_order_status(order)
    order_type = normalize_status(order.get("order_type"))
    side = normalize_status(order.get("side"))
    trigger = safe_float(order.get("entry_trigger"))

    if order_type == "MARKET" and status in {"WATCHING", "PLANNED"} and trigger > 0:
        confirm_pct = CONFIG.TRADE.BREAKOUT_CONFIRM_PCT / 100.0
        tf_1h = market_ctx.get("tf", {}).get("1H", {})
        ema20 = safe_float(tf_1h.get("ema20"))
        if side == "LONG" and live_price >= trigger * (1 + confirm_pct) and (ema20 <= 0 or live_price >= ema20):
            order["status"] = "READY"
            order["watch_reason"] = "BREAKOUT_CONFIRM_LONG"
        elif side == "SHORT" and live_price <= trigger * (1 - confirm_pct) and (ema20 <= 0 or live_price <= ema20):
            order["status"] = "READY"
            order["watch_reason"] = "BREAKOUT_CONFIRM_SHORT"

    if zone_is_touched(order, live_price):
        if int(order.get("zone_touched", 0)) != 1:
            log.info(
                "ORDER_ZONE_TOUCHED symbol=%s order_id=%s side=%s setup=%s trigger=%s zone=[%s,%s] live=%s high=%s low=%s",
                order.get("symbol"),
                order.get("order_id"),
                side,
                order.get("setup_type"),
                trigger,
                order.get("entry_zone_low"),
                order.get("entry_zone_high"),
                live_price,
                live_high,
                live_low,
            )
        order["zone_touched"] = 1
        if get_order_status(order) in {"WATCHING", "PLANNED"}:
            order["status"] = "READY"
            order["watch_reason"] = "ZONE_TOUCHED"

    if near_trigger(order, live_price):
        order["alarm_near_trigger_sent"] = order.get("alarm_near_trigger_sent", 0)
        if get_order_status(order) in {"WATCHING", "PLANNED"}:
            order["status"] = "READY"
            order["watch_reason"] = "NEAR_TRIGGER"

    if _is_pullback_like_order(order) and int(order.get("zone_touched", 0)) == 1 and get_order_status(order) in {"WATCHING", "PLANNED"}:
        order["status"] = "READY"
        order["watch_reason"] = "PULLBACK_ZONE_TOUCHED_READY"

    if trigger > 0 and get_order_status(order) in {"WATCHING", "PLANNED", "READY", "NEW"}:
        drift_pct = abs(live_price - trigger) / trigger * 100.0
        if drift_pct > CONFIG.TRADE.DEAD_TRADE_MAX_DEVIATION_PCT:
            if _is_breakout_order(order):
                breakout_continuation = (side == "LONG" and live_price >= trigger) or (side == "SHORT" and live_price <= trigger)
                if not breakout_continuation:
                    order["status"] = "CANCELLED"
                    order["exchange_status"] = "DEAD_TRADE_KILLER"
                    order["close_reason"] = "DEAD_TRADE_KILLER"
                    order["drift_pct"] = drift_pct
                    return stamp_updated(order)
            else:
                zone_low = safe_float(order.get("entry_zone_low"))
                zone_high = safe_float(order.get("entry_zone_high"))
                ran_away = False
                if side == "LONG":
                    ran_away = live_price > max(zone_low, zone_high)
                elif side == "SHORT":
                    ran_away = live_price < min(zone_low, zone_high)
                if ran_away and int(order.get("zone_touched", 0)) == 0:
                    order["status"] = "CANCELLED"
                    order["exchange_status"] = "DEAD_TRADE_KILLER"
                    order["close_reason"] = "DEAD_TRADE_KILLER"
                    order["drift_pct"] = drift_pct
                    return stamp_updated(order)

    if get_order_status(order) in {"WATCHING", "PLANNED"}:
        order["watch_reason"] = order.get("watch_reason") or "WAITING_ENTRY_ZONE"

    return stamp_updated(order)


# ============================================================================
# order submission
# ============================================================================

def maybe_reject_invalid_symbol_meta(candidate: Dict[str, Any], symbol_meta: Dict[str, Any]) -> Tuple[bool, str]:
    tick_size = safe_float(symbol_meta.get("tick_size"))
    step_size = safe_float(symbol_meta.get("step_size"))
    min_qty = safe_float(symbol_meta.get("min_qty"))
    min_notional = safe_float(symbol_meta.get("min_notional"))

    if tick_size <= 0:
        return False, "INVALID_TICK_SIZE"
    if step_size <= 0:
        return False, "INVALID_STEP_SIZE"
    if min_qty < 0:
        return False, "INVALID_MIN_QTY"
    if min_notional < 0:
        return False, "INVALID_MIN_NOTIONAL"

    return True, "OK"


def _order_setup_type(order: Dict[str, Any]) -> str:
    return normalize_status(order.get("setup_type"))


def _order_entry_type(order: Dict[str, Any]) -> str:
    return normalize_status(order.get("entry_type"))


def _is_breakout_order(order: Dict[str, Any]) -> bool:
    setup_type = _order_setup_type(order)
    setup_reason = normalize_status(order.get("setup_reason"))
    entry_type = _order_entry_type(order)

    return (
        "BREAKOUT" in setup_type
        or "BREAKDOWN" in setup_type
        or "BREAKOUT" in setup_reason
        or "BREAKDOWN" in setup_reason
        or entry_type == "BREAKOUT_CONFIRM"
    )


def _is_pullback_like_order(order: Dict[str, Any]) -> bool:
    return not _is_breakout_order(order)


def _is_a_plus_order(order: Dict[str, Any]) -> bool:
    return "A_PLUS" in normalize_status(order.get("market_event_reason"))


def _priority_rank(order: Dict[str, Any]) -> Tuple[int, int, int, int, int]:
    status = get_order_status(order)
    score = int(safe_float(order.get("score"), 0.0))
    touched = int(order.get("zone_touched", 0))
    breakout_confirm = int(_order_entry_type(order) == "BREAKOUT_CONFIRM")
    a_plus = int(_is_a_plus_order(order))
    ready = int(status == "READY")
    return ready, touched, breakout_confirm + a_plus, score, int(_is_breakout_order(order))


def _preempt_weaker_orders_if_needed(order: Dict[str, Any], open_orders: List[Dict[str, Any]]) -> int:
    active_orders = [
        o for o in open_orders
        if not is_final_order_status(o.get("status")) and o.get("order_id") != order.get("order_id")
    ]
    overflow = (len(active_orders) + 1) - int(CONFIG.TRADE.MAX_OPEN_ORDERS)
    if overflow <= 0:
        return 0

    incoming_rank = _priority_rank(order)
    watchers = []
    for other in active_orders:
        status = get_order_status(other)
        if status not in {"WATCHING", "PLANNED"}:
            continue
        if _priority_rank(other) >= incoming_rank:
            continue
        age_min = get_order_age_minutes(other)
        untouched = int(other.get("zone_touched", 0)) == 0
        stale = age_min >= 60 or untouched
        if stale:
            watchers.append((other, age_min, untouched))

    watchers.sort(
        key=lambda row: (
            _priority_rank(row[0]),
            0 if row[2] else 1,
            -row[1],
        )
    )

    cancelled = 0
    for watcher, age_min, _ in watchers:
        watcher["status"] = "CANCELLED"
        watcher["exchange_status"] = "PREEMPTED_BY_READY"
        watcher["close_reason"] = "PREEMPTED_BY_READY"
        watcher["preempted_by_order_id"] = order.get("order_id", "")
        watcher["preempted_by_score"] = safe_float(order.get("score"))
        watcher["preempted_after_min"] = age_min
        stamp_updated(watcher)
        cancelled += 1
        if cancelled >= overflow:
            break
    return cancelled


def _paper_fill_or_queue(order: Dict[str, Any]) -> Dict[str, Any]:
    trigger = safe_float(order.get("entry_trigger"))
    live_price = safe_float(order.get("live_price"))
    live_high = max(live_price, safe_float(order.get("live_high", 0.0)))
    live_low = min([v for v in [live_price, safe_float(order.get("live_low", 0.0))] if v > 0] or [live_price])
    side = str(order.get("side", "")).upper()
    is_breakout = _is_breakout_order(order)

    order["exchange_status"] = "PAPER_NEW"
    order["status"] = "NEW"
    order["executed_qty"] = 0.0
    order["avg_fill_price"] = 0.0

    should_fill = normalize_status(order.get("order_type")) == "MARKET"
    if not should_fill:
        if is_breakout:
            if side == "LONG" and live_high >= trigger:
                should_fill = True
            elif side == "SHORT" and live_low <= trigger:
                should_fill = True
        else:
            if side == "LONG" and live_low <= trigger:
                should_fill = True
            elif side == "SHORT" and live_high >= trigger:
                should_fill = True

    log.info(
        "ORDER_PAPER_FILL_CHECK symbol=%s order_id=%s status=%s side=%s type=%s breakout=%s trigger=%s live=%s high=%s low=%s eligible=%s",
        order.get("symbol"),
        order.get("order_id"),
        order.get("status"),
        side,
        order.get("order_type"),
        int(is_breakout),
        trigger,
        live_price,
        live_high,
        live_low,
        int(should_fill),
    )

    if should_fill:
        order["status"] = "FILLED"
        order["exchange_status"] = "PAPER_FILLED"
        order["executed_qty"] = safe_float(order.get("submitted_qty"))
        order["avg_fill_price"] = live_price if normalize_status(order.get("order_type")) == "MARKET" else trigger
        log.info(
            "ORDER_PAPER_FILLED symbol=%s order_id=%s side=%s trigger=%s fill=%s qty=%s",
            order.get("symbol"),
            order.get("order_id"),
            side,
            trigger,
            order.get("avg_fill_price"),
            order.get("executed_qty"),
        )
    else:
        log.info(
            "ORDER_PAPER_QUEUED symbol=%s order_id=%s side=%s trigger=%s live=%s high=%s low=%s",
            order.get("symbol"),
            order.get("order_id"),
            side,
            trigger,
            live_price,
            live_high,
            live_low,
        )

    return stamp_updated(order)


def _paper_reconcile_pending_order(order: Dict[str, Any]) -> Dict[str, Any]:
    if CONFIG.ENGINE.EXECUTION_MODE != "PAPER":
        return order
    if get_order_status(order) not in {"NEW", "PARTIALLY_FILLED"}:
        return order
    if normalize_status(order.get("exchange_status")) == "PAPER_FILLED":
        return order

    trigger = safe_float(order.get("entry_trigger"))
    live_price = safe_float(order.get("live_price"))
    live_high = max(live_price, safe_float(order.get("live_high", 0.0)))
    live_low = min([v for v in [live_price, safe_float(order.get("live_low", 0.0))] if v > 0] or [live_price])
    side = normalize_status(order.get("side"))
    order_type = normalize_status(order.get("order_type"))
    is_breakout = _is_breakout_order(order)

    should_fill = order_type == "MARKET"
    if not should_fill:
        if is_breakout:
            should_fill = (side == "LONG" and live_high >= trigger) or (side == "SHORT" and live_low <= trigger)
        else:
            should_fill = (side == "LONG" and live_low <= trigger) or (side == "SHORT" and live_high >= trigger)

    log.info(
        "ORDER_PAPER_RECONCILE_CHECK symbol=%s order_id=%s side=%s type=%s breakout=%s trigger=%s live=%s high=%s low=%s eligible=%s",
        order.get("symbol"),
        order.get("order_id"),
        side,
        order_type,
        int(is_breakout),
        trigger,
        live_price,
        live_high,
        live_low,
        int(should_fill),
    )

    if not should_fill:
        return stamp_updated(order)

    order["status"] = "FILLED"
    order["exchange_status"] = "PAPER_FILLED"
    order["executed_qty"] = safe_float(order.get("submitted_qty"))
    order["avg_fill_price"] = live_price if order_type == "MARKET" else trigger
    log.info(
        "ORDER_PAPER_RECONCILE_FILLED symbol=%s order_id=%s side=%s trigger=%s fill=%s qty=%s",
        order.get("symbol"),
        order.get("order_id"),
        side,
        trigger,
        order.get("avg_fill_price"),
        order.get("executed_qty"),
    )
    return stamp_updated(order)


def _prepare_order_qty(order: Dict[str, Any], symbol_meta: Dict[str, Any]) -> Tuple[float, Optional[str]]:
    if CONFIG.ENGINE.EXECUTION_MODE == "PAPER":
        account_balance = safe_float(getattr(CONFIG.TRADE, "PAPER_BALANCE_USDT", 300.0), 300.0)
    else:
        account_balance = binance.get_available_balance("USDT")
    qty = risk.calc_position_size(
        entry=safe_float(order["entry_trigger"]),
        sl=safe_float(order["sl"]),
        account_balance=account_balance,
        risk_pct=CONFIG.TRADE.RISK_PER_TRADE_PCT,
        symbol_meta=symbol_meta,
    )
    size_mult = safe_float(order.get("size_mult", 1.0), 1.0)
    score = safe_float(order.get("score"))
    score_mult = 0.70 + min(0.60, max(0.0, (score - CONFIG.FILTER.MIN_SCORE) * 0.06))
    expectancy = safe_float(order.get("adaptive_expectancy"))
    expectancy_mult = 1.0 + min(0.35, max(-0.40, expectancy * 0.25))
    size_mult *= max(0.25, score_mult * expectancy_mult)
    soft_flags = str(order.get("soft_filter_flags", ""))
    if "STOP_TOO_WIDE" in soft_flags:
        size_mult *= 0.75
    if size_mult > 0:
        qty *= size_mult
    if qty <= 0:
        return 0.0, "QTY_LE_ZERO"
    return qty, None


def _ensure_client_order_id(order: Dict[str, Any]) -> Dict[str, Any]:
    if not order.get("client_order_id"):
        order["client_order_id"] = make_client_order_id(
            order["symbol"],
            order["side"],
            order["setup_type"],
            safe_float(order["entry_trigger"]),
        )
    return order


def _submit_real_exchange_order(order: Dict[str, Any], qty: float) -> Dict[str, Any]:
    if order["order_type"] == "LIMIT":
        return binance.safe_submit_order(
            binance.place_limit_entry,
            order["symbol"],
            order["client_order_id"],
            order["symbol"],
            order["side"],
            qty,
            safe_float(order["entry_trigger"]),
            order["client_order_id"],
        )

    return binance.safe_submit_order(
        binance.place_market_entry,
        order["symbol"],
        order["client_order_id"],
        order["symbol"],
        order["side"],
        qty,
        order["client_order_id"],
    )


def submit_real_order_from_virtual(order: Dict[str, Any]) -> Dict[str, Any]:
    symbol_meta = binance.get_symbol_meta(order["symbol"])
    ok, reason = maybe_reject_invalid_symbol_meta(order, symbol_meta)
    if not ok:
        order["status"] = "REJECTED"
        order["exchange_status"] = reason
        return stamp_updated(order)

    qty, qty_error = _prepare_order_qty(order, symbol_meta)
    if qty_error:
        order["status"] = "REJECTED"
        order["exchange_status"] = qty_error
        return stamp_updated(order)

    order["submitted_qty"] = qty
    order = _ensure_client_order_id(order)
    log.info(
        "ORDER_SUBMIT_TRANSITION symbol=%s order_id=%s side=%s type=%s status=%s zone_touched=%s trigger=%s live=%s high=%s low=%s adaptive_reason=%s",
        order.get("symbol"),
        order.get("order_id"),
        order.get("side"),
        order.get("order_type"),
        order.get("status"),
        order.get("zone_touched", 0),
        order.get("entry_trigger"),
        order.get("live_price"),
        order.get("live_high"),
        order.get("live_low"),
        order.get("adaptive_reason", ""),
    )

    if CONFIG.ENGINE.EXECUTION_MODE == "PAPER":
        return _paper_fill_or_queue(order)

    try:
        resp = _submit_real_exchange_order(order, qty)
    except Exception as exc:
        log.exception(
            "ORDER_SUBMIT_ERROR symbol=%s order_id=%s side=%s type=%s qty=%s err=%s",
            order.get("symbol"),
            order.get("order_id"),
            order.get("side"),
            order.get("order_type"),
            qty,
            exc,
        )
        order["status"] = "FAILED"
        order["exchange_status"] = f"SUBMIT_ERROR:{type(exc).__name__}:{str(exc)}"
        order["submit_error_detail"] = str(exc)
        return stamp_updated(order)

    order["exchange_order_id"] = resp.get("orderId", order.get("exchange_order_id", ""))
    order["exchange_status"] = normalize_status(resp.get("status", "NEW"))
    order["executed_qty"] = resp.get("executedQty", order.get("executed_qty", 0.0))
    order["avg_fill_price"] = resp.get("avgPrice", order.get("avg_fill_price", 0.0))
    order = apply_exchange_status_to_order(order, order["exchange_status"])

    if get_order_status(order) not in ACTIVE_ORDER_STATUSES | FINAL_ORDER_STATUSES:
        order["status"] = "NEW"

    order = stamp_updated(order)
    notify_real_order_submitted(order)
    return order


# ============================================================================
# order creation
# ============================================================================

def maybe_create_virtual_order(
    symbol: str,
    market_ctx: Dict[str, Any],
    open_orders: List[Dict[str, Any]],
    open_positions: List[Dict[str, Any]],
    optimizer_weights: Optional[Dict[str, Any]] = None,
) -> Optional[Dict[str, Any]]:
    if not market_ctx:
        market_ctx = _get_market_context_from_local_cache(symbol)
    candidate = build_order_candidate(symbol, market_ctx)
    if not candidate:
        return None

    event_source = str(candidate.get("setup_type") or candidate.get("setup_reason") or "")
    event_stats = {}
    event_n = 0
    event_wr = 0.0
    event_bonus = 0

    if event_source:
        try:
            event_key = build_event_key(event_source, market_ctx)
            event_stats = get_event_stats(event_key) or {}
        except Exception as exc:
            log.exception("EVENT_MINER_LOOKUP_ERROR symbol=%s err=%s", symbol, type(exc).__name__)
            event_stats = {}

    if event_stats:
        event_n = int(event_stats.get("n", 0) or 0)
        event_wr = float(event_stats.get("win_rate", 0.0) or 0.0)

        if event_n >= 20:
            raw_bonus = (event_wr - 0.5) * 10.0
            event_bonus = int(round(max(-2.0, min(2.0, raw_bonus))))

    candidate["score"] = score_candidate(candidate, market_ctx) + event_bonus
    candidate["event_miner_sample_size"] = event_n
    candidate["event_miner_win_rate"] = event_wr
    candidate["event_miner_score_bonus"] = event_bonus
    candidate["expected_net_pnl_pct"] = estimate_expected_net_pnl_pct(candidate)
    candidate["stop_net_loss_pct"] = estimate_stop_net_loss_pct(candidate)

    candidate = _apply_learning_layers(candidate, optimizer_weights=optimizer_weights)
    candidate = _apply_market_event_intelligence(candidate, market_ctx)

    candidate["expected_net_pnl_pct"] = estimate_expected_net_pnl_pct(candidate)
    candidate["stop_net_loss_pct"] = estimate_stop_net_loss_pct(candidate)

    if int(candidate.get("adaptive_sample_size", 0)) > 0 and safe_float(candidate.get("adaptive_expectancy")) < 0:
        candidate["score"] = max(0, int(candidate["score"]) - 1)

    ok, reason = passes_order_filters(candidate)
    if not ok:
        log.info(
            "ORDER_SKIP_FILTER %s %s score=%s setup=%s entry_type=%s delta=%s exp=%s samples=%s event=%s event_wr=%s event_n=%s",
            symbol,
            reason,
            candidate.get("score", 0),
            candidate.get("setup_reason", ""),
            candidate.get("entry_type", ""),
            candidate.get("adaptive_score_delta", 0),
            candidate.get("adaptive_expectancy", 0.0),
            candidate.get("adaptive_sample_size", 0),
            candidate.get("market_event_key", ""),
            candidate.get("market_event_winrate", 0.0),
            candidate.get("market_event_sample_size", 0),
        )
        return None

    ok, reason = risk.can_open_new_order(candidate, open_orders, open_positions)
    if not ok:
        log.info("ORDER_SKIP_RISK %s %s", symbol, reason)
        return None

    ok, reason = check_duplicate_order(candidate, open_orders, open_positions)
    if not ok:
        log.info("ORDER_SKIP_DUPLICATE %s %s", symbol, reason)
        return None

    order = create_virtual_order(candidate)

    try:
        notify_order_created(order)
    except Exception:
        pass

    return order


# ============================================================================
# processing
# ============================================================================

def should_skip_order_update(order: Dict[str, Any]) -> bool:
    return is_final_order_status(order.get("status"))


def get_order_age_minutes(order: Dict[str, Any]) -> float:
    created_at = order.get("created_at")
    if not created_at:
        return 0.0
    try:
        dt = datetime.strptime(created_at, "%Y-%m-%d %H:%M:%S UTC").replace(tzinfo=timezone.utc)
        now = datetime.now(timezone.utc)
        return (now - dt).total_seconds() / 60.0
    except Exception:
        return 0.0


def _parse_utc(raw: str) -> Optional[datetime]:
    try:
        return datetime.strptime(str(raw), "%Y-%m-%d %H:%M:%S UTC").replace(tzinfo=timezone.utc)
    except Exception:
        return None


def _symbol_is_on_cooldown(symbol: str) -> bool:
    cooldown_min = max(0, int(CONFIG.TRADE.ORDER_COOLDOWN_MINUTES))
    if cooldown_min <= 0:
        return False
    path = CONFIG.FILES.CLOSED_ORDERS_CSV
    if not os.path.exists(path):
        return False

    now = datetime.now(timezone.utc)
    cutoff = now - timedelta(minutes=cooldown_min)
    try:
        with open(path, "r", newline="") as f:
            for row in reversed(list(csv.DictReader(f))):
                if row.get("symbol") != symbol:
                    continue
                closed_at = _parse_utc(row.get("closed_at", ""))
                if closed_at and closed_at >= cutoff:
                    return True
                if closed_at:
                    return False
    except Exception:
        return False
    return False


def process_existing_order(
    order: Dict[str, Any],
    market_ctx: Dict[str, Any],
    open_orders: List[Dict[str, Any]],
    open_positions: List[Dict[str, Any]],
) -> Dict[str, Any]:
    if should_skip_order_update(order):
        return order

    order = update_virtual_order_market_state(order, market_ctx)

    current_event = _extract_market_event(market_ctx)
    if get_order_status(order) in {"WATCHING", "PLANNED", "READY"}:
        current_event_type = current_event.get("event_type", "")
        if current_event_type in BAD_EVENT_TYPES:
            order["status"] = "CANCELLED"
            order["exchange_status"] = "EVENT_DECAY_CANCEL"
            order["close_reason"] = "EVENT_DECAY_CANCEL"
            return stamp_updated(order)

    status = get_order_status(order)

    if status in {"WATCHING", "PLANNED"}:
        age_min = get_order_age_minutes(order)
        if age_min > 240:
            if _is_breakout_order(order):
                order["status"] = "EXPIRED"
                order["exchange_status"] = "STALE_NO_BREAKOUT"
                order["close_reason"] = "STALE_NO_BREAKOUT"
                return stamp_updated(order)
            if int(order.get("zone_touched", 0)) == 0:
                order["status"] = "EXPIRED"
                order["exchange_status"] = "STALE_NO_TOUCH"
                order["close_reason"] = "STALE_NO_TOUCH"
                return stamp_updated(order)

    if get_order_status(order) == "READY":
        ok, reason = risk.can_open_new_order(order, open_orders, open_positions)
        if not ok and reason == "MAX_OPEN_ORDERS_REACHED":
            cancelled = _preempt_weaker_orders_if_needed(order, open_orders)
            if cancelled > 0:
                active_after_preempt = [o for o in open_orders if not is_final_order_status(o.get("status"))]
                ok, reason = risk.can_open_new_order(order, active_after_preempt, open_positions)
        if not ok:
            log.info(
                "ORDER_READY_BLOCKED symbol=%s order_id=%s reason=%s side=%s setup=%s adaptive_reason=%s zone_touched=%s",
                order.get("symbol"),
                order.get("order_id"),
                reason,
                order.get("side"),
                order.get("setup_type"),
                order.get("adaptive_reason", ""),
                order.get("zone_touched", 0),
            )
            order["status"] = "CANCELLED"
            order["exchange_status"] = reason
            order["close_reason"] = reason
            return stamp_updated(order)

        others = [o for o in open_orders if o.get("order_id") != order.get("order_id")]
        ok, reason = check_duplicate_order(order, others, open_positions)
        if not ok:
            order["status"] = "CANCELLED"
            order["exchange_status"] = reason
            order["close_reason"] = reason
            return stamp_updated(order)

        order = submit_real_order_from_virtual(order)

    if get_order_status(order) in {"NEW", "PARTIALLY_FILLED"}:
        if CONFIG.ENGINE.EXECUTION_MODE == "PAPER":
            order = _paper_reconcile_pending_order(order)
        else:
            order = reconcile_exchange_order_status(order)

    return order


# ============================================================================
# main scan
# ============================================================================

def scan_once() -> None:
    feedback_changed = reconcile_open_orders()
    optimizer_weights = load_or_refresh_optimizer_weights(feedback_changed)

    open_orders = storage.load_open_orders()
    open_positions = storage.load_open_positions()
    symbols = market.get_top_symbols_by_volume(CONFIG.ENGINE.MAX_SYMBOLS)

    log.info(
        "ORDER_SCAN_START symbols=%s open_orders=%s open_positions=%s max_open_orders=%s optimizer_weights=%s",
        len(symbols),
        len(open_orders),
        len(open_positions),
        CONFIG.TRADE.MAX_OPEN_ORDERS,
        len(optimizer_weights),
    )

    updated_orders: List[Dict[str, Any]] = []

    for order in open_orders:
        symbol = order.get("symbol")
        if not symbol:
            continue

        try:
            market_ctx = market.build_market_context(symbol)
            updated = process_existing_order(order, market_ctx, open_orders, open_positions)
        except Exception as exc:
            log.exception("ORDER_PROCESS_ERROR symbol=%s err=%s", symbol, type(exc).__name__)
            updated = order

        updated_orders.append(updated)

    refreshed_active_orders = [
        o for o in updated_orders
        if not is_final_order_status(o.get("status"))
    ]
    active_symbols = {o["symbol"] for o in refreshed_active_orders}
    position_symbols = {
        p["symbol"]
        for p in open_positions
        if normalize_status(p.get("status")) == "OPEN_POSITION"
    }

    created_this_scan = 0
    max_new_per_scan = max(1, CONFIG.TRADE.MAX_NEW_ORDERS_PER_SCAN)

    for symbol in symbols:
        if created_this_scan >= max_new_per_scan:
            break
        if symbol in active_symbols or symbol in position_symbols:
            continue
        if _symbol_is_on_cooldown(symbol):
            continue

        try:
            market_ctx = market.build_market_context(symbol)
            new_order = maybe_create_virtual_order(
                symbol,
                market_ctx,
                refreshed_active_orders,
                open_positions,
                optimizer_weights=optimizer_weights,
            )
        except Exception as exc:
            log.exception("ORDER_CREATE_ERROR symbol=%s err=%s", symbol, type(exc).__name__)
            continue

        if new_order:
            log.info(
                "ORDER_WATCHING_CREATED %s side=%s zone_low=%s zone_high=%s trigger=%s score=%s delta=%s reason=%s expectancy=%s samples=%s event=%s event_wr=%s event_n=%s",
                new_order["symbol"],
                new_order["side"],
                new_order["entry_zone_low"],
                new_order["entry_zone_high"],
                new_order["entry_trigger"],
                new_order["score"],
                new_order.get("adaptive_score_delta", 0),
                new_order.get("adaptive_reason", ""),
                new_order.get("adaptive_expectancy", 0.0),
                new_order.get("adaptive_sample_size", 0),
                new_order.get("market_event_key", ""),
                new_order.get("market_event_winrate", 0.0),
                new_order.get("market_event_sample_size", 0),
            )
            refreshed_active_orders.append(new_order)
            updated_orders.append(new_order)
            created_this_scan += 1

    deduped_open, duplicate_closed = cleanup_duplicate_open_orders(updated_orders)
    storage.save_open_orders(deduped_open)

    if duplicate_closed:
        for row in duplicate_closed:
            row["closed_at"] = row.get("closed_at") or now_utc()
            row["close_reason"] = row.get("close_reason") or normalize_status(row.get("status"))

        append_closed_orders(duplicate_closed)
        _record_feedback_from_closed_orders(duplicate_closed)
        refresh_optimizer_state(force_persist=True)


if __name__ == "__main__":
    scan_once()
