from __future__ import annotations

import json
import os
import time
from typing import Any, Dict, List, Optional, Tuple

from utils import calc_rr, safe_float

_DEFAULT_STATS_PATH = os.getenv("NEWS_STATS_JSON", "data/news_backtest/news_stats.json")
_DEFAULT_EVENTS_PATH = os.getenv("NEWS_EVENTS_JSON", "data/news_backtest/events_enriched.json")
_LOOKBACK_SECONDS = int(os.getenv("NEWS_EVENT_LOOKBACK_SECONDS", str(8 * 3600)))

_STATS_CACHE: Dict[str, Any] = {"mtime": 0.0, "path": "", "payload": {}}
_EVENTS_CACHE: Dict[str, Any] = {"mtime": 0.0, "path": "", "payload": []}


def _load_json_cached(path: str, cache: Dict[str, Any], default: Any) -> Any:
    if not path:
        return default
    if not os.path.exists(path):
        return default

    try:
        mtime = os.path.getmtime(path)
    except OSError:
        return default

    if cache.get("path") == path and cache.get("mtime") == mtime:
        return cache.get("payload", default)

    try:
        with open(path, "r", encoding="utf-8") as f:
            payload = json.load(f)
    except Exception:
        return default

    cache["path"] = path
    cache["mtime"] = mtime
    cache["payload"] = payload
    return payload


def _load_stats() -> Dict[str, Any]:
    return _load_json_cached(_DEFAULT_STATS_PATH, _STATS_CACHE, {})


def _load_events() -> List[Dict[str, Any]]:
    rows = _load_json_cached(_DEFAULT_EVENTS_PATH, _EVENTS_CACHE, [])
    return rows if isinstance(rows, list) else []


def _now_ms() -> int:
    return int(time.time() * 1000)


def _latest_symbol_event(symbol: str) -> Optional[Dict[str, Any]]:
    now_ms = _now_ms()
    best: Optional[Dict[str, Any]] = None

    for event in _load_events():
        if str(event.get("symbol", "")).upper() != symbol.upper():
            continue
        ts = int(safe_float(event.get("event_time_ms", 0)))
        if ts <= 0:
            continue
        if now_ms - ts > _LOOKBACK_SECONDS * 1000:
            continue
        if best is None or ts > int(safe_float(best.get("event_time_ms", 0))):
            best = event

    return best


def _event_sentiment_bucket(score: float) -> str:
    if score >= 0.15:
        return "positive"
    if score <= -0.15:
        return "negative"
    return "neutral"


def _derivatives_bucket(funding_rate_pct: float, open_interest_change_pct: float) -> str:
    hot = funding_rate_pct >= 0.03 and open_interest_change_pct >= 1.5
    stressed = funding_rate_pct <= -0.03 and open_interest_change_pct >= 1.5
    if hot:
        return "euphoric"
    if stressed:
        return "panic"
    if open_interest_change_pct >= 1.0:
        return "leveraged"
    return "balanced"


def _find_stat(stats: Dict[str, Any], symbol: str, event_type: str, sentiment_bucket: str, derivatives_bucket: str) -> Optional[Dict[str, Any]]:
    rows = stats.get("symbol_event_stats", []) if isinstance(stats, dict) else []
    symbol = symbol.upper()

    best: Optional[Dict[str, Any]] = None
    for row in rows:
        if str(row.get("symbol", "")).upper() != symbol:
            continue
        if str(row.get("event_type", "unknown")) != event_type:
            continue
        if str(row.get("sentiment_bucket", "neutral")) != sentiment_bucket:
            continue
        if str(row.get("derivatives_bucket", "balanced")) != derivatives_bucket:
            continue
        if best is None or safe_float(row.get("sample_size", 0)) > safe_float(best.get("sample_size", 0)):
            best = row

    if best:
        return best

    for row in rows:
        if str(row.get("symbol", "")).upper() != symbol:
            continue
        if str(row.get("event_type", "unknown")) != event_type:
            continue
        if str(row.get("sentiment_bucket", "neutral")) != sentiment_bucket:
            continue
        return row
    return None


def apply_news_context(candidate: Dict[str, Any], market_ctx: Dict[str, Any]) -> Dict[str, Any]:
    symbol = str(candidate.get("symbol", "")).upper()
    side = str(candidate.get("side", "")).upper()

    candidate.setdefault("news_data_available", 0)
    candidate.setdefault("news_score_delta", 0)
    candidate.setdefault("news_blocked", 0)
    candidate.setdefault("news_block_reason", "")
    candidate.setdefault("news_alignment", "unknown")
    candidate.setdefault("news_tp_policy", "default")

    latest_event = _latest_symbol_event(symbol)
    if not latest_event:
        return candidate

    candidate["news_data_available"] = 1
    sentiment_score = safe_float(latest_event.get("sentiment_score", 0.0))
    sentiment_bucket = _event_sentiment_bucket(sentiment_score)
    event_type = str(latest_event.get("event_type", "unknown"))

    funding_rate_pct = safe_float(market_ctx.get("funding_rate_pct", candidate.get("funding_rate_pct", 0.0)))
    open_interest_change_pct = safe_float(market_ctx.get("open_interest_change_pct", 0.0))
    derivatives_bucket = _derivatives_bucket(funding_rate_pct, open_interest_change_pct)

    stats = _load_stats()
    stat = _find_stat(stats, symbol, event_type, sentiment_bucket, derivatives_bucket)

    aligned = (side == "LONG" and sentiment_bucket == "positive") or (side == "SHORT" and sentiment_bucket == "negative")
    conflict = (side == "LONG" and sentiment_bucket == "negative") or (side == "SHORT" and sentiment_bucket == "positive")
    neutral = sentiment_bucket == "neutral"

    if aligned:
        candidate["news_alignment"] = "aligned"
    elif conflict:
        candidate["news_alignment"] = "conflict"
    elif neutral:
        candidate["news_alignment"] = "neutral"

    mention_volume = safe_float(latest_event.get("mention_volume", 0.0))
    crowd_extreme = (derivatives_bucket == "euphoric" and side == "LONG") or (derivatives_bucket == "panic" and side == "SHORT")
    if mention_volume >= 90 and crowd_extreme:
        candidate["news_blocked"] = 1
        candidate["news_block_reason"] = f"CROWD_EXTREME_{derivatives_bucket.upper()}"

    if conflict and not neutral:
        candidate["news_blocked"] = 1
        candidate["news_block_reason"] = "NEWS_CONFLICT"

    if aligned and not candidate.get("news_blocked"):
        candidate["news_score_delta"] = int(safe_float(candidate.get("news_score_delta", 0))) + 2

    continuation_probability = safe_float(stat.get("continuation_probability", 0.0)) if stat else 0.0
    reversal_probability = safe_float(stat.get("reversal_probability", 0.0)) if stat else 0.0
    expectancy = safe_float(stat.get("expectancy_close_return_pct", 0.0)) if stat else 0.0

    if aligned and continuation_probability >= 0.58 and expectancy > 0:
        tp_mult = 1.12
        entry = safe_float(candidate.get("entry_trigger", 0.0))
        tp = safe_float(candidate.get("tp", 0.0))
        if entry > 0 and tp > 0:
            if side == "LONG":
                candidate["tp"] = tp + abs(tp - entry) * (tp_mult - 1.0)
            else:
                candidate["tp"] = tp - abs(tp - entry) * (tp_mult - 1.0)
            candidate["rr"] = calc_rr(entry, safe_float(candidate.get("sl", 0.0)), safe_float(candidate.get("tp", 0.0)), side)
        candidate["news_tp_policy"] = "widen_or_trail"
    elif conflict or reversal_probability >= 0.60:
        candidate["news_tp_policy"] = "reduce_or_skip"
        entry = safe_float(candidate.get("entry_trigger", 0.0))
        tp = safe_float(candidate.get("tp", 0.0))
        if entry > 0 and tp > 0:
            if side == "LONG":
                candidate["tp"] = entry + (tp - entry) * 0.80
            else:
                candidate["tp"] = entry - (entry - tp) * 0.80
            candidate["rr"] = calc_rr(entry, safe_float(candidate.get("sl", 0.0)), safe_float(candidate.get("tp", 0.0)), side)

    return candidate
