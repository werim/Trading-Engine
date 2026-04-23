from __future__ import annotations

from typing import Any, Dict, List, Optional, Tuple


_EVENT_STATS: Dict[str, Dict[str, Any]] = {}

DEFAULT_MIN_CANDLES = 30
LOOKBACK_CORE = 20


def _safe_float(value: Any, default: float = 0.0) -> float:
    try:
        if value is None:
            return default
        return float(value)
    except (TypeError, ValueError):
        return default


def _normalize_token(value: Any) -> str:
    return str(value or "").strip().lower()


def _mean(values: List[float]) -> float:
    return sum(values) / len(values) if values else 0.0


def _std(values: List[float]) -> float:
    if len(values) < 2:
        return 0.0
    avg = _mean(values)
    var = sum((x - avg) ** 2 for x in values) / len(values)
    return var ** 0.5


def _pct_change(a: float, b: float) -> float:
    if a <= 0:
        return 0.0
    return (b - a) / a


def _normalize_side(raw: Any) -> str:
    side = str(raw or "").strip().upper()
    return side if side in {"LONG", "SHORT"} else ""


def _get_regime(market_ctx: Dict[str, Any], tf_name: str) -> str:
    return str(
        market_ctx.get("tf", {})
        .get(tf_name, {})
        .get("regime", "")
    ).strip().upper()


def load_event_stats(data: Dict[str, Dict]) -> None:
    global _EVENT_STATS
    _EVENT_STATS = {}

    if not isinstance(data, dict):
        return

    for key, raw in data.items():
        if not isinstance(raw, dict):
            continue

        n = int(_safe_float(raw.get("n", raw.get("count", 0)), 0))
        wins = int(_safe_float(raw.get("wins", 0), 0))
        losses = int(_safe_float(raw.get("losses", 0), 0))
        total_pnl = _safe_float(raw.get("total_pnl", raw.get("sum_pnl", 0.0)), 0.0)

        if n <= 0:
            n = max(0, wins + losses)

        win_rate = raw.get("win_rate")
        if win_rate is None:
            win_rate = (wins / n) if n > 0 else 0.0
        else:
            win_rate = _safe_float(win_rate, 0.0)

        avg_pnl = raw.get("avg_pnl")
        if avg_pnl is None:
            avg_pnl = (total_pnl / n) if n > 0 else 0.0
        else:
            avg_pnl = _safe_float(avg_pnl, 0.0)

        _EVENT_STATS[str(key)] = {
            "n": n,
            "count": n,
            "wins": wins,
            "losses": losses,
            "win_rate": win_rate,
            "avg_pnl": avg_pnl,
            "total_pnl": total_pnl,
            "tp_hit_rate": _safe_float(raw.get("tp_hit_rate", 0.0), 0.0),
            "sl_hit_rate": _safe_float(raw.get("sl_hit_rate", 0.0), 0.0),
        }


def get_event_stats(event_key: str) -> Optional[Dict[str, Any]]:
    return _EVENT_STATS.get(str(event_key))


def all_event_stats() -> Dict[str, Dict[str, Any]]:
    return dict(_EVENT_STATS)


def _extract_candles(market_ctx: Dict[str, Any]) -> List[Dict[str, Any]]:
    candles = market_ctx.get("candles", [])
    if isinstance(candles, list):
        return [c for c in candles if isinstance(c, dict)]
    return []


def _build_series(candles: List[Dict[str, Any]]) -> Tuple[List[float], List[float], List[float], List[float]]:
    closes = [_safe_float(c.get("close")) for c in candles]
    volumes = [_safe_float(c.get("volume")) for c in candles]
    highs = [_safe_float(c.get("high")) for c in candles]
    lows = [_safe_float(c.get("low")) for c in candles]
    return closes, volumes, highs, lows


def _event_tags_from_market_ctx(market_ctx: Dict[str, Any]) -> List[str]:
    candles = _extract_candles(market_ctx)
    last_price = _safe_float(market_ctx.get("last_price"))

    if len(candles) < DEFAULT_MIN_CANDLES or last_price <= 0:
        return ["base"]

    closes, volumes, highs, lows = _build_series(candles)

    closes = closes[-30:]
    volumes = volumes[-30:]
    highs = highs[-30:]
    lows = lows[-30:]

    last_close = closes[-1]
    last_volume = volumes[-1]
    last_high = highs[-1]
    last_low = lows[-1]

    prev_closes = closes[-(LOOKBACK_CORE + 1):-1]
    prev_highs = highs[-(LOOKBACK_CORE + 1):-1]
    prev_lows = lows[-(LOOKBACK_CORE + 1):-1]
    prev_volumes = volumes[-(LOOKBACK_CORE + 1):-1]

    tags: List[str] = []

    avg_vol = _mean(prev_volumes)
    if avg_vol > 0:
        vol_ratio = last_volume / avg_vol
        if vol_ratio >= 2.2:
            tags.append("volume_spike_hard")
        elif vol_ratio >= 1.45:
            tags.append("volume_spike")

    prev_close_high = max(prev_closes) if prev_closes else last_close
    prev_close_low = min(prev_closes) if prev_closes else last_close
    prev_high = max(prev_highs) if prev_highs else last_high
    prev_low = min(prev_lows) if prev_lows else last_low

    if last_close > prev_close_high:
        tags.append("breakout_up_close")
    elif last_close < prev_close_low:
        tags.append("breakout_down_close")
    elif last_high > prev_high:
        tags.append("breakout_up_wick")
    elif last_low < prev_low:
        tags.append("breakout_down_wick")

    ranges = [max(0.0, h - l) for h, l in zip(prev_highs, prev_lows)]
    avg_range = _mean(ranges)
    last_range = max(0.0, last_high - last_low)

    if avg_range > 0:
        range_ratio = last_range / avg_range
        if range_ratio >= 1.8:
            tags.append("volatility_burst")
        elif range_ratio <= 0.70:
            tags.append("range_compression")

    rets = []
    for i in range(1, len(closes)):
        prev = closes[i - 1]
        cur = closes[i]
        if prev > 0:
            rets.append((cur - prev) / prev)

    ret_std = _std(rets[:-1]) if len(rets) >= 5 else 0.0
    last_ret = rets[-1] if rets else 0.0

    if ret_std > 0:
        z = abs(last_ret) / ret_std
        if z >= 2.5:
            tags.append("momentum_shock")

    tf_1h = market_ctx.get("tf", {}).get("1H", {}) or {}
    tf_4h = market_ctx.get("tf", {}).get("4H", {}) or {}
    tf_1d = market_ctx.get("tf", {}).get("1D", {}) or {}

    ema20_1h = _safe_float(tf_1h.get("ema20"))
    ema50_1h = _safe_float(tf_1h.get("ema50"))
    ema20_4h = _safe_float(tf_4h.get("ema20"))
    ema50_4h = _safe_float(tf_4h.get("ema50"))

    reg1 = _get_regime(market_ctx, "1H")
    reg4 = _get_regime(market_ctx, "4H")
    regd = _get_regime(market_ctx, "1D")

    if reg1 == reg4 == regd and reg1 in {"LONG", "SHORT"}:
        tags.append(f"trend_aligned_{reg1.lower()}")
    elif reg1 == reg4 and reg1 in {"LONG", "SHORT"}:
        tags.append(f"trend_stack_{reg1.lower()}")
    elif reg1 == "RANGE" and reg4 == "RANGE":
        tags.append("range_regime")

    if ema20_1h > 0 and ema50_1h > 0:
        if last_close > ema20_1h > ema50_1h:
            tags.append("ema_bull_1h")
        elif last_close < ema20_1h < ema50_1h:
            tags.append("ema_bear_1h")

    if ema20_4h > 0 and ema50_4h > 0:
        if last_close > ema20_4h > ema50_4h:
            tags.append("ema_bull_4h")
        elif last_close < ema20_4h < ema50_4h:
            tags.append("ema_bear_4h")

    atr_1h = _safe_float(tf_1h.get("atr"))
    if atr_1h > 0 and last_price > 0:
        atr_pct = atr_1h / last_price
        if atr_pct >= 0.025:
            tags.append("atr_hot")
        elif atr_pct <= 0.008:
            tags.append("atr_calm")

    tags = sorted(set(_normalize_token(t) for t in tags if t))
    return tags or ["base"]


def mine_events(market_ctx: Dict[str, Any]) -> List[str]:
    return _event_tags_from_market_ctx(market_ctx)


def _canonical_setup_name(setup: str) -> str:
    raw = str(setup or "").strip().upper()
    if not raw:
        return "UNKNOWN"

    if "BREAKOUT" in raw or "BREAKDOWN" in raw:
        if "LONG" in raw:
            return "BREAKOUT_LONG"
        if "SHORT" in raw:
            return "BREAKOUT_SHORT"
        return "BREAKOUT"

    if "PULLBACK" in raw:
        if "LONG" in raw:
            return "PULLBACK_LONG"
        if "SHORT" in raw:
            return "PULLBACK_SHORT"
        return "PULLBACK"

    if "RECLAIM" in raw:
        if "LONG" in raw:
            return "RECLAIM_LONG"
        if "SHORT" in raw:
            return "RECLAIM_SHORT"
        return "RECLAIM"

    if "LONG" in raw:
        return raw.replace(" ", "_")
    if "SHORT" in raw:
        return raw.replace(" ", "_")

    return raw.replace(" ", "_")


def build_event_key(setup: str, market_ctx: Dict[str, Any]) -> str:
    setup_name = _canonical_setup_name(setup)
    tags = mine_events(market_ctx)

    # çok fazla kombinasyon patlamasın diye en önemli 4 tag ile sınırlıyoruz
    key_tags = sorted(tags)[:4]
    return f"{setup_name}|{'|'.join(key_tags)}"


def _future_path_label(side: str, entry_price: float, future_candles: List[Dict[str, Any]], threshold_pct: float) -> Dict[str, Any]:
    if entry_price <= 0 or not future_candles:
        return {
            "result": "NO_DATA",
            "pnl": 0.0,
            "max_favorable_pct": 0.0,
            "max_adverse_pct": 0.0,
        }

    highs = [_safe_float(c.get("high")) for c in future_candles]
    lows = [_safe_float(c.get("low")) for c in future_candles]
    closes = [_safe_float(c.get("close")) for c in future_candles]

    max_high = max(highs) if highs else entry_price
    min_low = min(lows) if lows else entry_price
    final_close = closes[-1] if closes else entry_price

    if side == "SHORT":
        max_favorable = max(0.0, (entry_price - min_low) / entry_price)
        max_adverse = max(0.0, (max_high - entry_price) / entry_price)
        pnl = (entry_price - final_close) / entry_price
    else:
        max_favorable = max(0.0, (max_high - entry_price) / entry_price)
        max_adverse = max(0.0, (entry_price - min_low) / entry_price)
        pnl = (final_close - entry_price) / entry_price

    if max_favorable >= threshold_pct:
        result = "WIN"
    elif max_adverse >= threshold_pct:
        result = "LOSS"
    else:
        result = "NEUTRAL"

    return {
        "result": result,
        "pnl": pnl,
        "max_favorable_pct": max_favorable,
        "max_adverse_pct": max_adverse,
    }


def accumulate_event_stat(
    stats: Dict[str, Dict[str, Any]],
    event_key: str,
    pnl: float,
    result: str,
) -> None:
    row = stats.setdefault(
        event_key,
        {
            "n": 0,
            "wins": 0,
            "losses": 0,
            "neutrals": 0,
            "total_pnl": 0.0,
            "win_rate": 0.0,
            "avg_pnl": 0.0,
        },
    )

    row["n"] += 1
    row["total_pnl"] += pnl

    result_u = str(result or "").upper()
    if result_u == "WIN":
        row["wins"] += 1
    elif result_u == "LOSS":
        row["losses"] += 1
    else:
        row["neutrals"] += 1

    n = row["n"]
    row["win_rate"] = (row["wins"] / n) if n > 0 else 0.0
    row["avg_pnl"] = (row["total_pnl"] / n) if n > 0 else 0.0


def build_event_stats_from_candles(
    candles: List[Dict[str, Any]],
    setup: str,
    side: str,
    lookback: int = 30,
    lookahead: int = 12,
    threshold_pct: float = 0.02,
) -> Dict[str, Dict[str, Any]]:
    result: Dict[str, Dict[str, Any]] = {}
    side = _normalize_side(side)

    if side not in {"LONG", "SHORT"}:
        return result
    if not isinstance(candles, list) or len(candles) < lookback + lookahead + 1:
        return result

    for i in range(lookback, len(candles) - lookahead):
        ctx_candles = candles[: i + 1]
        current = candles[i]
        entry_price = _safe_float(current.get("close"))
        market_ctx = {
            "candles": ctx_candles,
            "last_price": entry_price,
            "tf": {},
        }

        event_key = build_event_key(setup, market_ctx)
        future = candles[i + 1 : i + 1 + lookahead]
        label = _future_path_label(side, entry_price, future, threshold_pct)

        accumulate_event_stat(
            stats=result,
            event_key=event_key,
            pnl=_safe_float(label["pnl"]),
            result=str(label["result"]),
        )

    return result