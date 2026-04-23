from typing import Dict, List, Optional


_EVENT_STATS: Dict[str, Dict] = {}


def load_event_stats(data: Dict[str, Dict]) -> None:
    global _EVENT_STATS
    _EVENT_STATS = data or {}


def get_event_stats(event_key: str) -> Optional[Dict]:
    return _EVENT_STATS.get(event_key)


def mine_events(market_ctx: Dict) -> List[str]:
    events: List[str] = []

    candles = market_ctx.get("candles", [])
    last_price = float(market_ctx.get("last_price", 0.0) or 0.0)

    if len(candles) < 20 or last_price <= 0:
        return ["normal"]

    closes = [float(c.get("close", 0.0) or 0.0) for c in candles]
    volumes = [float(c.get("volume", 0.0) or 0.0) for c in candles]
    highs = [float(c.get("high", 0.0) or 0.0) for c in candles]
    lows = [float(c.get("low", 0.0) or 0.0) for c in candles]

    recent_closes = closes[-20:]
    recent_volumes = volumes[-20:]
    recent_highs = highs[-20:]
    recent_lows = lows[-20:]

    avg_vol = sum(recent_volumes) / len(recent_volumes)
    if avg_vol > 0 and recent_volumes[-1] > avg_vol * 1.5:
        events.append("volume_spike")

    recent_high = max(recent_closes[:-1]) if len(recent_closes) > 1 else recent_closes[-1]
    recent_low = min(recent_closes[:-1]) if len(recent_closes) > 1 else recent_closes[-1]

    if closes[-1] > recent_high:
        events.append("breakout_up")
    elif closes[-1] < recent_low:
        events.append("breakout_down")

    ranges = [h - l for h, l in zip(recent_highs, recent_lows)]
    avg_range = sum(ranges) / len(ranges)
    last_range = ranges[-1]

    if avg_range > 0 and last_range > avg_range * 1.5:
        events.append("volatility_burst")

    if last_price > 0 and avg_range / last_price < 0.01:
        events.append("range_compression")

    return sorted(events) if events else ["normal"]


def build_event_key(setup: str, market_ctx: Dict) -> str:
    events = mine_events(market_ctx)
    return f"{setup}|{'|'.join(events)}"