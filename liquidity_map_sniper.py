from typing import Dict, List, Any
from utils import safe_float


def _dedupe_levels(levels: List[float], min_gap_ratio: float = 0.0015) -> List[float]:
    clean: List[float] = []
    for raw in sorted([safe_float(x) for x in levels if safe_float(x) > 0]):
        if not clean:
            clean.append(raw)
            continue
        prev = clean[-1]
        gap_ratio = abs(raw - prev) / prev if prev > 0 else 0.0
        if gap_ratio >= min_gap_ratio:
            clean.append(raw)
    return clean


def _atr_or_fallback(price: float, atr: float) -> float:
    if atr > 0:
        return atr
    if price > 0:
        return price * 0.006
    return 0.0


def _build_long_pullback_levels(price: float, atr: float, ema20: float, ema50: float) -> List[float]:
    levels: List[float] = []
    if ema20 > 0:
        levels.append(ema20)
        levels.append(ema20 - atr * 0.20)
    if ema50 > 0:
        levels.append(ema50)
        levels.append(ema50 - atr * 0.15)
    if price > 0:
        levels.append(price - atr * 0.35)
        levels.append(price - atr * 0.55)
    return [x for x in levels if x > 0]


def _build_short_pullback_levels(price: float, atr: float, ema20: float, ema50: float) -> List[float]:
    levels: List[float] = []
    if ema20 > 0:
        levels.append(ema20)
        levels.append(ema20 + atr * 0.20)
    if ema50 > 0:
        levels.append(ema50)
        levels.append(ema50 + atr * 0.15)
    if price > 0:
        levels.append(price + atr * 0.35)
        levels.append(price + atr * 0.55)
    return [x for x in levels if x > 0]


def _build_breakout_above(price: float, atr: float, ema20: float) -> List[float]:
    levels: List[float] = []
    if price > 0:
        levels.append(price + atr * 0.18)
        levels.append(price + atr * 0.35)
    if ema20 > 0 and price > ema20:
        levels.append(max(price, ema20) + atr * 0.22)
    return [x for x in levels if x > 0]


def _build_breakout_below(price: float, atr: float, ema20: float) -> List[float]:
    levels: List[float] = []
    if price > 0:
        levels.append(price - atr * 0.18)
        levels.append(price - atr * 0.35)
    if ema20 > 0 and price < ema20:
        levels.append(min(price, ema20) - atr * 0.22)
    return [x for x in levels if x > 0]


def build_liquidity_map(symbol: str, market_ctx: Dict[str, Any]) -> Dict[str, Any]:
    tf = market_ctx["tf"]

    t1 = tf["1H"]
    t4 = tf["4H"]
    td = tf["1D"]

    price = safe_float(market_ctx.get("last_price"))
    spread_pct = safe_float(market_ctx.get("spread_pct"))

    ema20_1h = safe_float(t1.get("ema20"))
    ema50_1h = safe_float(t1.get("ema50"))
    atr_1h = _atr_or_fallback(price, safe_float(t1.get("atr")))

    ema20_4h = safe_float(t4.get("ema20"))
    ema50_4h = safe_float(t4.get("ema50"))
    ema20_1d = safe_float(td.get("ema20"))
    ema50_1d = safe_float(td.get("ema50"))

    resting_liquidity_below = _build_long_pullback_levels(
        price=price,
        atr=atr_1h,
        ema20=ema20_1h if ema20_1h > 0 else ema20_4h,
        ema50=ema50_1h if ema50_1h > 0 else ema50_4h,
    )

    resting_liquidity_above = _build_short_pullback_levels(
        price=price,
        atr=atr_1h,
        ema20=ema20_1h if ema20_1h > 0 else ema20_4h,
        ema50=ema50_1h if ema50_1h > 0 else ema50_4h,
    )

    trap_long_zone: List[float] = []
    trap_short_zone: List[float] = []

    if price > 0:
        trap_long_zone.extend([
            price - atr_1h * 0.22,
            price - atr_1h * 0.38,
        ])
        trap_short_zone.extend([
            price + atr_1h * 0.22,
            price + atr_1h * 0.38,
        ])

    if ema20_1h > 0:
        trap_long_zone.append(min(price, ema20_1h))
        trap_short_zone.append(max(price, ema20_1h))

    if ema50_1h > 0:
        trap_long_zone.append(min(price, ema50_1h))
        trap_short_zone.append(max(price, ema50_1h))

    breakout_above = _build_breakout_above(price, atr_1h, ema20_1h)
    breakout_below = _build_breakout_below(price, atr_1h, ema20_1h)

    reclaim_levels: List[float] = []
    for lvl in [ema20_1h, ema50_1h, ema20_4h, ema50_4h, ema20_1d, ema50_1d]:
        if lvl > 0:
            reclaim_levels.append(lvl)

    max_distance_ratio = 0.02 if spread_pct < 0.10 else 0.015

    def _clip(levels: List[float]) -> List[float]:
        out: List[float] = []
        for x in levels:
            if price <= 0:
                continue
            if abs(x - price) / price <= max_distance_ratio:
                out.append(x)
        return _dedupe_levels(out)

    return {
        "symbol": symbol,
        "atr_1h": atr_1h,
        "resting_liquidity_below": _clip(resting_liquidity_below),
        "resting_liquidity_above": _clip(resting_liquidity_above),
        "trap_long_zone": _clip(trap_long_zone),
        "trap_short_zone": _clip(trap_short_zone),
        "breakout_above": _clip(breakout_above),
        "breakout_below": _clip(breakout_below),
        "reclaim_levels": _clip(reclaim_levels),
    }
