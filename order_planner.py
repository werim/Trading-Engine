from typing import Dict, Any, List
from utils import calc_rr, safe_float


def _unique_sorted_levels(levels: List[float]) -> List[float]:
    out = []
    seen = set()
    for x in levels:
        v = round(safe_float(x), 8)
        if v <= 0:
            continue
        if v in seen:
            continue
        seen.add(v)
        out.append(v)
    return sorted(out)


def _distance_pct(a: float, b: float) -> float:
    if b <= 0:
        return 999.0
    return abs(a - b) / b * 100.0


def build_order_plan(
    symbol: str,
    market_ctx: Dict[str, Any],
    regime: str,
    liq: Dict[str, Any],
    scenarios: List[Dict[str, Any]],
) -> List[Dict[str, Any]]:
    tf = market_ctx["tf"]

    r1 = str(tf["1H"]["regime"]).upper()
    r4 = str(tf["4H"]["regime"]).upper()
    rD = str(tf["1D"]["regime"]).upper()

    last_price = safe_float(market_ctx["last_price"])
    atr_1h = safe_float(tf["1H"].get("atr", 0.0))

    plans: List[Dict[str, Any]] = []

    if last_price <= 0:
        return plans

    # Çok uzak order üretme
    max_distance_pct = 1.6 if atr_1h > 0 else 1.2

    for s in scenarios:
        name = str(s.get("name", "")).upper()
        side = str(s.get("side", "")).upper()
        prob = safe_float(s.get("probability", 0.0))
        levels = _unique_sorted_levels(s.get("entry_zone", []))

        if prob < 0.50:
            continue

        # Counter-trend sert yasak
        if side == "SHORT" and rD == "LONG":
            continue
        if side == "LONG" and rD == "SHORT":
            continue

        # 4H karşıysa daha da sert davran
        if side == "SHORT" and r4 == "LONG":
            continue
        if side == "LONG" and r4 == "SHORT":
            continue

        for entry in levels[:2]:
            dist_pct = _distance_pct(entry, last_price)
            if dist_pct > max_distance_pct:
                continue

            if atr_1h > 0:
                zone_pad = atr_1h * 0.12
                sl_pad = atr_1h * 0.85
            else:
                zone_pad = entry * 0.0015
                sl_pad = entry * 0.0065

            score_bonus = 0
            size_mult = 1.0
            setup_type = "REGIME_SCENARIO"
            setup_reason = name

            if name in {"TREND_CONTINUATION_LONG", "TREND_CONTINUATION_SHORT"}:
                score_bonus += 4
                size_mult = 1.0
            elif name in {"BREAKOUT_LONG", "BREAKDOWN_SHORT"}:
                score_bonus += 3
                size_mult = 0.90
            elif name in {"PULLBACK_LONG", "PULLBACK_SHORT"}:
                score_bonus += 2
                size_mult = 0.80
            elif name in {"RANGE_LOWER_BOUNCE", "RANGE_UPPER_REJECT"}:
                score_bonus += 0
                size_mult = 0.45
            else:
                continue

            if side == "LONG":
                entry_zone_low = entry - zone_pad
                entry_zone_high = entry + zone_pad * 0.35
                sl = entry - sl_pad
                rr_target = 2.2 if "RANGE_" in name else 2.8
                tp = entry + (entry - sl) * rr_target
            else:
                entry_zone_low = entry - zone_pad * 0.35
                entry_zone_high = entry + zone_pad
                sl = entry + sl_pad
                rr_target = 2.2 if "RANGE_" in name else 2.8
                tp = entry - (sl - entry) * rr_target

            rr = calc_rr(entry, sl, tp, side)

            # Kalitesiz geometriyi çöpe at
            if rr < 2.0:
                continue

            # Range setup'larda 1H ters ise at
            if name == "RANGE_LOWER_BOUNCE" and r1 == "SHORT":
                continue
            if name == "RANGE_UPPER_REJECT" and r1 == "LONG":
                continue

            plans.append({
                "side": side,
                "entry": entry,
                "entry_zone_low": entry_zone_low,
                "entry_zone_high": entry_zone_high,
                "entry_trigger": entry,
                "sl": sl,
                "tp": tp,
                "rr": rr,
                "setup_type": setup_type,
                "setup_reason": setup_reason,
                "scenario_name": name,
                "scenario_probability": prob,
                "score_bonus": score_bonus,
                "size_mult": size_mult,
            })

    plans.sort(
        key=lambda p: (
            safe_float(p.get("scenario_probability", 0.0)),
            safe_float(p.get("rr", 0.0)),
            safe_float(p.get("score_bonus", 0.0)),
        ),
        reverse=True,
    )
    return plans