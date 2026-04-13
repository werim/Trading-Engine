from typing import Any, Dict, List, Optional, Tuple

from binance_real import BinanceFuturesClient
from config import CONFIG
from utils import (
    compute_rr,
    expected_net_pnl_pct,
    price_distance_pct,
    safe_float,
    stop_net_loss_pct,
)

client = BinanceFuturesClient()


def ema(values: List[float], period: int) -> List[float]:
    if not values:
        return []
    if len(values) < period:
        return values[:]

    k = 2 / (period + 1)
    out = [values[0]]
    for v in values[1:]:
        out.append((v * k) + (out[-1] * (1 - k)))
    return out


def true_range(high: float, low: float, prev_close: float) -> float:
    return max(high - low, abs(high - prev_close), abs(low - prev_close))


def atr(candles: List[Dict[str, float]], period: int) -> float:
    if len(candles) < period + 1:
        return 0.0
    trs = []
    for i in range(1, len(candles)):
        trs.append(
            true_range(
                candles[i]["high"],
                candles[i]["low"],
                candles[i - 1]["close"],
            )
        )
    if len(trs) < period:
        return 0.0
    return sum(trs[-period:]) / period


def parse_klines(raw: List[List[Any]]) -> List[Dict[str, float]]:
    candles = []
    for row in raw:
        candles.append(
            {
                "open": float(row[1]),
                "high": float(row[2]),
                "low": float(row[3]),
                "close": float(row[4]),
                "volume": float(row[5]),
            }
        )
    return candles


def get_tf_trend(symbol: str, interval: str) -> str:
    raw = client.get_klines(symbol, interval, CONFIG.TRADE.KLINE_LIMIT)
    candles = parse_klines(raw)
    closes = [c["close"] for c in candles]
    if len(closes) < CONFIG.TRADE.EMA_SLOW:
        return "RANGE"

    e20 = ema(closes, CONFIG.TRADE.EMA_FAST)[-1]
    e50 = ema(closes, CONFIG.TRADE.EMA_MID)[-1]
    e200 = ema(closes, CONFIG.TRADE.EMA_SLOW)[-1]
    last = closes[-1]

    if last > e20 > e50 > e200:
        return "LONG"
    if last < e20 < e50 < e200:
        return "SHORT"
    return "RANGE"


def tf_context(symbol: str) -> str:
    t1 = get_tf_trend(symbol, CONFIG.TRADE.LTF_INTERVAL)
    t4 = get_tf_trend(symbol, CONFIG.TRADE.MTF_INTERVAL)
    tD = get_tf_trend(symbol, CONFIG.TRADE.HTF_INTERVAL)
    return f"1H={t1}|4H={t4}|1D={tD}"


def extract_tf(context: str) -> Tuple[str, str, str]:
    parts = context.split("|")
    t1 = parts[0].split("=")[1]
    t4 = parts[1].split("=")[1]
    tD = parts[2].split("=")[1]
    return t1, t4, tD


def regime_name(tD: str) -> str:
    if tD == "RANGE":
        return "RANGE"
    return "TREND"


def score_setup(
    t1: str,
    t4: str,
    tD: str,
    rr: float,
    spread_pct: float,
    funding_rate_pct: float,
    setup_type: str,
) -> int:
    score = 0

    if t1 == t4:
        score += 2
    if t4 == tD:
        score += 2
    if rr >= CONFIG.TRADE.RR_IDEAL:
        score += 1
    if spread_pct <= CONFIG.TRADE.MAX_SPREAD_PCT * 0.5:
        score += 1
    if funding_rate_pct <= CONFIG.TRADE.MAX_FUNDING_ABS_PCT * 0.5:
        score += 1

    if setup_type == "PULLBACK":
        score += 1

    return score


def filter_regime(
    t1: str,
    t4: str,
    tD: str,
    setup_type: str,
    rr: float,
    exp_net: float,
    score: int,
) -> Tuple[bool, str]:
    if not CONFIG.TRADE.ENABLE_REGIME_FILTER:
        return True, "REGIME_FILTER_DISABLED"

    if tD != "RANGE":
        if score < CONFIG.TRADE.SCORE_MIN:
            return False, "TREND_SCORE_TOO_LOW"
        if rr < CONFIG.TRADE.RR_MIN:
            return False, "TREND_RR_TOO_LOW"
        if exp_net < CONFIG.TRADE.MIN_EXPECTED_NET_PNL_PCT:
            return False, "TREND_EXPECTED_NET_TOO_LOW"
        return True, "TREND_OK"

    # Daily range logic
    if CONFIG.TRADE.SKIP_RANGE_DAILY:
        return False, "RANGE_BLOCKED_BY_SKIP_RANGE_DAILY"

    if setup_type == "BREAKOUT" and not CONFIG.TRADE.ALLOW_RANGE_BREAKOUTS:
        return False, "RANGE_BREAKOUT_BLOCKED"

    if setup_type == "PULLBACK" and not CONFIG.TRADE.ALLOW_RANGE_PULLBACKS:
        return False, "RANGE_PULLBACK_BLOCKED"

    if score < CONFIG.TRADE.RANGE_SCORE_MIN:
        return False, "RANGE_SCORE_TOO_LOW"

    if rr < CONFIG.TRADE.RANGE_RR_MIN:
        return False, "RANGE_RR_TOO_LOW"

    if exp_net < CONFIG.TRADE.RANGE_MIN_EXPECTED_NET_PNL_PCT:
        return False, "RANGE_EXPECTED_NET_TOO_LOW"

    # In range, prefer 1H and 4H alignment at least
    if t1 != t4:
        return False, "RANGE_LTF_MTF_MISMATCH"

    return True, "RANGE_OK"


def get_setup(symbol: str, market_snapshot: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    raw = client.get_klines(symbol, CONFIG.TRADE.LTF_INTERVAL, CONFIG.TRADE.KLINE_LIMIT)
    candles = parse_klines(raw)
    if len(candles) < 80:
        return None

    closes = [c["close"] for c in candles]
    highs = [c["high"] for c in candles]
    lows = [c["low"] for c in candles]
    last = closes[-1]
    prev = closes[-2]
    current_atr = atr(candles, CONFIG.TRADE.ATR_PERIOD)
    if current_atr <= 0:
        return None

    context = tf_context(symbol)
    t1, t4, tD = extract_tf(context)

    spread_pct = safe_float(market_snapshot.get("spread_pct"))
    funding_rate_pct = abs(safe_float(market_snapshot.get("funding_rate_pct")))

    if spread_pct > CONFIG.TRADE.MAX_SPREAD_PCT:
        return None
    if funding_rate_pct > CONFIG.TRADE.MAX_FUNDING_ABS_PCT:
        return None

    recent_high = max(highs[-CONFIG.TRADE.BREAKOUT_LOOKBACK:])
    recent_low = min(lows[-CONFIG.TRADE.BREAKOUT_LOOKBACK:])
    ema20 = ema(closes, CONFIG.TRADE.EMA_FAST)[-1]
    ema50 = ema(closes, CONFIG.TRADE.EMA_MID)[-1]

    entry = sl = tp = 0.0
    side = ""
    setup_type = ""
    reason = ""

    # Trend-following breakouts
    if t1 == "LONG" and t4 == "LONG" and tD == "LONG" and last > recent_high * 0.998:
        entry = last
        sl = entry - (current_atr * CONFIG.TRADE.SL_MULTIPLIER)
        tp = entry + (current_atr * 2.90)
        side = "LONG"
        setup_type = "BREAKOUT"
        reason = "BREAKOUT_LONG"

    elif t1 == "SHORT" and t4 == "SHORT" and tD == "SHORT" and last < recent_low * 1.002:
        entry = last
        sl = entry + (current_atr * CONFIG.TRADE.SL_MULTIPLIER)
        tp = entry - (current_atr * 2.90)
        side = "SHORT"
        setup_type = "BREAKOUT"
        reason = "BREAKOUT_SHORT"

    # Trend pullbacks
    elif t4 == "LONG" and tD == "LONG" and last > ema50 and prev >= ema20 * 0.995 and last >= ema20:
        entry = ema20
        sl = entry - (current_atr * CONFIG.TRADE.SL_MULTIPLIER)
        tp = entry + (current_atr * 2.70)
        side = "LONG"
        setup_type = "PULLBACK"
        reason = "PULLBACK_LONG"

    elif t4 == "SHORT" and tD == "SHORT" and last < ema50 and prev <= ema20 * 1.005 and last <= ema20:
        entry = ema20
        sl = entry + (current_atr * CONFIG.TRADE.SL_MULTIPLIER)
        tp = entry - (current_atr * 2.70)
        side = "SHORT"
        setup_type = "PULLBACK"
        reason = "PULLBACK_SHORT"

    # Daily range pullbacks only
    elif tD == "RANGE" and CONFIG.TRADE.ALLOW_RANGE_PULLBACKS:
        if t4 == "LONG" and last > ema50 and prev >= ema20 * 0.996 and last >= ema20:
            entry = ema20
            sl = entry - (current_atr * CONFIG.TRADE.SL_MULTIPLIER)
            tp = entry + (current_atr * 2.20)
            side = "LONG"
            setup_type = "PULLBACK"
            reason = "RANGE_PULLBACK_LONG"

        elif t4 == "SHORT" and last < ema50 and prev <= ema20 * 1.004 and last <= ema20:
            entry = ema20
            sl = entry + (current_atr * CONFIG.TRADE.SL_MULTIPLIER)
            tp = entry - (current_atr * 2.20)
            side = "SHORT"
            setup_type = "PULLBACK"
            reason = "RANGE_PULLBACK_SHORT"

    # Optional range breakouts, off by default
    if not side and tD == "RANGE" and CONFIG.TRADE.ALLOW_RANGE_BREAKOUTS:
        if t1 == "LONG" and t4 == "LONG" and last > recent_high * 0.999:
            entry = last
            sl = entry - (current_atr * CONFIG.TRADE.SL_MULTIPLIER)
            tp = entry + (current_atr * 2.30)
            side = "LONG"
            setup_type = "BREAKOUT"
            reason = "RANGE_BREAKOUT_LONG"

        elif t1 == "SHORT" and t4 == "SHORT" and last < recent_low * 1.001:
            entry = last
            sl = entry + (current_atr * CONFIG.TRADE.SL_MULTIPLIER)
            tp = entry - (current_atr * 2.30)
            side = "SHORT"
            setup_type = "BREAKOUT"
            reason = "RANGE_BREAKOUT_SHORT"

    if not side:
        return None

    rr = compute_rr(entry, sl, tp, side)
    sl_pct = price_distance_pct(entry, sl)
    tp_pct = price_distance_pct(entry, tp)
    exp_net = expected_net_pnl_pct(entry, tp, side, CONFIG.TRADE.USE_LIMIT_ENTRY)
    stop_net = stop_net_loss_pct(entry, sl, side, CONFIG.TRADE.USE_LIMIT_ENTRY)

    if sl_pct < CONFIG.TRADE.MIN_STOP_PCT or sl_pct > CONFIG.TRADE.MAX_STOP_PCT:
        return None
    if tp_pct < CONFIG.TRADE.MIN_TP_PCT or tp_pct > CONFIG.TRADE.MAX_TP_PCT:
        return None

    score = score_setup(
        t1=t1,
        t4=t4,
        tD=tD,
        rr=rr,
        spread_pct=spread_pct,
        funding_rate_pct=funding_rate_pct,
        setup_type=setup_type,
    )

    regime_ok, regime_reason = filter_regime(
        t1=t1,
        t4=t4,
        tD=tD,
        setup_type=setup_type,
        rr=rr,
        exp_net=exp_net,
        score=score,
    )

    if not regime_ok:
        return None

    zone_half_atr = current_atr * (0.25 if setup_type == "PULLBACK" else 0.15)
    zone_low = entry - zone_half_atr
    zone_high = entry + zone_half_atr
    trigger = entry

    return {
        "symbol": symbol,
        "side": side,
        "entry_zone_low": zone_low,
        "entry_zone_high": zone_high,
        "entry_trigger": trigger,
        "sl": sl,
        "tp": tp,
        "rr": round(rr, 2),
        "score": score,
        "tf_context": context,
        "setup_type": setup_type,
        "setup_reason": reason,
        "regime": regime_name(tD),
        "regime_reason": regime_reason,
        "expected_net_pnl_pct": round(exp_net, 4),
        "stop_net_loss_pct": round(stop_net, 4),
        "spread_pct": round(spread_pct, 4),
        "funding_rate_pct": round(funding_rate_pct, 4),
    }


def rank_setups(setups: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    return sorted(
        setups,
        key=lambda x: (
            x.get("score", 0),
            x.get("expected_net_pnl_pct", 0),
            x.get("rr", 0),
        ),
        reverse=True,
    )