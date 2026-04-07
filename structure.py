from __future__ import annotations

import math
from dataclasses import dataclass
from typing import List, Optional, Tuple

import pandas as pd
import config


def cfg(name: str, default):
    return getattr(config, name, default)


USE_4H_FILTER = cfg("USE_4H_FILTER", False)
USE_1D_FILTER = cfg("USE_1D_FILTER", True)
USE_1W_FILTER = cfg("USE_1W_FILTER", True)

MIN_SIGNAL_SCORE = cfg("MIN_SIGNAL_SCORE", 2)
BLOCK_RANGE_TREND = cfg("BLOCK_RANGE_TREND", True)
STRICT_TREND_ALIGNMENT = cfg("STRICT_TREND_ALIGNMENT", False)

MIN_ATR_PCT = cfg("MIN_ATR_PCT", 0.004)
MIN_PRICE = cfg("MIN_PRICE", 0.000001)

MAX_SL_DISTANCE_PCT = cfg("MAX_SL_DISTANCE_PCT", 5.5)
MIN_TP_DISTANCE_PCT = cfg("MIN_TP_DISTANCE_PCT", 1.2)

REGRESSION_FLAT_ANGLE = cfg("REGRESSION_FLAT_ANGLE", 0.9)
REGRESSION_TREND_ANGLE = cfg("REGRESSION_TREND_ANGLE", 2.2)

SL_ATR_BUFFER_MULT = cfg("SL_ATR_BUFFER_MULT", 0.20)
TP_RR_MULT = cfg("TP_RR_MULT", 1.8)

ZONE_LOOKBACK_1H = cfg("ZONE_LOOKBACK_1H", 120)
ZONE_PIVOT_LEFT = cfg("ZONE_PIVOT_LEFT", 2)
ZONE_PIVOT_RIGHT = cfg("ZONE_PIVOT_RIGHT", 2)
ZONE_CLUSTER_ATR_MULT = cfg("ZONE_CLUSTER_ATR_MULT", 0.35)
ZONE_MIN_TOUCHES = cfg("ZONE_MIN_TOUCHES", 2)
ZONE_MAX_COUNT = cfg("ZONE_MAX_COUNT", 8)

PULLBACK_NEAR_ZONE_ATR_MULT = cfg("PULLBACK_NEAR_ZONE_ATR_MULT", 0.30)
BREAKOUT_BODY_ATR_MULT = cfg("BREAKOUT_BODY_ATR_MULT", 0.25)
RETEST_ZONE_ATR_MULT = cfg("RETEST_ZONE_ATR_MULT", 0.35)

ENTRY_CONFIRMATION_BUFFER_ATR_MULT = cfg("ENTRY_CONFIRMATION_BUFFER_ATR_MULT", 0.05)
BREAKOUT_CONFIRMATION_BUFFER_ATR_MULT = cfg("BREAKOUT_CONFIRMATION_BUFFER_ATR_MULT", 0.05)

USE_VOLUME_CONFIRMATION = cfg("USE_VOLUME_CONFIRMATION", False)
VOLUME_SPIKE_MULT = cfg("VOLUME_SPIKE_MULT", 1.20)

REQUIRED_COLUMNS = {"open", "high", "low", "close"}


@dataclass
class Zone:
    kind: str
    low: float
    high: float
    center: float
    touches: int
    strength: float

    def contains(self, price: float) -> bool:
        return self.low <= price <= self.high


@dataclass
class Setup:
    setup_type: str
    side: str
    entry_zone_low: float
    entry_zone_high: float
    entry_trigger: float
    sl: float
    tp: float
    ref_zone: Zone
    target_zone: Optional[Zone]
    score: int
    tf_context: str
    reason: str


def _validate_df(df: pd.DataFrame, name: str) -> pd.DataFrame:
    if df is None or df.empty:
        raise ValueError(f"{name} is empty")

    missing = REQUIRED_COLUMNS - set(df.columns)
    if missing:
        raise ValueError(f"{name} missing columns: {missing}")

    out = df.copy()
    for col in ["open", "high", "low", "close"]:
        out[col] = pd.to_numeric(out[col], errors="coerce")

    if "volume" in out.columns:
        out["volume"] = pd.to_numeric(out["volume"], errors="coerce").fillna(0.0)
    else:
        out["volume"] = 0.0

    out = out.dropna(subset=["open", "high", "low", "close"]).reset_index(drop=True)

    if len(out) < 30:
        raise ValueError(f"{name} too short")

    return out


def round_price(price: float, precision: int) -> float:
    return round(float(price), int(precision))


def build_tf_context(entry_tf: str, structure_tf: str, trend_tf: str, macro_tf: str) -> str:
    return f"1H={entry_tf}|4H={structure_tf}|1D={trend_tf}|1W={macro_tf}"


def calculate_signal_score(entry_side: str, structure_tf: str, trend_tf: str, macro_tf: str) -> int:
    score = 1
    if USE_4H_FILTER and structure_tf == entry_side:
        score += 1
    if USE_1D_FILTER and trend_tf == entry_side:
        score += 1
    if USE_1W_FILTER and macro_tf == entry_side:
        score += 1
    return score


def mtf_alignment_ok(entry_side: str, structure_tf: str, trend_tf: str, macro_tf: str) -> bool:
    if USE_4H_FILTER and structure_tf not in (entry_side, "RANGE"):
        return False
    if USE_1D_FILTER and trend_tf not in (entry_side, "RANGE"):
        return False
    if USE_1W_FILTER and macro_tf not in (entry_side, "RANGE"):
        return False
    return True


def calc_atr_df(df: pd.DataFrame, period: int = 14) -> Optional[float]:
    if len(df) < period + 1:
        return None

    high = df["high"]
    low = df["low"]
    close = df["close"]
    prev_close = close.shift(1)

    tr = pd.concat(
        [
            (high - low),
            (high - prev_close).abs(),
            (low - prev_close).abs(),
        ],
        axis=1,
    ).max(axis=1)

    atr = tr.rolling(period).mean().iloc[-1]
    if pd.isna(atr):
        return None
    return float(atr)


def linear_regression_slope(values: List[float], length: int = 100) -> Optional[float]:
    if length < 2 or len(values) < length:
        return None

    y = values[-length:]
    x = list(range(length))

    x_mean = sum(x) / length
    y_mean = sum(y) / length

    numerator = sum((x[i] - x_mean) * (y[i] - y_mean) for i in range(length))
    denominator = sum((x[i] - x_mean) ** 2 for i in range(length))

    if denominator == 0:
        return None

    return numerator / denominator


def regression_angle_deg(values: List[float], length: int = 100, normalize: bool = True) -> Optional[float]:
    if len(values) < length:
        return None

    series = values[-length:]

    if normalize:
        base = series[0]
        if base == 0:
            return None
        series = [((v / base) - 1.0) * 100.0 for v in series]

    slope = linear_regression_slope(series, length=length)
    if slope is None:
        return None

    return math.degrees(math.atan(slope))


def detect_trend_df(df: pd.DataFrame) -> str:
    closes = df["close"].tolist()
    length = min(100, len(closes))
    angle = regression_angle_deg(closes, length=length, normalize=True)

    if angle is None:
        return "NONE"

    if angle >= REGRESSION_TREND_ANGLE:
        return "LONG"
    if angle <= -REGRESSION_TREND_ANGLE:
        return "SHORT"
    if -REGRESSION_FLAT_ANGLE <= angle <= REGRESSION_FLAT_ANGLE:
        return "RANGE"
    return "RANGE"


def detect_structure_df(df: pd.DataFrame, lookback: int = 8) -> str:
    if len(df) < lookback + 1:
        return "NONE"

    recent = df.tail(lookback + 1).reset_index(drop=True)

    highs = recent["high"].tolist()
    lows = recent["low"].tolist()
    closes = recent["close"].tolist()

    prev_high = max(highs[:-1])
    prev_low = min(lows[:-1])
    last_close = closes[-1]

    if last_close > prev_high:
        return "LONG"
    if last_close < prev_low:
        return "SHORT"

    if highs[-1] > highs[-2] and lows[-1] > lows[-2]:
        return "LONG"
    if highs[-1] < highs[-2] and lows[-1] < lows[-2]:
        return "SHORT"

    return "RANGE"


def _is_pivot_high(df: pd.DataFrame, idx: int, left: int, right: int) -> bool:
    if idx - left < 0 or idx + right >= len(df):
        return False

    val = df.at[idx, "high"]
    left_slice = df.loc[idx - left:idx - 1, "high"]
    right_slice = df.loc[idx + 1:idx + right, "high"]

    return val >= left_slice.max() and val >= right_slice.max()


def _is_pivot_low(df: pd.DataFrame, idx: int, left: int, right: int) -> bool:
    if idx - left < 0 or idx + right >= len(df):
        return False

    val = df.at[idx, "low"]
    left_slice = df.loc[idx - left:idx - 1, "low"]
    right_slice = df.loc[idx + 1:idx + right, "low"]

    return val <= left_slice.min() and val <= right_slice.min()


def find_pivots(df: pd.DataFrame, left: int = 2, right: int = 2) -> Tuple[List[float], List[float]]:
    highs: List[float] = []
    lows: List[float] = []

    for i in range(len(df)):
        if _is_pivot_high(df, i, left, right):
            highs.append(float(df.at[i, "high"]))
        if _is_pivot_low(df, i, left, right):
            lows.append(float(df.at[i, "low"]))

    return highs, lows


def cluster_levels(levels: List[float], atr: float, kind: str) -> List[Zone]:
    if not levels:
        return []

    width = max(atr * ZONE_CLUSTER_ATR_MULT, 1e-12)
    sorted_levels = sorted(levels)

    clusters: List[List[float]] = []
    current = [sorted_levels[0]]

    for lvl in sorted_levels[1:]:
        center = sum(current) / len(current)
        if abs(lvl - center) <= width:
            current.append(lvl)
        else:
            clusters.append(current)
            current = [lvl]
    clusters.append(current)

    zones: List[Zone] = []
    for cluster in clusters:
        center = sum(cluster) / len(cluster)
        low = center - width
        high = center + width
        touches = len(cluster)

        zones.append(
            Zone(
                kind=kind,
                low=float(low),
                high=float(high),
                center=float(center),
                touches=int(touches),
                strength=float(touches),
            )
        )

    zones = [z for z in zones if z.touches >= ZONE_MIN_TOUCHES]
    zones.sort(key=lambda z: (z.strength, z.touches), reverse=True)
    return zones[:ZONE_MAX_COUNT]


def build_zones(df_1h: pd.DataFrame, atr_1h: float) -> Tuple[List[Zone], List[Zone]]:
    recent = df_1h.tail(ZONE_LOOKBACK_1H).reset_index(drop=True)
    pivot_highs, pivot_lows = find_pivots(recent, ZONE_PIVOT_LEFT, ZONE_PIVOT_RIGHT)

    resistance_zones = cluster_levels(pivot_highs, atr_1h, "resistance")
    support_zones = cluster_levels(pivot_lows, atr_1h, "support")

    support_zones.sort(key=lambda z: z.center)
    resistance_zones.sort(key=lambda z: z.center)

    return support_zones, resistance_zones


def nearest_zone_below(price: float, zones: List[Zone]) -> Optional[Zone]:
    candidates = [z for z in zones if z.center <= price]
    if not candidates:
        return None
    return sorted(candidates, key=lambda z: price - z.center)[0]


def nearest_zone_above(price: float, zones: List[Zone]) -> Optional[Zone]:
    candidates = [z for z in zones if z.center >= price]
    if not candidates:
        return None
    return sorted(candidates, key=lambda z: z.center - price)[0]


def next_zone_above(price: float, zones: List[Zone]) -> Optional[Zone]:
    candidates = [z for z in zones if z.center > price]
    if not candidates:
        return None
    return sorted(candidates, key=lambda z: z.center)[0]


def next_zone_below(price: float, zones: List[Zone]) -> Optional[Zone]:
    candidates = [z for z in zones if z.center < price]
    if not candidates:
        return None
    return sorted(candidates, key=lambda z: z.center, reverse=True)[0]


def candle_body(c: pd.Series) -> float:
    return abs(float(c["close"]) - float(c["open"]))


def is_bullish_rejection(c: pd.Series) -> bool:
    body = candle_body(c)
    lower_wick = min(float(c["open"]), float(c["close"])) - float(c["low"])
    upper_wick = float(c["high"]) - max(float(c["open"]), float(c["close"]))
    return float(c["close"]) > float(c["open"]) or lower_wick > max(body, upper_wick)


def is_bearish_rejection(c: pd.Series) -> bool:
    body = candle_body(c)
    upper_wick = float(c["high"]) - max(float(c["open"]), float(c["close"]))
    lower_wick = min(float(c["open"]), float(c["close"])) - float(c["low"])
    return float(c["close"]) < float(c["open"]) or upper_wick > max(body, lower_wick)


def volume_confirmed(df: pd.DataFrame) -> bool:
    if not USE_VOLUME_CONFIRMATION or "volume" not in df.columns or len(df) < 20:
        return True

    current = float(df.iloc[-1]["volume"])
    avg = float(df.iloc[-20:-1]["volume"].mean())

    if avg <= 0:
        return True

    return current >= avg * VOLUME_SPIKE_MULT


def detect_pullback_setup(
    trend_side: str,
    df_15m: pd.DataFrame,
    support_zones: List[Zone],
    resistance_zones: List[Zone],
    atr_15m: float,
    score: int,
    tf_context: str,
) -> Tuple[Optional[Setup], str]:
    last = df_15m.iloc[-1]
    prev = df_15m.iloc[-2]

    last_close = float(last["close"])
    last_high = float(last["high"])
    last_low = float(last["low"])

    near_dist = atr_15m * PULLBACK_NEAR_ZONE_ATR_MULT

    if trend_side == "LONG":
        zone = nearest_zone_below(last_close, support_zones)
        if zone is None:
            return None, "PULLBACK_NO_SUPPORT_ZONE"

        touched = last_low <= zone.high + near_dist
        confirmed = is_bullish_rejection(last) or float(last["close"]) > float(prev["high"])

        if not touched:
            return None, "PULLBACK_LONG_NOT_TOUCHED"
        if not confirmed:
            return None, "PULLBACK_LONG_NO_CONFIRM"

        entry_zone_low = zone.low
        entry_zone_high = zone.high
        entry_trigger = max(last_high, zone.center) + atr_15m * ENTRY_CONFIRMATION_BUFFER_ATR_MULT
        sl = zone.low - atr_15m * SL_ATR_BUFFER_MULT

        target = next_zone_above(entry_trigger, resistance_zones)
        if target is not None and target.center > entry_trigger:
            tp = target.center
        else:
            risk = entry_trigger - sl
            tp = entry_trigger + risk * TP_RR_MULT

        return (
            Setup(
                setup_type="PULLBACK",
                side="LONG",
                entry_zone_low=float(entry_zone_low),
                entry_zone_high=float(entry_zone_high),
                entry_trigger=float(entry_trigger),
                sl=float(sl),
                tp=float(tp),
                ref_zone=zone,
                target_zone=target,
                score=score,
                tf_context=tf_context,
                reason="PULLBACK_LONG",
            ),
            "OK",
        )

    if trend_side == "SHORT":
        zone = nearest_zone_above(last_close, resistance_zones)
        if zone is None:
            return None, "PULLBACK_NO_RESISTANCE_ZONE"

        touched = last_high >= zone.low - near_dist
        confirmed = is_bearish_rejection(last) or float(last["close"]) < float(prev["low"])

        if not touched:
            return None, "PULLBACK_SHORT_NOT_TOUCHED"
        if not confirmed:
            return None, "PULLBACK_SHORT_NO_CONFIRM"

        entry_zone_low = zone.low
        entry_zone_high = zone.high
        entry_trigger = min(last_low, zone.center) - atr_15m * ENTRY_CONFIRMATION_BUFFER_ATR_MULT
        sl = zone.high + atr_15m * SL_ATR_BUFFER_MULT

        target = next_zone_below(entry_trigger, support_zones)
        if target is not None and target.center < entry_trigger:
            tp = target.center
        else:
            risk = sl - entry_trigger
            tp = entry_trigger - risk * TP_RR_MULT

        return (
            Setup(
                setup_type="PULLBACK",
                side="SHORT",
                entry_zone_low=float(entry_zone_low),
                entry_zone_high=float(entry_zone_high),
                entry_trigger=float(entry_trigger),
                sl=float(sl),
                tp=float(tp),
                ref_zone=zone,
                target_zone=target,
                score=score,
                tf_context=tf_context,
                reason="PULLBACK_SHORT",
            ),
            "OK",
        )

    return None, "PULLBACK_TREND_INVALID"


def detect_breakout_retest_setup(
    trend_side: str,
    df_15m: pd.DataFrame,
    support_zones: List[Zone],
    resistance_zones: List[Zone],
    atr_15m: float,
    score: int,
    tf_context: str,
) -> Tuple[Optional[Setup], str]:
    if len(df_15m) < 4:
        return None, "BREAKOUT_TOO_SHORT"

    c1 = df_15m.iloc[-3]
    c2 = df_15m.iloc[-2]
    c3 = df_15m.iloc[-1]

    retest_width = atr_15m * RETEST_ZONE_ATR_MULT
    min_body = atr_15m * BREAKOUT_BODY_ATR_MULT

    if trend_side == "LONG":
        zone = nearest_zone_above(float(c1["close"]), resistance_zones)
        if zone is None:
            return None, "BREAKOUT_LONG_NO_RES_ZONE"

        broke = (
            float(c2["close"]) > zone.high
            and candle_body(c2) >= min_body
            and volume_confirmed(df_15m.iloc[:-1])
        )
        retested = float(c3["low"]) <= zone.high + retest_width
        confirmed = float(c3["close"]) > zone.center and is_bullish_rejection(c3)

        if not broke:
            return None, "BREAKOUT_LONG_NOT_BROKE"
        if not retested:
            return None, "BREAKOUT_LONG_NO_RETEST"
        if not confirmed:
            return None, "BREAKOUT_LONG_NO_CONFIRM"

        entry_zone_low = zone.low
        entry_zone_high = zone.high + retest_width
        entry_trigger = float(c3["high"]) + atr_15m * BREAKOUT_CONFIRMATION_BUFFER_ATR_MULT
        sl = zone.low - atr_15m * SL_ATR_BUFFER_MULT

        target = next_zone_above(entry_trigger, resistance_zones)
        if target is not None and target.center > entry_trigger:
            tp = target.center
        else:
            risk = entry_trigger - sl
            tp = entry_trigger + risk * TP_RR_MULT

        return (
            Setup(
                setup_type="BREAKOUT_RETEST",
                side="LONG",
                entry_zone_low=float(entry_zone_low),
                entry_zone_high=float(entry_zone_high),
                entry_trigger=float(entry_trigger),
                sl=float(sl),
                tp=float(tp),
                ref_zone=zone,
                target_zone=target,
                score=score,
                tf_context=tf_context,
                reason="BREAKOUT_RETEST_LONG",
            ),
            "OK",
        )

    if trend_side == "SHORT":
        zone = nearest_zone_below(float(c1["close"]), support_zones)
        if zone is None:
            return None, "BREAKOUT_SHORT_NO_SUP_ZONE"

        broke = (
            float(c2["close"]) < zone.low
            and candle_body(c2) >= min_body
            and volume_confirmed(df_15m.iloc[:-1])
        )
        retested = float(c3["high"]) >= zone.low - retest_width
        confirmed = float(c3["close"]) < zone.center and is_bearish_rejection(c3)

        if not broke:
            return None, "BREAKOUT_SHORT_NOT_BROKE"
        if not retested:
            return None, "BREAKOUT_SHORT_NO_RETEST"
        if not confirmed:
            return None, "BREAKOUT_SHORT_NO_CONFIRM"

        entry_zone_low = zone.low - retest_width
        entry_zone_high = zone.high
        entry_trigger = float(c3["low"]) - atr_15m * BREAKOUT_CONFIRMATION_BUFFER_ATR_MULT
        sl = zone.high + atr_15m * SL_ATR_BUFFER_MULT

        target = next_zone_below(entry_trigger, support_zones)
        if target is not None and target.center < entry_trigger:
            tp = target.center
        else:
            risk = sl - entry_trigger
            tp = entry_trigger - risk * TP_RR_MULT

        return (
            Setup(
                setup_type="BREAKOUT_RETEST",
                side="SHORT",
                entry_zone_low=float(entry_zone_low),
                entry_zone_high=float(entry_zone_high),
                entry_trigger=float(entry_trigger),
                sl=float(sl),
                tp=float(tp),
                ref_zone=zone,
                target_zone=target,
                score=score,
                tf_context=tf_context,
                reason="BREAKOUT_RETEST_SHORT",
            ),
            "OK",
        )

    return None, "BREAKOUT_TREND_INVALID"


def validate_geometry(side: str, entry_zone_low: float, entry_zone_high: float, entry_trigger: float, sl: float, tp: float) -> Tuple[bool, str]:
    if min(entry_zone_low, entry_zone_high, entry_trigger, sl, tp) <= 0:
        return False, "NON_POSITIVE_PRICE"

    if entry_zone_low > entry_zone_high:
        return False, "ENTRY_ZONE_INVALID"

    if side == "LONG":
        if not (sl < entry_zone_low <= entry_zone_high <= tp):
            return False, "LONG_ZONE_ORDER_INVALID"
        if entry_trigger < entry_zone_low:
            return False, "LONG_TRIGGER_BELOW_ZONE"
    elif side == "SHORT":
        if not (tp <= entry_zone_low <= entry_zone_high < sl):
            return False, "SHORT_ZONE_ORDER_INVALID"
        if entry_trigger > entry_zone_high:
            return False, "SHORT_TRIGGER_ABOVE_ZONE"
    else:
        return False, "INVALID_SIDE"

    return True, "OK"


def validate_distances(entry_trigger: float, sl: float, tp: float) -> Tuple[bool, str]:
    sl_distance_pct = abs((entry_trigger - sl) / entry_trigger) * 100.0
    tp_distance_pct = abs((tp - entry_trigger) / entry_trigger) * 100.0

    if sl_distance_pct > MAX_SL_DISTANCE_PCT:
        return False, f"SL_DIST_FAIL sl_pct={sl_distance_pct:.2f}"

    if tp_distance_pct < MIN_TP_DISTANCE_PCT:
        return False, f"TP_DIST_FAIL tp_pct={tp_distance_pct:.2f}"

    return True, "OK"


def setup_to_order(setup: Setup, precision: int) -> Tuple[Optional[dict], str]:
    valid, reason = validate_geometry(
        setup.side,
        setup.entry_zone_low,
        setup.entry_zone_high,
        setup.entry_trigger,
        setup.sl,
        setup.tp,
    )
    if not valid:
        return None, reason

    valid, reason = validate_distances(setup.entry_trigger, setup.sl, setup.tp)
    if not valid:
        return None, reason

    entry_zone_low = round_price(setup.entry_zone_low, precision)
    entry_zone_high = round_price(setup.entry_zone_high, precision)
    entry_trigger = round_price(setup.entry_trigger, precision)
    sl = round_price(setup.sl, precision)
    tp = round_price(setup.tp, precision)

    rr = abs((tp - entry_trigger) / (entry_trigger - sl)) if abs(entry_trigger - sl) > 1e-12 else 0.0

    order = {
        "symbol": "",
        "side": setup.side,
        "entry_zone_low": entry_zone_low,
        "entry_zone_high": entry_zone_high,
        "entry_trigger": entry_trigger,
        "sl": sl,
        "tp": tp,
        "rr": round(rr, 2),
        "score": setup.score,
        "tf_context": setup.tf_context,
        "setup_type": setup.setup_type,
        "setup_reason": setup.reason,
        "zone_low": round_price(setup.ref_zone.low, precision),
        "zone_high": round_price(setup.ref_zone.high, precision),
        "zone_center": round_price(setup.ref_zone.center, precision),
    }
    return order, "OK"


def evaluate_symbol(
    symbol: str,
    precision: int,
    df_1h_raw: pd.DataFrame,
    df_15m_raw: pd.DataFrame,
    min_score_override: Optional[int] = None,
):
    try:
        df_1h = _validate_df(df_1h_raw, "df_1h_raw")
        df_15m = _validate_df(df_15m_raw, "df_15m_raw")
    except Exception as e:
        return None, f"DATAFRAME_FAIL {e}"

    live_price = float(df_15m.iloc[-1]["close"])
    if live_price < MIN_PRICE:
        return None, f"MIN_PRICE_FAIL live={live_price}"

    atr_1h = calc_atr_df(df_1h, 14)
    atr_15m = calc_atr_df(df_15m, 14)

    if atr_1h is None or atr_15m is None:
        return None, "ATR_NONE"

    atr_pct = atr_1h / live_price if live_price > 0 else 0.0
    if atr_pct < MIN_ATR_PCT:
        return None, f"ATR_PCT_FAIL atr_pct={atr_pct:.5f} min={MIN_ATR_PCT}"

    entry_bias = detect_trend_df(df_1h)
    structure_bias = detect_structure_df(df_1h)

    # Şimdilik sadece 1H ve 15M geldiği için fallback
    trend_bias = detect_trend_df(df_1h.tail(min(100, len(df_1h))))
    macro_bias = detect_trend_df(df_1h.tail(min(100, len(df_1h))))

    if entry_bias not in ("LONG", "SHORT"):
        return None, f"ENTRY_BIAS_{entry_bias}"

    if BLOCK_RANGE_TREND and trend_bias == "RANGE":
        return None, f"BLOCK_RANGE_TREND trend={trend_bias}"

    if STRICT_TREND_ALIGNMENT and trend_bias != entry_bias:
        return None, f"STRICT_ALIGNMENT_FAIL entry={entry_bias} trend={trend_bias}"

    if not mtf_alignment_ok(entry_bias, structure_bias, trend_bias, macro_bias):
        return None, (
            f"MTF_FAIL entry={entry_bias} "
            f"structure={structure_bias} trend={trend_bias} macro={macro_bias}"
        )

    score = calculate_signal_score(entry_bias, structure_bias, trend_bias, macro_bias)
    min_score = min_score_override if min_score_override is not None else MIN_SIGNAL_SCORE
    if score < min_score:
        return None, f"SCORE_FAIL score={score} min={min_score}"

    tf_context = build_tf_context(entry_bias, structure_bias, trend_bias, macro_bias)

    support_zones, resistance_zones = build_zones(df_1h, atr_1h)
    if not support_zones and not resistance_zones:
        return None, "NO_ZONES"

    pullback_setup, pullback_reason = detect_pullback_setup(
        trend_side=entry_bias,
        df_15m=df_15m,
        support_zones=support_zones,
        resistance_zones=resistance_zones,
        atr_15m=atr_15m,
        score=score,
        tf_context=tf_context,
    )

    if pullback_setup is not None:
        order, reason = setup_to_order(pullback_setup, precision)
        if order is not None:
            order["symbol"] = symbol
            order["live_price"] = round_price(live_price, precision)
            return order, f"OK {pullback_setup.reason}"
        return None, reason

    breakout_setup, breakout_reason = detect_breakout_retest_setup(
        trend_side=entry_bias,
        df_15m=df_15m,
        support_zones=support_zones,
        resistance_zones=resistance_zones,
        atr_15m=atr_15m,
        score=score,
        tf_context=tf_context,
    )

    if breakout_setup is not None:
        order, reason = setup_to_order(breakout_setup, precision)
        if order is not None:
            order["symbol"] = symbol
            order["live_price"] = round_price(live_price, precision)
            return order, f"OK {breakout_setup.reason}"
        return None, reason

    if "PULLBACK" in pullback_reason:
        return None, pullback_reason
    return None, breakout_reason