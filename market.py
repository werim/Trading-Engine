from __future__ import annotations

from typing import Any, Dict, List, Optional, Tuple
import math

import binance
from utils import safe_float


def ema(values: List[float], period: int) -> float:
    if not values:
        return 0.0
    if len(values) < period:
        return sum(values) / len(values)

    multiplier = 2 / (period + 1)
    ema_value = values[0]
    for v in values[1:]:
        ema_value = (v - ema_value) * multiplier + ema_value
    return ema_value


def rsi(values: List[float], period: int = 14) -> float:
    if len(values) < period + 1:
        return 50.0

    gains: List[float] = []
    losses: List[float] = []
    for i in range(1, len(values)):
        diff = values[i] - values[i - 1]
        gains.append(max(diff, 0.0))
        losses.append(abs(min(diff, 0.0)))

    avg_gain = sum(gains[-period:]) / period
    avg_loss = sum(losses[-period:]) / period

    if avg_loss == 0:
        return 100.0
    rs = avg_gain / avg_loss
    return 100 - (100 / (1 + rs))


def atr(klines: List[Dict[str, Any]], period: int = 14) -> float:
    if len(klines) < period + 1:
        return 0.0

    trs: List[float] = []
    for i in range(1, len(klines)):
        high = safe_float(klines[i]["high"])
        low = safe_float(klines[i]["low"])
        prev_close = safe_float(klines[i - 1]["close"])
        tr = max(high - low, abs(high - prev_close), abs(low - prev_close))
        trs.append(tr)

    return sum(trs[-period:]) / period if trs else 0.0


def build_indicators(klines: List[Dict[str, Any]]) -> Dict[str, Any]:
    closes = [safe_float(k["close"]) for k in klines]
    return {
        "ema20": ema(closes, 20),
        "ema50": ema(closes, 50),
        "rsi": rsi(closes, 14),
        "atr": atr(klines, 14),
        "last_close": closes[-1] if closes else 0.0,
    }


def detect_regime(indicators: Dict[str, Any]) -> str:
    ema20 = safe_float(indicators.get("ema20"))
    ema50 = safe_float(indicators.get("ema50"))
    last_close = safe_float(indicators.get("last_close"))

    if last_close > ema20 > ema50:
        return "LONG"
    if last_close < ema20 < ema50:
        return "SHORT"
    return "RANGE"


def get_book_snapshot(symbol: str) -> Dict[str, Any]:
    data = binance.get_book_ticker(symbol)
    bid = safe_float(data.get("bid"))
    ask = safe_float(data.get("ask"))
    mid = (bid + ask) / 2.0 if bid and ask else 0.0
    spread_pct = ((ask - bid) / mid * 100.0) if mid > 0 else 0.0
    return {
        "bid": bid,
        "ask": ask,
        "spread_pct": spread_pct,
    }


def get_tf_snapshot(symbol: str, interval: str, limit: int = 200) -> Dict[str, Any]:
    klines = binance.get_klines(symbol, interval, limit)
    indicators = build_indicators(klines)
    indicators["regime"] = detect_regime(indicators)
    return indicators


def _mean(values: List[float]) -> float:
    return sum(values) / len(values) if values else 0.0


def _std(values: List[float]) -> float:
    if len(values) < 2:
        return 0.0
    m = _mean(values)
    var = sum((x - m) ** 2 for x in values) / (len(values) - 1)
    return math.sqrt(max(var, 0.0))


def _safe_pct_change(a: float, b: float) -> float:
    if a <= 0:
        return 0.0
    return ((b - a) / a) * 100.0


def _log_returns(closes: List[float]) -> List[float]:
    out: List[float] = []
    for i in range(1, len(closes)):
        prev_c = max(closes[i - 1], 1e-12)
        curr_c = max(closes[i], 1e-12)
        out.append(math.log(curr_c / prev_c))
    return out


def _bucket_volume_zscore(z: float) -> str:
    if z >= 5.0:
        return "extreme"
    if z >= 3.0:
        return "high"
    return "normal"


def _bucket_return_1m(ret: float) -> str:
    if ret >= 1.0:
        return "sharp_up"
    if ret <= -1.0:
        return "sharp_down"
    return "mild"


def _get_recent_1m_klines(symbol: str, limit: int = 240) -> List[Dict[str, Any]]:
    rows = binance.get_klines(symbol, "1m", limit)
    out: List[Dict[str, Any]] = []
    for row in rows:
        out.append(
            {
                "open_time": int(row.get("open_time", 0) or 0),
                "open": safe_float(row.get("open")),
                "high": safe_float(row.get("high")),
                "low": safe_float(row.get("low")),
                "close": safe_float(row.get("close")),
                "volume": safe_float(row.get("volume")),
            }
        )
    return out


def _detect_market_event_from_1m(symbol: str, klines: List[Dict[str, Any]]) -> Dict[str, Any]:
    if len(klines) < 40:
        return {
            "event_type": "",
            "direction": "NEUTRAL",
            "volume_bucket": "normal",
            "return_bucket": "mild",
            "winrate": 0.0,
            "sample_size": 0,
            "recent": 0,
            "volume_zscore": 0.0,
            "return_1m_pct": 0.0,
            "range_pct": 0.0,
            "realized_vol_15m": 0.0,
            "symbol": symbol,
        }

    row = klines[-1]
    prev_20 = klines[-21:-1]
    prev_15 = klines[-16:-1]

    volumes = [safe_float(r["volume"]) for r in prev_20]
    vol_mean = _mean(volumes)
    vol_std = _std(volumes)
    current_volume = safe_float(row["volume"])
    volume_zscore = 0.0 if vol_std <= 1e-12 else (current_volume - vol_mean) / vol_std

    open_p = safe_float(row["open"])
    high_p = safe_float(row["high"])
    low_p = safe_float(row["low"])
    close_p = safe_float(row["close"])

    return_1m_pct = _safe_pct_change(open_p, close_p)
    range_pct = ((high_p - low_p) / max(open_p, 1e-12)) * 100.0

    closes_15 = [safe_float(r["close"]) for r in prev_15] + [close_p]
    realized_vol_15m = _std(_log_returns(closes_15))

    flags: List[str] = []
    direction = "NEUTRAL"

    if volume_zscore >= 3.0:
        flags.append("volume_spike")

    if return_1m_pct >= 0.8:
        flags.append("price_spike_up")
        direction = "UP"
    elif return_1m_pct <= -0.8:
        flags.append("price_spike_down")
        direction = "DOWN"

    if range_pct >= 1.2 or realized_vol_15m >= 0.01:
        flags.append("volatility_burst")

    if ("volume_spike" in flags) and (
        "price_spike_up" in flags or "price_spike_down" in flags or "volatility_burst" in flags
    ):
        flags.append("combined_shock")

    flags = list(dict.fromkeys(flags))
    if not flags:
        return {
            "event_type": "",
            "direction": "NEUTRAL",
            "volume_bucket": _bucket_volume_zscore(volume_zscore),
            "return_bucket": _bucket_return_1m(return_1m_pct),
            "winrate": 0.0,
            "sample_size": 0,
            "recent": 0,
            "volume_zscore": volume_zscore,
            "return_1m_pct": return_1m_pct,
            "range_pct": range_pct,
            "realized_vol_15m": realized_vol_15m,
            "symbol": symbol,
        }

    event_type = "combined_shock" if "combined_shock" in flags else flags[0]
    volume_bucket = _bucket_volume_zscore(volume_zscore)
    return_bucket = _bucket_return_1m(return_1m_pct)

    # Backtest bulgularına göre gömülü event edge tablosu
    profile_map: Dict[Tuple[str, str, str], Dict[str, float]] = {
        ("price_spike_up", "normal", "sharp_up"): {"winrate": 0.975, "sample_size": 40},
        ("combined_shock", "high", "mild"): {"winrate": 0.7636, "sample_size": 55},
        ("combined_shock", "high", "sharp_up"): {"winrate": 0.7333, "sample_size": 15},
        ("price_spike_up", "normal", "mild"): {"winrate": 0.63, "sample_size": 100},
        ("combined_shock", "extreme", "sharp_down"): {"winrate": 0.625, "sample_size": 40},
        ("combined_shock", "extreme", "mild"): {"winrate": 0.6193, "sample_size": 155},
        ("combined_shock", "extreme", "sharp_up"): {"winrate": 0.6133, "sample_size": 75},
        ("price_spike_down", "normal", "sharp_down"): {"winrate": 0.60, "sample_size": 10},
        ("combined_shock", "high", "sharp_down"): {"winrate": 0.4667, "sample_size": 15},
        ("price_spike_down", "normal", "mild"): {"winrate": 0.45, "sample_size": 20},
        ("volatility_burst", "normal", "mild"): {"winrate": 0.0, "sample_size": 45},
        ("volume_spike", "extreme", "mild"): {"winrate": 0.0, "sample_size": 29930},
        ("volume_spike", "high", "mild"): {"winrate": 0.0, "sample_size": 33110},
    }

    profile = profile_map.get((event_type, volume_bucket, return_bucket), {"winrate": 0.0, "sample_size": 0})

    return {
        "event_type": event_type,
        "direction": direction,
        "volume_bucket": volume_bucket,
        "return_bucket": return_bucket,
        "winrate": float(profile["winrate"]),
        "sample_size": int(profile["sample_size"]),
        "recent": 1,
        "volume_zscore": volume_zscore,
        "return_1m_pct": return_1m_pct,
        "range_pct": range_pct,
        "realized_vol_15m": realized_vol_15m,
        "symbol": symbol,
    }


def build_market_context(symbol: str) -> Dict[str, Any]:
    ticker = binance.get_24h_ticker(symbol)
    book = get_book_snapshot(symbol)
    open_interest = binance.get_open_interest(symbol)

    tf_1h = get_tf_snapshot(symbol, "1h")
    tf_4h = get_tf_snapshot(symbol, "4h")
    tf_1d = get_tf_snapshot(symbol, "1d")

    recent_1m = _get_recent_1m_klines(symbol, 240)
    market_event = _detect_market_event_from_1m(symbol, recent_1m)

    return {
        "symbol": symbol,
        "last_price": safe_float(ticker.get("lastPrice")),
        "bid": book["bid"],
        "ask": book["ask"],
        "spread_pct": book["spread_pct"],
        "volume_24h_usdt": safe_float(ticker.get("quoteVolume")),
        "funding_rate_pct": binance.get_funding_rate(symbol),
        "funding_rate_available": 1,
        "open_interest": open_interest,
        "open_interest_change_pct": 0.0,
        "market_event": market_event,
        "tf": {
            "1H": tf_1h,
            "4H": tf_4h,
            "1D": tf_1d,
        },
    }


def get_top_symbols_by_volume(limit: int) -> List[str]:
    return binance.get_top_symbols_by_volume(limit)