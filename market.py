from typing import Any, Dict, List

import binance
from utils import pct_diff, safe_float


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

    gains = []
    losses = []
    for i in range(1, len(values)):
        diff = values[i] - values[i - 1]
        gains.append(max(diff, 0))
        losses.append(abs(min(diff, 0)))

    avg_gain = sum(gains[-period:]) / period
    avg_loss = sum(losses[-period:]) / period

    if avg_loss == 0:
        return 100.0
    rs = avg_gain / avg_loss
    return 100 - (100 / (1 + rs))


def atr(klines: List[Dict[str, Any]], period: int = 14) -> float:
    if len(klines) < period + 1:
        return 0.0

    trs = []
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
    mid = (bid + ask) / 2 if bid and ask else 0.0
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


def build_market_context(symbol: str) -> Dict[str, Any]:
    ticker = binance.get_24h_ticker(symbol)
    book = get_book_snapshot(symbol)

    tf_1h = get_tf_snapshot(symbol, "1h")
    tf_4h = get_tf_snapshot(symbol, "4h")
    tf_1d = get_tf_snapshot(symbol, "1d")

    return {
        "symbol": symbol,
        "last_price": safe_float(ticker.get("lastPrice")),
        "bid": book["bid"],
        "ask": book["ask"],
        "spread_pct": book["spread_pct"],
        "volume_24h_usdt": safe_float(ticker.get("quoteVolume")),
        "funding_rate_pct": binance.get_funding_rate(symbol),
        "tf": {
            "1H": tf_1h,
            "4H": tf_4h,
            "1D": tf_1d,
        }
    }


def get_top_symbols_by_volume(limit: int) -> List[str]:
    return binance.get_top_symbols_by_volume(limit)