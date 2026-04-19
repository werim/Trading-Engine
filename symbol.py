from __future__ import annotations
from binance import get_available_balance
import csv
import math
import os
from dataclasses import dataclass, asdict
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

import requests


# =========================
# CONFIG
# =========================
BASE_URL = os.getenv("BINANCE_FUTURES_BASE_URL", "https://fapi.binance.com")
TOP_N = int(os.getenv("TOP_N", "50"))
QUOTE_ASSET = os.getenv("QUOTE_ASSET", "USDT")
TIMEFRAMES = ["1h", "4h", "1d"]
KLINE_LIMIT = int(os.getenv("KLINE_LIMIT", "220"))
OUTPUT_CSV = os.getenv("SYMBOL_SCAN_OUTPUT", "symbol_setups.csv")
MIN_24H_VOLUME_USDT = float(os.getenv("MIN_24H_VOLUME_USDT", "10000000"))
MAX_SPREAD_PCT = float(os.getenv("MAX_SPREAD_PCT", "0.35"))
MIN_RR = float(os.getenv("MIN_RR", "1.8"))
PULLBACK_BUFFER = float(os.getenv("PULLBACK_BUFFER", "0.15"))
BREAKOUT_BUFFER = float(os.getenv("BREAKOUT_BUFFER", "0.10"))
REQUEST_TIMEOUT = float(os.getenv("REQUEST_TIMEOUT", "10"))

# Portfolio / risk
ACCOUNT_SIZE_USDT = get_available_balance("USDT")
RISK_PER_TRADE_PCT = float(os.getenv("RISK_PER_TRADE_PCT", "2.0"))
FUTURES_LEVERAGE = float(os.getenv("FUTURES_LEVERAGE", "3.0"))
MAX_LEVERAGE = float(os.getenv("MAX_LEVERAGE", "5.0"))
SPOT_CAPITAL_PCT = float(os.getenv("SPOT_CAPITAL_PCT", "35.0"))
FUTURES_CAPITAL_PCT = float(os.getenv("FUTURES_CAPITAL_PCT", "25.0"))
GRID_CAPITAL_PCT = float(os.getenv("GRID_CAPITAL_PCT", "20.0"))
RESERVE_CAPITAL_PCT = float(os.getenv("RESERVE_CAPITAL_PCT", "20.0"))

# Quantity safety
MAX_POSITION_NOTIONAL_PCT = float(os.getenv("MAX_POSITION_NOTIONAL_PCT", "25.0"))
DEFAULT_FEE_PCT = float(os.getenv("DEFAULT_FEE_PCT", "0.08"))


# =========================
# DATA STRUCTURES
# =========================
@dataclass
class Setup:
    symbol: str
    market_type: str
    side: str
    setup_type: str
    score: int
    rr: float
    entry: float
    sl: float
    tp: float
    stop_pct: float
    tp_pct: float
    spread_pct: float
    volume_24h_usdt: float
    qty: float
    notional_usdt: float
    margin_usdt: float
    risk_usdt: float
    leverage: float
    tf_context: str
    reason: str
    created_at: str


# =========================
# HTTP
# =========================
class BinanceHTTP:
    def __init__(self, base_url: str = BASE_URL) -> None:
        self.base_url = base_url.rstrip("/")
        self.session = requests.Session()

    def _get(self, path: str, params: Optional[Dict[str, Any]] = None) -> Any:
        url = f"{self.base_url}{path}"
        r = self.session.get(url, params=params, timeout=REQUEST_TIMEOUT)
        r.raise_for_status()
        return r.json()

    def exchange_info(self) -> Dict[str, Any]:
        return self._get("/fapi/v1/exchangeInfo")

    def ticker_24h(self) -> List[Dict[str, Any]]:
        return self._get("/fapi/v1/ticker/24hr")

    def book_ticker(self, symbol: str) -> Dict[str, Any]:
        return self._get("/fapi/v1/ticker/bookTicker", {"symbol": symbol})

    def klines(self, symbol: str, interval: str, limit: int = KLINE_LIMIT) -> List[List[Any]]:
        return self._get(
            "/fapi/v1/klines",
            {"symbol": symbol, "interval": interval, "limit": limit},
        )


# =========================
# HELPERS
# =========================
def to_float(x: Any, default: float = 0.0) -> float:
    try:
        return float(x)
    except Exception:
        return default


def now_utc() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")


def ema(values: List[float], period: int) -> List[float]:
    if not values:
        return []
    if period <= 1:
        return values[:]
    alpha = 2 / (period + 1)
    out = [values[0]]
    for v in values[1:]:
        out.append((v * alpha) + (out[-1] * (1 - alpha)))
    return out


def atr(candles: List[Dict[str, float]], period: int = 14) -> float:
    if len(candles) < period + 1:
        return 0.0
    trs: List[float] = []
    for i in range(1, len(candles)):
        h = candles[i]["high"]
        l = candles[i]["low"]
        pc = candles[i - 1]["close"]
        tr = max(h - l, abs(h - pc), abs(l - pc))
        trs.append(tr)
    atr_values = ema(trs, period)
    return atr_values[-1] if atr_values else 0.0


def highest(values: List[float], lookback: int) -> float:
    if not values:
        return 0.0
    return max(values[-lookback:])


def lowest(values: List[float], lookback: int) -> float:
    if not values:
        return 0.0
    return min(values[-lookback:])


def round_step(value: float, step: float) -> float:
    if step <= 0:
        return value
    return math.floor(value / step) * step


def round_tick(value: float, tick: float) -> float:
    if tick <= 0:
        return value
    return round(round_step(value, tick), 12)


def parse_klines(raw: List[List[Any]]) -> List[Dict[str, float]]:
    out: List[Dict[str, float]] = []
    for row in raw:
        out.append(
            {
                "open": to_float(row[1]),
                "high": to_float(row[2]),
                "low": to_float(row[3]),
                "close": to_float(row[4]),
                "volume": to_float(row[5]),
            }
        )
    return out


# =========================
# MARKET PARSING
# =========================
def get_symbol_meta(exchange_info: Dict[str, Any]) -> Dict[str, Dict[str, float]]:
    meta: Dict[str, Dict[str, float]] = {}
    for s in exchange_info.get("symbols", []):
        if s.get("contractType") != "PERPETUAL":
            continue
        filters = {f["filterType"]: f for f in s.get("filters", [])}
        price_filter = filters.get("PRICE_FILTER", {})
        lot_filter = filters.get("LOT_SIZE", {})
        meta[s["symbol"]] = {
            "tick_size": to_float(price_filter.get("tickSize"), 0.0),
            "step_size": to_float(lot_filter.get("stepSize"), 0.0),
            "min_qty": to_float(lot_filter.get("minQty"), 0.0),
        }
    return meta


def get_top_volume_symbols(client: BinanceHTTP, quote_asset: str = QUOTE_ASSET, top_n: int = TOP_N) -> List[Dict[str, Any]]:
    rows = client.ticker_24h()
    filtered: List[Dict[str, Any]] = []

    for row in rows:
        symbol = row.get("symbol", "")
        if not symbol.endswith(quote_asset):
            continue
        if any(x in symbol for x in ["BUSD", "USDCUSDT", "USDTUSDC"]):
            continue
        volume_usdt = to_float(row.get("quoteVolume"))
        if volume_usdt < MIN_24H_VOLUME_USDT:
            continue
        filtered.append(
            {
                "symbol": symbol,
                "quoteVolume": volume_usdt,
                "lastPrice": to_float(row.get("lastPrice")),
                "priceChangePercent": to_float(row.get("priceChangePercent")),
            }
        )

    filtered.sort(key=lambda x: x["quoteVolume"], reverse=True)
    return filtered[:top_n]


# =========================
# TREND + SCORING
# =========================
def tf_regime(candles: List[Dict[str, float]]) -> Dict[str, float | str]:
    closes = [c["close"] for c in candles]
    if len(closes) < 200:
        return {
            "regime": "RANGE",
            "ema20": 0.0,
            "ema50": 0.0,
            "ema200": 0.0,
            "last": closes[-1] if closes else 0.0,
        }

    e20 = ema(closes, 20)[-1]
    e50 = ema(closes, 50)[-1]
    e200 = ema(closes, 200)[-1]
    last = closes[-1]

    if last > e20 > e50 > e200:
        regime = "LONG"
    elif last < e20 < e50 < e200:
        regime = "SHORT"
    else:
        regime = "RANGE"

    return {
        "regime": regime,
        "ema20": e20,
        "ema50": e50,
        "ema200": e200,
        "last": last,
    }


def build_tf_context(tf_map: Dict[str, Dict[str, float | str]]) -> str:
    return "|".join(f"{tf.upper()}={data['regime']}" for tf, data in tf_map.items())


def calc_spread_pct(client: BinanceHTTP, symbol: str, last_price: float) -> float:
    try:
        book = client.book_ticker(symbol)
        bid = to_float(book.get("bidPrice"))
        ask = to_float(book.get("askPrice"))
        if bid <= 0 or ask <= 0 or last_price <= 0:
            return 0.0
        return ((ask - bid) / last_price) * 100.0
    except Exception:
        return 0.0


def score_symbol(tf_map: Dict[str, Dict[str, float | str]], spread_pct: float) -> Tuple[int, Optional[str]]:
    score = 0
    t1 = str(tf_map["1h"]["regime"])
    t4 = str(tf_map["4h"]["regime"])
    td = str(tf_map["1d"]["regime"])

    if t1 == t4 == td == "LONG":
        return 8, "LONG"
    if t1 == t4 == td == "SHORT":
        return 8, "SHORT"

    if t4 == td == "LONG" and t1 in {"LONG", "RANGE"}:
        score += 6
        side = "LONG"
    elif t4 == td == "SHORT" and t1 in {"SHORT", "RANGE"}:
        score += 6
        side = "SHORT"
    else:
        side = None

    if t1 == t4 and t1 != "RANGE":
        score += 1
    if spread_pct <= 0.12:
        score += 1

    return score, side


# =========================
# RISK ENGINE
# =========================
def risk_budget_usdt() -> float:
    return ACCOUNT_SIZE_USDT * (RISK_PER_TRADE_PCT / 100.0)


def max_position_notional_usdt() -> float:
    return ACCOUNT_SIZE_USDT * (MAX_POSITION_NOTIONAL_PCT / 100.0)


def round_qty(qty: float, step_size: float, min_qty: float) -> float:
    if step_size > 0:
        qty = round_step(qty, step_size)
    if qty < min_qty:
        return 0.0
    return round(qty, 12)


def calc_futures_qty(entry: float, sl: float, meta: Dict[str, float], leverage: float) -> Tuple[float, float, float, float]:
    leverage = min(leverage, MAX_LEVERAGE)
    risk_per_unit = abs(entry - sl)
    if entry <= 0 or risk_per_unit <= 0:
        return 0.0, 0.0, 0.0, 0.0

    budget_risk = risk_budget_usdt()
    raw_qty = budget_risk / risk_per_unit
    step_size = meta.get("step_size", 0.0)
    min_qty = meta.get("min_qty", 0.0)
    qty = round_qty(raw_qty, step_size, min_qty)
    if qty <= 0:
        return 0.0, 0.0, 0.0, 0.0

    notional = qty * entry
    max_notional = max_position_notional_usdt()
    if notional > max_notional:
        qty = round_qty(max_notional / entry, step_size, min_qty)
        notional = qty * entry

    est_fee = notional * (DEFAULT_FEE_PCT / 100.0)
    total_risk = (qty * risk_per_unit) + est_fee
    margin = notional / leverage if leverage > 0 else notional
    return qty, notional, margin, total_risk


def calc_spot_qty(entry: float, sl: float, meta: Dict[str, float]) -> Tuple[float, float, float, float]:
    risk_per_unit = abs(entry - sl)
    if entry <= 0 or risk_per_unit <= 0:
        return 0.0, 0.0, 0.0, 0.0

    budget_risk = risk_budget_usdt()
    budget_capital = ACCOUNT_SIZE_USDT * (SPOT_CAPITAL_PCT / 100.0)
    raw_qty_from_risk = budget_risk / risk_per_unit
    raw_qty_from_capital = budget_capital / entry
    raw_qty = min(raw_qty_from_risk, raw_qty_from_capital)

    step_size = meta.get("step_size", 0.0)
    min_qty = meta.get("min_qty", 0.0)
    qty = round_qty(raw_qty, step_size, min_qty)
    if qty <= 0:
        return 0.0, 0.0, 0.0, 0.0

    notional = qty * entry
    est_fee = notional * (DEFAULT_FEE_PCT / 100.0)
    total_risk = (qty * risk_per_unit) + est_fee
    return qty, notional, notional, total_risk


def choose_market_type(setup_type: str, tf_map: Dict[str, Dict[str, float | str]], stop_pct: float) -> str:
    t1 = str(tf_map["1h"]["regime"])
    t4 = str(tf_map["4h"]["regime"])
    td = str(tf_map["1d"]["regime"])

    if td == t4 == t1 and stop_pct <= 3.5:
        return "FUTURES"
    if setup_type.startswith("PULLBACK") and stop_pct <= 6.0:
        return "SPOT"
    return "FUTURES"


# =========================
# SETUP BUILDERS
# =========================
def finalize_setup(
    symbol: str,
    setup_type: str,
    side: str,
    score: int,
    rr: float,
    entry: float,
    sl: float,
    tp: float,
    spread_pct: float,
    volume_24h_usdt: float,
    tf_map: Dict[str, Dict[str, float | str]],
    meta: Dict[str, float],
    reason: str,
) -> Optional[Setup]:
    if entry <= 0 or sl <= 0 or tp <= 0:
        return None

    if side == "LONG":
        stop_pct = ((entry - sl) / entry) * 100.0
        tp_pct = ((tp - entry) / entry) * 100.0
    else:
        stop_pct = ((sl - entry) / entry) * 100.0
        tp_pct = ((entry - tp) / entry) * 100.0

    market_type = choose_market_type(setup_type, tf_map, stop_pct)
    if market_type == "SPOT" and side == "SHORT":
        market_type = "FUTURES"

    if market_type == "SPOT":
        qty, notional, margin, total_risk = calc_spot_qty(entry, sl, meta)
        leverage = 1.0
    else:
        qty, notional, margin, total_risk = calc_futures_qty(entry, sl, meta, FUTURES_LEVERAGE)
        leverage = min(FUTURES_LEVERAGE, MAX_LEVERAGE)

    if qty <= 0 or total_risk <= 0:
        return None

    if total_risk > (risk_budget_usdt() * 1.08):
        return None

    return Setup(
        symbol=symbol,
        market_type=market_type,
        side=side,
        setup_type=setup_type,
        score=score,
        rr=round(rr, 2),
        entry=entry,
        sl=sl,
        tp=tp,
        stop_pct=round(stop_pct, 4),
        tp_pct=round(tp_pct, 4),
        spread_pct=round(spread_pct, 4),
        volume_24h_usdt=round(volume_24h_usdt, 2),
        qty=qty,
        notional_usdt=round(notional, 4),
        margin_usdt=round(margin, 4),
        risk_usdt=round(total_risk, 4),
        leverage=leverage,
        tf_context=build_tf_context(tf_map),
        reason=reason,
        created_at=now_utc(),
    )


def long_pullback_setup(
    symbol: str,
    tf_map: Dict[str, Dict[str, float | str]],
    candles_1h: List[Dict[str, float]],
    meta: Dict[str, float],
    spread_pct: float,
    volume_24h_usdt: float,
    score: int,
) -> Optional[Setup]:
    closes = [c["close"] for c in candles_1h]
    highs = [c["high"] for c in candles_1h]
    lows = [c["low"] for c in candles_1h]
    last = closes[-1]
    e20 = float(tf_map["1h"]["ema20"])
    e50 = float(tf_map["1h"]["ema50"])
    a = atr(candles_1h, 14)
    if a <= 0:
        return None

    support = max(e20, e50)
    entry = min(last, support * (1 - (PULLBACK_BUFFER / 100)))
    sl = min(lowest(lows, 12), entry - (1.2 * a))
    resistance = highest(highs[:-1], 20)
    tp = max(resistance, entry + (2.0 * a))

    risk = entry - sl
    reward = tp - entry
    if risk <= 0 or reward <= 0:
        return None

    rr = reward / risk
    if rr < MIN_RR:
        return None

    tick = meta.get("tick_size", 0.0)
    entry = round_tick(entry, tick)
    sl = round_tick(sl, tick)
    tp = round_tick(tp, tick)

    return finalize_setup(
        symbol=symbol,
        setup_type="PULLBACK_LONG",
        side="LONG",
        score=score + 1,
        rr=rr,
        entry=entry,
        sl=sl,
        tp=tp,
        spread_pct=spread_pct,
        volume_24h_usdt=volume_24h_usdt,
        tf_map=tf_map,
        meta=meta,
        reason="Trend-aligned pullback to EMA20/EMA50 area",
    )


def short_pullback_setup(
    symbol: str,
    tf_map: Dict[str, Dict[str, float | str]],
    candles_1h: List[Dict[str, float]],
    meta: Dict[str, float],
    spread_pct: float,
    volume_24h_usdt: float,
    score: int,
) -> Optional[Setup]:
    closes = [c["close"] for c in candles_1h]
    highs = [c["high"] for c in candles_1h]
    lows = [c["low"] for c in candles_1h]
    last = closes[-1]
    e20 = float(tf_map["1h"]["ema20"])
    e50 = float(tf_map["1h"]["ema50"])
    a = atr(candles_1h, 14)
    if a <= 0:
        return None

    resistance = min(e20, e50) if e20 < e50 else max(e20, e50)
    entry = max(last, resistance * (1 + (PULLBACK_BUFFER / 100)))
    sl = max(highest(highs, 12), entry + (1.2 * a))
    support = lowest(lows[:-1], 20)
    tp = min(support, entry - (2.0 * a))

    risk = sl - entry
    reward = entry - tp
    if risk <= 0 or reward <= 0:
        return None

    rr = reward / risk
    if rr < MIN_RR:
        return None

    tick = meta.get("tick_size", 0.0)
    entry = round_tick(entry, tick)
    sl = round_tick(sl, tick)
    tp = round_tick(tp, tick)

    return finalize_setup(
        symbol=symbol,
        setup_type="PULLBACK_SHORT",
        side="SHORT",
        score=score + 1,
        rr=rr,
        entry=entry,
        sl=sl,
        tp=tp,
        spread_pct=spread_pct,
        volume_24h_usdt=volume_24h_usdt,
        tf_map=tf_map,
        meta=meta,
        reason="Trend-aligned short pullback to EMA20/EMA50 area",
    )


def long_breakout_setup(
    symbol: str,
    tf_map: Dict[str, Dict[str, float | str]],
    candles_1h: List[Dict[str, float]],
    meta: Dict[str, float],
    spread_pct: float,
    volume_24h_usdt: float,
    score: int,
) -> Optional[Setup]:
    highs = [c["high"] for c in candles_1h]
    lows = [c["low"] for c in candles_1h]
    a = atr(candles_1h, 14)
    if a <= 0:
        return None

    breakout = highest(highs[:-1], 20)
    entry = breakout * (1 + (BREAKOUT_BUFFER / 100))
    sl = max(lowest(lows[-8:], 8), entry - (1.3 * a))
    tp = entry + (2.2 * a)

    risk = entry - sl
    reward = tp - entry
    if risk <= 0 or reward <= 0:
        return None

    rr = reward / risk
    if rr < MIN_RR:
        return None

    tick = meta.get("tick_size", 0.0)
    entry = round_tick(entry, tick)
    sl = round_tick(sl, tick)
    tp = round_tick(tp, tick)

    return finalize_setup(
        symbol=symbol,
        setup_type="BREAKOUT_LONG",
        side="LONG",
        score=score,
        rr=rr,
        entry=entry,
        sl=sl,
        tp=tp,
        spread_pct=spread_pct,
        volume_24h_usdt=volume_24h_usdt,
        tf_map=tf_map,
        meta=meta,
        reason="Trend continuation breakout above 20-bar high",
    )


def short_breakout_setup(
    symbol: str,
    tf_map: Dict[str, Dict[str, float | str]],
    candles_1h: List[Dict[str, float]],
    meta: Dict[str, float],
    spread_pct: float,
    volume_24h_usdt: float,
    score: int,
) -> Optional[Setup]:
    highs = [c["high"] for c in candles_1h]
    lows = [c["low"] for c in candles_1h]
    a = atr(candles_1h, 14)
    if a <= 0:
        return None

    breakdown = lowest(lows[:-1], 20)
    entry = breakdown * (1 - (BREAKOUT_BUFFER / 100))
    sl = min(highest(highs[-8:], 8), entry + (1.3 * a))
    sl = max(sl, entry + (0.8 * a))
    tp = entry - (2.2 * a)

    risk = sl - entry
    reward = entry - tp
    if risk <= 0 or reward <= 0:
        return None

    rr = reward / risk
    if rr < MIN_RR:
        return None

    tick = meta.get("tick_size", 0.0)
    entry = round_tick(entry, tick)
    sl = round_tick(sl, tick)
    tp = round_tick(tp, tick)

    return finalize_setup(
        symbol=symbol,
        setup_type="BREAKOUT_SHORT",
        side="SHORT",
        score=score,
        rr=rr,
        entry=entry,
        sl=sl,
        tp=tp,
        spread_pct=spread_pct,
        volume_24h_usdt=volume_24h_usdt,
        tf_map=tf_map,
        meta=meta,
        reason="Trend continuation breakdown below 20-bar low",
    )


def build_grid_candidate(
    symbol: str,
    tf_map: Dict[str, Dict[str, float | str]],
    candles_1h: List[Dict[str, float]],
    meta: Dict[str, float],
    spread_pct: float,
    volume_24h_usdt: float,
) -> Optional[Setup]:
    t1 = str(tf_map["1h"]["regime"])
    t4 = str(tf_map["4h"]["regime"])
    if t1 != "RANGE" and t4 != "RANGE":
        return None

    highs = [c["high"] for c in candles_1h]
    lows = [c["low"] for c in candles_1h]
    closes = [c["close"] for c in candles_1h]
    a = atr(candles_1h, 14)
    if a <= 0:
        return None

    range_high = highest(highs[:-1], 24)
    range_low = lowest(lows[:-1], 24)
    mid = (range_high + range_low) / 2
    width_pct = ((range_high - range_low) / mid) * 100 if mid > 0 else 0
    if width_pct < 3.0 or width_pct > 12.0:
        return None

    tick = meta.get("tick_size", 0.0)
    entry = round_tick(range_low + (0.15 * (range_high - range_low)), tick)
    sl = round_tick(range_low - (0.5 * a), tick)
    tp = round_tick(mid, tick)

    risk = entry - sl
    reward = tp - entry
    if risk <= 0 or reward <= 0:
        return None
    rr = reward / risk
    if rr < 1.4:
        return None

    qty, notional, margin, total_risk = calc_spot_qty(entry, sl, meta)
    if qty <= 0:
        return None

    return Setup(
        symbol=symbol,
        market_type="GRID_HINT",
        side="LONG",
        setup_type="GRID_RANGE",
        score=4,
        rr=round(rr, 2),
        entry=entry,
        sl=sl,
        tp=tp,
        stop_pct=round(((entry - sl) / entry) * 100.0, 4),
        tp_pct=round(((tp - entry) / entry) * 100.0, 4),
        spread_pct=round(spread_pct, 4),
        volume_24h_usdt=round(volume_24h_usdt, 2),
        qty=qty,
        notional_usdt=round(notional, 4),
        margin_usdt=round(margin, 4),
        risk_usdt=round(total_risk, 4),
        leverage=1.0,
        tf_context=build_tf_context(tf_map),
        reason="Range structure suitable for spot/grid accumulation near lower band",
        created_at=now_utc(),
    )


# =========================
# MAIN ENGINE
# =========================
def build_best_setup(
    client: BinanceHTTP,
    symbol: str,
    symbol_meta: Dict[str, float],
    volume_24h_usdt: float,
) -> Optional[Setup]:
    tf_map: Dict[str, Dict[str, float | str]] = {}
    candles_cache: Dict[str, List[Dict[str, float]]] = {}

    for tf in TIMEFRAMES:
        raw = client.klines(symbol, tf, KLINE_LIMIT)
        candles = parse_klines(raw)
        if len(candles) < 200:
            return None
        candles_cache[tf] = candles
        tf_map[tf] = tf_regime(candles)

    last_price = float(tf_map["1h"]["last"])
    spread_pct = calc_spread_pct(client, symbol, last_price)
    if spread_pct > MAX_SPREAD_PCT:
        return None

    score, side = score_symbol(tf_map, spread_pct)
    c1h = candles_cache["1h"]
    candidates: List[Setup] = []

    if side == "LONG" and score >= 6:
        a = long_pullback_setup(symbol, tf_map, c1h, symbol_meta, spread_pct, volume_24h_usdt, score)
        b = long_breakout_setup(symbol, tf_map, c1h, symbol_meta, spread_pct, volume_24h_usdt, score)
        if a:
            candidates.append(a)
        if b:
            candidates.append(b)
    elif side == "SHORT" and score >= 6:
        a = short_pullback_setup(symbol, tf_map, c1h, symbol_meta, spread_pct, volume_24h_usdt, score)
        b = short_breakout_setup(symbol, tf_map, c1h, symbol_meta, spread_pct, volume_24h_usdt, score)
        if a:
            candidates.append(a)
        if b:
            candidates.append(b)
    else:
        grid = build_grid_candidate(symbol, tf_map, c1h, symbol_meta, spread_pct, volume_24h_usdt)
        if grid:
            candidates.append(grid)

    if not candidates:
        return None

    candidates.sort(
        key=lambda x: (
            x.score,
            x.rr,
            x.volume_24h_usdt,
            -x.spread_pct,
        ),
        reverse=True,
    )
    return candidates[0]


def save_setups_csv(setups: List[Setup], filepath: str = OUTPUT_CSV) -> None:
    rows = [asdict(s) for s in setups]
    if not rows:
        print("No setup found. CSV not written.")
        return

    with open(filepath, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=list(rows[0].keys()))
        writer.writeheader()
        writer.writerows(rows)


def print_setups(setups: List[Setup]) -> None:
    if not setups:
        print("No valid setup found.")
        return

    print("\n=== TOP SETUPS ===")
    for s in setups:
        print(
            f"{s.symbol:12} | {s.market_type:9} | {s.side:5} | {s.setup_type:14} | "
            f"entry={s.entry} sl={s.sl} tp={s.tp} | qty={s.qty} | "
            f"notional={s.notional_usdt} | risk={s.risk_usdt} | rr={s.rr} | score={s.score}"
        )


def print_capital_plan() -> None:
    print("\n=== CAPITAL PLAN ===")
    print(f"Account Size      : {ACCOUNT_SIZE_USDT}")
    print(f"Risk / Trade      : {risk_budget_usdt():.2f} USDT ({RISK_PER_TRADE_PCT}%)")
    print(f"Spot Allocation   : {ACCOUNT_SIZE_USDT * SPOT_CAPITAL_PCT / 100.0:.2f} USDT")
    print(f"Futures Allocation: {ACCOUNT_SIZE_USDT * FUTURES_CAPITAL_PCT / 100.0:.2f} USDT")
    print(f"Grid Allocation   : {ACCOUNT_SIZE_USDT * GRID_CAPITAL_PCT / 100.0:.2f} USDT")
    print(f"Reserve           : {ACCOUNT_SIZE_USDT * RESERVE_CAPITAL_PCT / 100.0:.2f} USDT")


def run() -> List[Setup]:
    client = BinanceHTTP()
    exchange_info = client.exchange_info()
    meta_map = get_symbol_meta(exchange_info)
    top_symbols = get_top_volume_symbols(client, QUOTE_ASSET, TOP_N)

    setups: List[Setup] = []
    print_capital_plan()
    print(f"\nScanning top {len(top_symbols)} {QUOTE_ASSET} perpetual symbols...")

    for row in top_symbols:
        symbol = row["symbol"]
        meta = meta_map.get(symbol)
        if not meta:
            continue

        try:
            setup = build_best_setup(
                client=client,
                symbol=symbol,
                symbol_meta=meta,
                volume_24h_usdt=row["quoteVolume"],
            )
            if setup:
                setups.append(setup)
                print(
                    f"SETUP {setup.symbol} {setup.market_type} {setup.side} {setup.setup_type} "
                    f"entry={setup.entry} sl={setup.sl} tp={setup.tp} qty={setup.qty} "
                    f"risk={setup.risk_usdt} rr={setup.rr} score={setup.score}"
                )
            else:
                print(f"SKIP  {symbol} no valid setup")
        except Exception as e:
            print(f"ERROR {symbol} {e}")

    setups.sort(
        key=lambda x: (x.score, x.rr, x.volume_24h_usdt, -x.spread_pct),
        reverse=True,
    )

    save_setups_csv(setups, OUTPUT_CSV)
    print_setups(setups[:15])
    return setups


if __name__ == "__main__":
    run()
