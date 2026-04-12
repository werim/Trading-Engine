from typing import Any, Dict, List

from binance_real import BinanceFuturesClient
from config import CONFIG
from utils import log_message, read_json, write_json


client = BinanceFuturesClient()


def build_symbol_meta() -> Dict[str, Dict[str, Any]]:
    info = client.get_exchange_info()
    result: Dict[str, Dict[str, Any]] = {}

    for sym in info.get("symbols", []):
        if sym.get("contractType") != "PERPETUAL":
            continue
        if sym.get("quoteAsset") != CONFIG.TRADE.QUOTE_ASSET:
            continue
        if sym.get("status") != "TRADING":
            continue

        price_tick = 0.0
        qty_step = 0.0
        min_qty = 0.0
        min_notional = 0.0

        for f in sym.get("filters", []):
            if f.get("filterType") == "PRICE_FILTER":
                price_tick = float(f.get("tickSize", 0))
            elif f.get("filterType") == "LOT_SIZE":
                qty_step = float(f.get("stepSize", 0))
                min_qty = float(f.get("minQty", 0))
            elif f.get("filterType") == "MIN_NOTIONAL":
                min_notional = float(f.get("notional", 0))

        result[sym["symbol"]] = {
            "symbol": sym["symbol"],
            "price_tick": price_tick,
            "qty_step": qty_step,
            "min_qty": min_qty,
            "min_notional": min_notional,
            "base_asset": sym.get("baseAsset"),
            "quote_asset": sym.get("quoteAsset"),
        }

    write_json(CONFIG.FILES.SYMBOL_META_JSON, result)
    log_message(f"SYMBOL_META_BUILT count={len(result)}")
    return result


def get_symbol_meta() -> Dict[str, Dict[str, Any]]:
    meta = read_json(CONFIG.FILES.SYMBOL_META_JSON, {})
    if meta:
        return meta
    return build_symbol_meta()


def get_tradeable_symbols() -> List[str]:
    data = client.get_24h_ticker()
    symbols = []

    for row in data:
        symbol = row.get("symbol", "")
        if not symbol.endswith(CONFIG.TRADE.QUOTE_ASSET):
            continue

        quote_volume = float(row.get("quoteVolume", 0))
        if quote_volume < CONFIG.TRADE.MIN_VOLUME_USDT_24H:
            continue

        symbols.append((symbol, quote_volume))

    symbols = sorted(symbols, key=lambda x: x[1], reverse=True)
    return [s for s, _ in symbols[: CONFIG.TRADE.MAX_SYMBOLS]]


def get_market_snapshot(symbol: str) -> Dict[str, Any]:
    book = client.get_book_ticker(symbol)
    price = client.get_ticker_price(symbol)
    funding_rows = client.get_funding_rate(symbol, limit=1)
    funding_rate_pct = 0.0
    if funding_rows:
        funding_rate_pct = float(funding_rows[-1].get("fundingRate", 0)) * 100.0

    bid = float(book["bidPrice"])
    ask = float(book["askPrice"])
    spread_pct = ((ask - bid) / price) * 100.0 if price > 0 else 0.0

    return {
        "symbol": symbol,
        "price": price,
        "bid": bid,
        "ask": ask,
        "spread_pct": spread_pct,
        "funding_rate_pct": funding_rate_pct,
    }


def refresh_market_cache() -> Dict[str, Any]:
    symbols = get_tradeable_symbols()
    cache: Dict[str, Any] = {}

    for symbol in symbols:
        try:
            cache[symbol] = get_market_snapshot(symbol)
        except Exception as e:
            log_message(f"MARKET_SNAPSHOT_FAIL symbol={symbol} error={e}")

    write_json(CONFIG.FILES.MARKET_CACHE_JSON, cache)
    return cache