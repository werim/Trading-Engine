from __future__ import annotations

import hashlib
import hmac
import os
import threading
import time
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import urlencode

import requests

from config import CONFIG
from position import log
from utils import (
    floor_qty_to_step,
    round_price_to_tick,
    safe_float,
    utc_ts_ms,
)

# =========================================================
# GLOBALS
# =========================================================

_SESSION: Optional[requests.Session] = None
_SYMBOL_META_CACHE: Dict[str, Dict[str, Any]] = {}
_SYMBOL_META_CACHE_TS: float = 0.0

_USER_STREAM_STATE: Dict[str, Any] = {
    "listen_key": "",
    "last_keepalive_ts": 0.0,
    "last_event_ts": 0.0,
    "running": False,
    "thread_started": False,
    "recent_events": [],
}

_CACHE_LOCK = threading.Lock()
_STREAM_LOCK = threading.Lock()


# =========================================================
# CONFIG HELPERS
# =========================================================

def _env(name: str, default: str = "") -> str:
    return os.getenv(name, default)


def _base_rest_url() -> str:
    """
    USDⓈ-M Futures REST base.
    Official base endpoint is fapi.binance.com for user data streams docs too.
    """
    testnet = _env("BINANCE_TESTNET", "0") == "1"
    return "https://testnet.binancefuture.com" if testnet else "https://fapi.binance.com"


def _base_ws_market_url() -> str:
    """
    New websocket split exists. Keep this abstract.
    """
    testnet = _env("BINANCE_TESTNET", "0") == "1"
    return "wss://testnet.binancefuture.com/ws-fapi/v1" if testnet else "wss://ws-fapi.binance.com/ws-fapi/v1"


def _api_key() -> str:
    return _env("BINANCE_API_KEY", "")


def _api_secret() -> str:
    return _env("BINANCE_API_SECRET", "")


def _recv_window() -> int:
    return int(_env("BINANCE_RECV_WINDOW", "5000"))


def _http_timeout() -> Tuple[int, int]:
    """
    connect timeout, read timeout
    """
    return (5, 15)


# =========================================================
# SESSION / LOW LEVEL HTTP
# =========================================================

def get_session() -> requests.Session:
    global _SESSION
    if _SESSION is None:
        s = requests.Session()
        s.headers.update({
            "X-MBX-APIKEY": _api_key(),
            "Content-Type": "application/x-www-form-urlencoded",
        })
        _SESSION = s
    return _SESSION


def _sign(params: Dict[str, Any]) -> str:
    query = urlencode(params, doseq=True)
    return hmac.new(
        _api_secret().encode("utf-8"),
        query.encode("utf-8"),
        hashlib.sha256,
    ).hexdigest()


def _prepare_signed_params(params: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    data = dict(params or {})
    data["timestamp"] = utc_ts_ms()
    data["recvWindow"] = _recv_window()
    data["signature"] = _sign(data)
    return data


def _parse_json_response(resp: requests.Response) -> Any:
    try:
        return resp.json()
    except Exception:
        text = resp.text.strip()
        raise RuntimeError(f"Non-JSON response status={resp.status_code} body={text[:300]}")


def _raise_for_binance_error(resp: requests.Response, payload: Any) -> None:
    if 200 <= resp.status_code < 300:
        return

    if isinstance(payload, dict):
        code = payload.get("code")
        msg = payload.get("msg", "")
    else:
        code = None
        msg = str(payload)

    raise RuntimeError(f"Binance API error status={resp.status_code} code={code} msg={msg}")


def _request(
    method: str,
    path: str,
    params: Optional[Dict[str, Any]] = None,
    signed: bool = False,
    data_in_query: bool = True,
) -> Any:
    """
    Generic REST request wrapper.
    """
    session = get_session()
    url = f"{_base_rest_url()}{path}"

    payload = dict(params or {})
    if signed:
        payload = _prepare_signed_params(payload)

    try:
        if method == "GET":
            resp = session.get(url, params=payload, timeout=_http_timeout())
        elif method == "POST":
            if data_in_query:
                resp = session.post(url, params=payload, timeout=_http_timeout())
            else:
                resp = session.post(url, data=payload, timeout=_http_timeout())
        elif method == "PUT":
            resp = session.put(url, params=payload, timeout=_http_timeout())
        elif method == "DELETE":
            resp = session.delete(url, params=payload, timeout=_http_timeout())
        else:
            raise ValueError(f"Unsupported method: {method}")
    except requests.Timeout as exc:
        raise TimeoutError(f"HTTP timeout {method} {path}") from exc
    except requests.RequestException as exc:
        raise RuntimeError(f"HTTP request error {method} {path}: {exc}") from exc

    payload = _parse_json_response(resp)
    _raise_for_binance_error(resp, payload)
    return payload


# =========================================================
# SAFE REQUEST / UNKNOWN RESULT HANDLING
# =========================================================

def _is_unknown_execution_error(exc: Exception) -> bool:
    """
    Binance general info:
    503 with 'Unknown error, please check your request or try again later.'
    means request may have succeeded and execution status is unknown.
    """
    msg = str(exc)
    needles = [
        "Unknown error, please check your request or try again later.",
        "HTTP timeout",
        "execution status is UNKNOWN",
    ]
    return any(n in msg for n in needles)


def with_retries(fn, *args, retries: int = 2, retry_sleep: float = 0.7, **kwargs):
    last_exc = None
    for attempt in range(retries + 1):
        try:
            return fn(*args, **kwargs)
        except Exception as exc:
            last_exc = exc
            if attempt >= retries:
                break
            time.sleep(retry_sleep)
    raise last_exc


# =========================================================
# EXCHANGE INFO / SYMBOL META
# =========================================================

def _parse_symbol_filters(symbol_info: Dict[str, Any]) -> Dict[str, Any]:
    filters = {f["filterType"]: f for f in symbol_info.get("filters", [])}

    price_filter = filters.get("PRICE_FILTER", {})
    lot_size = filters.get("LOT_SIZE", {})
    min_notional = filters.get("MIN_NOTIONAL", {})
    market_lot_size = filters.get("MARKET_LOT_SIZE", {})

    return {
        "symbol": symbol_info.get("symbol", ""),
        "status": symbol_info.get("status", ""),
        "base_asset": symbol_info.get("baseAsset", ""),
        "quote_asset": symbol_info.get("quoteAsset", ""),
        "price_precision": symbol_info.get("pricePrecision"),
        "quantity_precision": symbol_info.get("quantityPrecision"),
        "tick_size": safe_float(price_filter.get("tickSize")),
        "min_price": safe_float(price_filter.get("minPrice")),
        "max_price": safe_float(price_filter.get("maxPrice")),
        "step_size": safe_float(lot_size.get("stepSize")),
        "min_qty": safe_float(lot_size.get("minQty")),
        "max_qty": safe_float(lot_size.get("maxQty")),
        "market_step_size": safe_float(market_lot_size.get("stepSize")),
        "market_min_qty": safe_float(market_lot_size.get("minQty")),
        "market_max_qty": safe_float(market_lot_size.get("maxQty")),
        "min_notional": safe_float(min_notional.get("notional")),
        "raw": symbol_info,
    }


def get_exchange_info() -> Dict[str, Any]:
    return _request("GET", "/fapi/v1/exchangeInfo")


def refresh_symbol_meta_cache(force: bool = False) -> Dict[str, Dict[str, Any]]:
    global _SYMBOL_META_CACHE, _SYMBOL_META_CACHE_TS

    with _CACHE_LOCK:
        now = time.time()
        if not force and _SYMBOL_META_CACHE and (now - _SYMBOL_META_CACHE_TS) < 3600:
            return _SYMBOL_META_CACHE

        info = get_exchange_info()
        symbols = info.get("symbols", [])

        cache: Dict[str, Dict[str, Any]] = {}
        for s in symbols:
            symbol = s.get("symbol")
            if not symbol:
                continue
            cache[symbol] = _parse_symbol_filters(s)

        _SYMBOL_META_CACHE = cache
        _SYMBOL_META_CACHE_TS = now
        return _SYMBOL_META_CACHE


def get_symbol_meta(symbol: str) -> Dict[str, Any]:
    if symbol not in _SYMBOL_META_CACHE:
        refresh_symbol_meta_cache(force=False)
    return _SYMBOL_META_CACHE.get(symbol, {})


def normalize_order_params(
    symbol: str,
    price: Optional[float] = None,
    qty: Optional[float] = None,
    stop_price: Optional[float] = None,
    is_market: bool = False,
) -> Dict[str, Any]:
    meta = get_symbol_meta(symbol)
    if not meta:
        raise RuntimeError(f"Missing symbol meta for {symbol}")

    tick_size = safe_float(meta.get("tick_size"))
    step_size = safe_float(meta.get("market_step_size") if is_market else meta.get("step_size"))
    min_qty = safe_float(meta.get("market_min_qty") if is_market else meta.get("min_qty"))
    min_notional = safe_float(meta.get("min_notional"))

    out = {}

    if price is not None:
        out["price"] = round_price_to_tick(price, tick_size)

    if stop_price is not None:
        out["stop_price"] = round_price_to_tick(stop_price, tick_size)

    if qty is not None:
        q = floor_qty_to_step(qty, step_size)
        if q <= 0 or q < min_qty:
            raise RuntimeError(f"Normalized qty invalid for {symbol}: {q} < {min_qty}")
        out["qty"] = q

    if price is not None and qty is not None and min_notional > 0:
        if (out["price"] * out["qty"]) < min_notional:
            raise RuntimeError(f"Min notional failed for {symbol}: {out['price'] * out['qty']} < {min_notional}")

    return out


# =========================================================
# MARKET DATA
# =========================================================

def get_ticker_price(symbol: str) -> float:
    data = _request("GET", "/fapi/v1/ticker/price", {"symbol": symbol})
    return safe_float(data.get("price"))


def get_book_ticker(symbol: str) -> Dict[str, Any]:
    data = _request("GET", "/fapi/v1/ticker/bookTicker", {"symbol": symbol})
    return {
        "symbol": data.get("symbol", symbol),
        "bid": safe_float(data.get("bidPrice")),
        "ask": safe_float(data.get("askPrice")),
    }


def get_24h_ticker(symbol: str) -> Dict[str, Any]:
    data = _request("GET", "/fapi/v1/ticker/24hr", {"symbol": symbol})
    return {
        "symbol": data.get("symbol", symbol),
        "lastPrice": safe_float(data.get("lastPrice")),
        "quoteVolume": safe_float(data.get("quoteVolume")),
        "volume": safe_float(data.get("volume")),
        "priceChangePercent": safe_float(data.get("priceChangePercent")),
    }


def get_klines(symbol: str, interval: str, limit: int = 200) -> List[Dict[str, Any]]:
    rows = _request("GET", "/fapi/v1/klines", {
        "symbol": symbol,
        "interval": interval,
        "limit": limit,
    })

    out = []
    for row in rows:
        out.append({
            "open_time": row[0],
            "open": safe_float(row[1]),
            "high": safe_float(row[2]),
            "low": safe_float(row[3]),
            "close": safe_float(row[4]),
            "volume": safe_float(row[5]),
            "close_time": row[6],
            "quote_asset_volume": safe_float(row[7]),
            "trade_count": row[8],
        })
    return out


def get_funding_rate(symbol: str) -> float:
    """
    Latest funding rate snapshot.
    """
    rows = _request("GET", "/fapi/v1/fundingRate", {"symbol": symbol, "limit": 1})
    if not rows:
        return 0.0
    return safe_float(rows[0].get("fundingRate")) * 100.0


def get_open_interest(symbol: str) -> float:
    data = _request("GET", "/fapi/v1/openInterest", {"symbol": symbol})
    return safe_float(data.get("openInterest"))


def get_top_symbols_by_volume(limit: int = 100, quote: str = "USDT") -> List[str]:
    rows = _request("GET", "/fapi/v1/ticker/24hr")
    enriched = []
    for row in rows:
        symbol = row.get("symbol", "")
        if not symbol.endswith(quote):
            continue
        quote_volume = safe_float(row.get("quoteVolume"))
        if quote_volume <= 0:
            continue
        enriched.append((symbol, quote_volume))

    enriched.sort(key=lambda x: x[1], reverse=True)
    return [symbol for symbol, _ in enriched[:limit]]


# =========================================================
# ACCOUNT / POSITION
# =========================================================

def get_account_info_v3() -> Dict[str, Any]:
    try:
        return _request("GET", "/fapi/v3/account", signed=True)
    except Exception as e:
        log.warning("ACCOUNT_FETCH_FAIL err=%s", repr(e))
        return {}  # always dict


def get_available_balance(asset: str = "USDT") -> float:
    account = get_account_info_v3()
    log.info("ACCOUNT_KEYS=%s", list(account.keys()))
    # --- REAL BALANCE ---
    try:
        if "assets" in account:
            for item in account["assets"]:
                if item.get("asset") == asset:
                    balance = safe_float(item.get("availableBalance"))

                    if balance > 0:
                        log.info("BALANCE_SOURCE=REAL %s=%s", asset, balance)
                        return balance
                    else:
                        raise ValueError("Balance is zero")

        raise ValueError("Invalid account structure")

    except Exception as e:
        log.warning("REAL_BALANCE_FAIL %s err=%s", asset, repr(e))

    # --- PAPER FALLBACK ---
    paper_balance = CONFIG.PAPER_BALANCE.get(asset, 100.0)
    log.warning("BALANCE_SOURCE=PAPER %s=%s", asset, paper_balance)
    return paper_balance


def get_position_risk(symbol: Optional[str] = None) -> List[Dict[str, Any]]:
    params = {}
    if symbol:
        params["symbol"] = symbol
    rows = _request("GET", "/fapi/v3/positionRisk", params, signed=True)
    return rows if isinstance(rows, list) else []


def set_leverage(symbol: str, leverage: int) -> Dict[str, Any]:
    return _request("POST", "/fapi/v1/leverage", {
        "symbol": symbol,
        "leverage": leverage,
    }, signed=True)


# =========================================================
# ORDER QUERY / OPEN ORDERS
# =========================================================

def query_order(
    symbol: str,
    order_id: Optional[str] = None,
    client_order_id: Optional[str] = None,
) -> Dict[str, Any]:
    params = {"symbol": symbol}
    if order_id:
        params["orderId"] = order_id
    if client_order_id:
        params["origClientOrderId"] = client_order_id
    return _request("GET", "/fapi/v1/order", params, signed=True)


def query_all_orders(symbol: str, limit: int = 50) -> List[Dict[str, Any]]:
    return _request("GET", "/fapi/v1/allOrders", {
        "symbol": symbol,
        "limit": limit,
    }, signed=True)


def get_open_orders(symbol: Optional[str] = None) -> List[Dict[str, Any]]:
    params = {}
    if symbol:
        params["symbol"] = symbol
    rows = _request("GET", "/fapi/v1/openOrders", params, signed=True)
    return rows if isinstance(rows, list) else []


def get_account_trades(symbol: str, limit: int = 50) -> List[Dict[str, Any]]:
    rows = _request("GET", "/fapi/v1/userTrades", {
        "symbol": symbol,
        "limit": limit,
    }, signed=True)
    return rows if isinstance(rows, list) else []


# =========================================================
# ORDER SUBMIT
# =========================================================

def _futures_side(side: str) -> str:
    side_up = side.upper()
    if side_up in {"BUY", "SELL"}:
        return side_up
    if side_up == "LONG":
        return "BUY"
    if side_up == "SHORT":
        return "SELL"
    raise ValueError(f"Unsupported side: {side}")


def _build_entry_order_params(
    symbol: str,
    side: str,
    qty: float,
    client_order_id: str,
    price: Optional[float] = None,
) -> Dict[str, Any]:
    binance_side = _futures_side(side)

    is_market = price is None
    normalized = normalize_order_params(symbol, price=price, qty=qty, is_market=is_market)

    params = {
        "symbol": symbol,
        "side": binance_side,
        "quantity": normalized["qty"],
        "newClientOrderId": client_order_id,
    }

    if price is None:
        params["type"] = "MARKET"
    else:
        params["type"] = "LIMIT"
        params["timeInForce"] = "GTC"
        params["price"] = normalized["price"]

    return params


def place_limit_entry(
    symbol: str,
    side: str,
    qty: float,
    price: float,
    client_order_id: str,
) -> Dict[str, Any]:
    params = _build_entry_order_params(symbol, side, qty, client_order_id, price=price)
    return _request("POST", "/fapi/v1/order", params, signed=True)


def place_market_entry(
    symbol: str,
    side: str,
    qty: float,
    client_order_id: str,
) -> Dict[str, Any]:
    params = _build_entry_order_params(symbol, side, qty, client_order_id, price=None)
    return _request("POST", "/fapi/v1/order", params, signed=True)


# =========================================================
# CONDITIONAL / PROTECTION ORDERS
# IMPORTANT:
# Keep abstract because conditional orders are migrating to Algo Service.
# =========================================================

def _exit_side_for_position(position_side: str) -> str:
    return "SELL" if position_side.upper() == "LONG" else "BUY"


def place_stop_loss(
    symbol: str,
    side: str,
    stop_price: float,
    qty: float,
    reduce_only: bool = True,
) -> Dict[str, Any]:
    normalized = normalize_order_params(symbol, qty=qty, stop_price=stop_price, is_market=True)
    params = {
        "symbol": symbol,
        "side": _futures_side(side),
        "type": "STOP_MARKET",
        "stopPrice": normalized["stop_price"],
        "quantity": normalized["qty"],
        "reduceOnly": "true" if reduce_only else "false",
        "workingType": "MARK_PRICE",
    }
    return _request("POST", "/fapi/v1/order", params, signed=True)


def place_take_profit(
    symbol: str,
    side: str,
    stop_price: float,
    qty: float,
    reduce_only: bool = True,
) -> Dict[str, Any]:
    normalized = normalize_order_params(symbol, qty=qty, stop_price=stop_price, is_market=True)
    params = {
        "symbol": symbol,
        "side": _futures_side(side),
        "type": "TAKE_PROFIT_MARKET",
        "stopPrice": normalized["stop_price"],
        "quantity": normalized["qty"],
        "reduceOnly": "true" if reduce_only else "false",
        "workingType": "MARK_PRICE",
    }
    return _request("POST", "/fapi/v1/order", params, signed=True)


def cancel_order(
    symbol: str,
    order_id: Optional[str] = None,
    client_order_id: Optional[str] = None,
) -> Dict[str, Any]:
    params = {"symbol": symbol}
    if order_id:
        params["orderId"] = order_id
    if client_order_id:
        params["origClientOrderId"] = client_order_id
    return _request("DELETE", "/fapi/v1/order", params, signed=True)


def refresh_stop_loss_order(
    symbol: str,
    position_side: str,
    old_order_id: Optional[str],
    qty: float,
    new_stop_price: float,
) -> Dict[str, Any]:
    if old_order_id:
        try:
            cancel_order(symbol=symbol, order_id=old_order_id)
        except Exception:
            pass

    return place_stop_loss(
        symbol=symbol,
        side=_exit_side_for_position(position_side),
        stop_price=new_stop_price,
        qty=qty,
        reduce_only=True,
    )


def refresh_take_profit_order(
    symbol: str,
    position_side: str,
    old_order_id: Optional[str],
    qty: float,
    new_tp_price: float,
) -> Dict[str, Any]:
    if old_order_id:
        try:
            cancel_order(symbol=symbol, order_id=old_order_id)
        except Exception:
            pass

    return place_take_profit(
        symbol=symbol,
        side=_exit_side_for_position(position_side),
        stop_price=new_tp_price,
        qty=qty,
        reduce_only=True,
    )


# =========================================================
# SAFE SUBMIT / RECONCILE
# =========================================================

def safe_submit_order(
    submit_fn,
    query_symbol: str,
    client_order_id: str,
    *args,
    **kwargs,
) -> Dict[str, Any]:
    """
    Core idea:
    - submit
    - timeout / unknown result olursa başarısız sayma
    - önce query_order ile teyit et
    - bulunursa exchange truth kabul et
    - bulunmazsa kontrollü retry
    """
    try:
        return submit_fn(*args, **kwargs)
    except Exception as exc:
        if not _is_unknown_execution_error(exc):
            raise

        try:
            existing = query_order(symbol=query_symbol, client_order_id=client_order_id)
            if existing:
                return existing
        except Exception:
            pass

        time.sleep(0.5)

        try:
            existing = query_order(symbol=query_symbol, client_order_id=client_order_id)
            if existing:
                return existing
        except Exception:
            pass

        # Hala yoksa bir kez kontrollü retry
        return submit_fn(*args, **kwargs)


def safe_cancel_order(
    symbol: str,
    order_id: Optional[str] = None,
    client_order_id: Optional[str] = None,
) -> Dict[str, Any]:
    try:
        return cancel_order(symbol=symbol, order_id=order_id, client_order_id=client_order_id)
    except Exception:
        # Belki order zaten filled/cancelled oldu
        if order_id or client_order_id:
            return query_order(symbol=symbol, order_id=order_id, client_order_id=client_order_id)
        raise


# =========================================================
# USER DATA STREAM
# NOTE:
# This is intentionally abstract. You can wire websocket-client / websockets later.
# =========================================================

def create_listen_key() -> str:
    data = _request("POST", "/fapi/v1/listenKey", signed=False)
    listen_key = data.get("listenKey", "")
    if not listen_key:
        raise RuntimeError("Failed to create listenKey")
    return listen_key


def keepalive_listen_key(listen_key: str) -> Dict[str, Any]:
    return _request("PUT", "/fapi/v1/listenKey", {"listenKey": listen_key}, signed=False)


def close_listen_key(listen_key: str) -> Dict[str, Any]:
    return _request("DELETE", "/fapi/v1/listenKey", {"listenKey": listen_key}, signed=False)


def ensure_user_stream() -> str:
    with _STREAM_LOCK:
        now = time.time()
        listen_key = _USER_STREAM_STATE.get("listen_key", "")
        last_keepalive = safe_float(_USER_STREAM_STATE.get("last_keepalive_ts"))

        if not listen_key:
            listen_key = create_listen_key()
            _USER_STREAM_STATE["listen_key"] = listen_key
            _USER_STREAM_STATE["last_keepalive_ts"] = now
            return listen_key

        if (now - last_keepalive) > 30 * 60:
            try:
                keepalive_listen_key(listen_key)
                _USER_STREAM_STATE["last_keepalive_ts"] = now
                return listen_key
            except Exception:
                listen_key = create_listen_key()
                _USER_STREAM_STATE["listen_key"] = listen_key
                _USER_STREAM_STATE["last_keepalive_ts"] = now
                return listen_key

        return listen_key


def _remember_user_event(event: Dict[str, Any]) -> None:
    bucket = _USER_STREAM_STATE.setdefault("recent_events", [])
    bucket.append(event)
    if len(bucket) > 2000:
        del bucket[:-1000]
    _USER_STREAM_STATE["last_event_ts"] = time.time()


def handle_user_stream_event(event: Dict[str, Any]) -> None:
    """
    ORDER_TRADE_UPDATE / ACCOUNT_UPDATE için hafıza tamponu.
    order.py ve position.py isterse buradan son event'leri çekebilir.
    """
    if not isinstance(event, dict):
        return
    _remember_user_event(event)


def get_recent_user_events(limit: int = 100) -> List[Dict[str, Any]]:
    bucket = _USER_STREAM_STATE.get("recent_events", [])
    return bucket[-limit:]


def find_recent_order_event(symbol: str, client_order_id: Optional[str] = None, order_id: Optional[str] = None) -> Optional[Dict[str, Any]]:
    for event in reversed(_USER_STREAM_STATE.get("recent_events", [])):
        if event.get("e") != "ORDER_TRADE_UPDATE":
            continue

        order_data = event.get("o", {})
        if order_data.get("s") != symbol:
            continue
        if client_order_id and order_data.get("c") == client_order_id:
            return event
        if order_id and str(order_data.get("i")) == str(order_id):
            return event
    return None


# =========================================================
# HEALTH / DEBUG HELPERS
# =========================================================

def ping() -> Dict[str, Any]:
    return _request("GET", "/fapi/v1/ping")


def server_time() -> Dict[str, Any]:
    return _request("GET", "/fapi/v1/time")


def sync_server_time() -> None:
    """
    Starter placeholder.
    Requests signing generally tolerates recvWindow, but you can add local drift tracking here.
    """
    _ = server_time()


def bootstrap() -> None:
    """
    Engine start sırasında çağrılabilir.
    """
    sync_server_time()
    refresh_symbol_meta_cache(force=False)
    ensure_user_stream()
