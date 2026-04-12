from __future__ import annotations

import hashlib
import hmac
import time
from typing import Any, Dict, List, Optional
from urllib.parse import urlencode

import requests

from config import CONFIG
from utils import log_message


class BinanceFuturesClient:
    def __init__(self) -> None:
        self.api_key = CONFIG.BINANCE.API_KEY
        self.api_secret = CONFIG.BINANCE.API_SECRET.encode("utf-8")
        self.base_url = CONFIG.BINANCE.BASE_URL.rstrip("/")
        self.recv_window = CONFIG.BINANCE.RECV_WINDOW
        self.session = requests.Session()
        self.session.headers.update({"X-MBX-APIKEY": self.api_key})

    def _sign(self, params: Dict[str, Any]) -> str:
        query = urlencode(params, doseq=True)
        return hmac.new(self.api_secret, query.encode("utf-8"), hashlib.sha256).hexdigest()

    def _request(
        self,
        method: str,
        path: str,
        params: Optional[Dict[str, Any]] = None,
        signed: bool = False,
    ) -> Any:
        params = params or {}
        url = f"{self.base_url}{path}"

        if signed:
            params["timestamp"] = int(time.time() * 1000)
            params["recvWindow"] = self.recv_window
            params["signature"] = self._sign(params)

        try:
            if method.upper() == "GET":
                r = self.session.get(url, params=params, timeout=15)
            elif method.upper() == "POST":
                r = self.session.post(url, params=params, timeout=15)
            elif method.upper() == "DELETE":
                r = self.session.delete(url, params=params, timeout=15)
            else:
                raise ValueError(f"Unsupported method: {method}")

            if r.status_code != 200:
                raise RuntimeError(f"Binance API error {r.status_code}: {r.text}")

            return r.json()
        except Exception as e:
            log_message(f"[BINANCE] {method} {path} failed error={e}")
            raise

    def get_exchange_info(self) -> Dict[str, Any]:
        return self._request("GET", "/fapi/v1/exchangeInfo")

    def get_ticker_price(self, symbol: str) -> float:
        data = self._request("GET", "/fapi/v1/ticker/price", {"symbol": symbol})
        return float(data["price"])

    def get_book_ticker(self, symbol: str) -> Dict[str, Any]:
        return self._request("GET", "/fapi/v1/ticker/bookTicker", {"symbol": symbol})

    def get_24h_ticker(self) -> List[Dict[str, Any]]:
        return self._request("GET", "/fapi/v1/ticker/24hr")

    def get_funding_rate(self, symbol: str, limit: int = 1) -> List[Dict[str, Any]]:
        return self._request("GET", "/fapi/v1/fundingRate", {"symbol": symbol, "limit": limit})

    def get_klines(self, symbol: str, interval: str, limit: int = 240) -> List[List[Any]]:
        return self._request(
            "GET",
            "/fapi/v1/klines",
            {"symbol": symbol, "interval": interval, "limit": limit},
        )

    def set_leverage(self, symbol: str, leverage: int) -> Dict[str, Any]:
        return self._request(
            "POST",
            "/fapi/v1/leverage",
            {"symbol": symbol, "leverage": leverage},
            signed=True,
        )

    def get_position_risk(self) -> List[Dict[str, Any]]:
        return self._request("GET", "/fapi/v2/positionRisk", signed=True)

    def get_open_orders(self, symbol: Optional[str] = None) -> List[Dict[str, Any]]:
        params = {}
        if symbol:
            params["symbol"] = symbol
        return self._request("GET", "/fapi/v1/openOrders", params, signed=True)

    def place_limit_order(
        self,
        symbol: str,
        side: str,
        quantity: float,
        price: float,
        reduce_only: bool = False,
        time_in_force: str = "GTC",
    ) -> Dict[str, Any]:
        return self._request(
            "POST",
            "/fapi/v1/order",
            {
                "symbol": symbol,
                "side": side,
                "type": "LIMIT",
                "timeInForce": time_in_force,
                "quantity": quantity,
                "price": price,
                "reduceOnly": "true" if reduce_only else "false",
            },
            signed=True,
        )

    def place_market_order(
        self,
        symbol: str,
        side: str,
        quantity: float,
        reduce_only: bool = False,
    ) -> Dict[str, Any]:
        return self._request(
            "POST",
            "/fapi/v1/order",
            {
                "symbol": symbol,
                "side": side,
                "type": "MARKET",
                "quantity": quantity,
                "reduceOnly": "true" if reduce_only else "false",
            },
            signed=True,
        )

    def place_stop_market(
        self,
        symbol: str,
        side: str,
        stop_price: float,
        reduce_only: bool = True,
        close_position: bool = False,
        quantity: Optional[float] = None,
    ) -> Dict[str, Any]:
        params = {
            "symbol": symbol,
            "side": side,
            "type": "STOP_MARKET",
            "stopPrice": stop_price,
            "reduceOnly": "true" if reduce_only else "false",
            "workingType": "MARK_PRICE",
        }
        if close_position:
            params["closePosition"] = "true"
        elif quantity is not None:
            params["quantity"] = quantity

        return self._request("POST", "/fapi/v1/order", params, signed=True)

    def place_take_profit_market(
        self,
        symbol: str,
        side: str,
        stop_price: float,
        reduce_only: bool = True,
        close_position: bool = False,
        quantity: Optional[float] = None,
    ) -> Dict[str, Any]:
        params = {
            "symbol": symbol,
            "side": side,
            "type": "TAKE_PROFIT_MARKET",
            "stopPrice": stop_price,
            "reduceOnly": "true" if reduce_only else "false",
            "workingType": "MARK_PRICE",
        }
        if close_position:
            params["closePosition"] = "true"
        elif quantity is not None:
            params["quantity"] = quantity

        return self._request("POST", "/fapi/v1/order", params, signed=True)

    def cancel_order(self, symbol: str, order_id: int) -> Dict[str, Any]:
        return self._request(
            "DELETE",
            "/fapi/v1/order",
            {"symbol": symbol, "orderId": order_id},
            signed=True,
        )

    def cancel_all_orders(self, symbol: str) -> Dict[str, Any]:
        return self._request(
            "DELETE",
            "/fapi/v1/allOpenOrders",
            {"symbol": symbol},
            signed=True,
        )