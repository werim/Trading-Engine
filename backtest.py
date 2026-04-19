from __future__ import annotations

import argparse
import csv
import json
import math
import os
import time
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, Iterable, List, Optional, Tuple

import requests

import adaptive
import binance
import market
import optimizer
try:
    import order_modified as live_order
except ImportError:
    import order as live_order
import risk
import storage
from config import CONFIG
from utils import calc_progress_r, normalize_status, safe_float


INTERVAL_MS = {
    "1m": 60_000,
    "5m": 300_000,
    "15m": 900_000,
    "30m": 1_800_000,
    "1h": 3_600_000,
    "4h": 14_400_000,
    "1d": 86_400_000,
}

DEFAULT_DECISION_INTERVAL = "1h"
DEFAULT_WARMUP_BARS = 260
DEFAULT_FEE_RATE = 0.0004
DEFAULT_SLIPPAGE_PCT = 0.01
MAX_KLINES_PER_CALL = 1500


def _utc_ms(dt: datetime) -> int:
    return int(dt.timestamp() * 1000)


def _ms_to_utc_str(ms: int) -> str:
    return datetime.fromtimestamp(ms / 1000, tz=timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")


def _parse_dt(value: str) -> datetime:
    raw = value.strip()
    if raw.endswith("Z"):
        raw = raw[:-1] + "+00:00"
    try:
        dt = datetime.fromisoformat(raw)
    except ValueError as exc:
        raise ValueError(f"Unsupported datetime format: {value}") from exc
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def _base_rest_url() -> str:
    testnet = os.getenv("BINANCE_TESTNET", "0") == "1"
    return "https://testnet.binancefuture.com" if testnet else "https://fapi.binance.com"


def _ensure_dir(path: str) -> None:
    os.makedirs(path, exist_ok=True)


def _write_csv(path: str, rows: List[Dict[str, Any]], fieldnames: List[str]) -> None:
    _ensure_dir(os.path.dirname(path) or ".")
    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        for row in rows:
            writer.writerow({k: row.get(k, "") for k in fieldnames})


def _write_json(path: str, payload: Dict[str, Any]) -> None:
    _ensure_dir(os.path.dirname(path) or ".")
    with open(path, "w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2, ensure_ascii=False, sort_keys=True)


class BinanceHistoricalClient:
    def __init__(self, timeout: Tuple[int, int] = (5, 30)) -> None:
        self.base_url = _base_rest_url()
        self.session = requests.Session()
        self.timeout = timeout

    def get_klines_range(self, symbol: str, interval: str, start_ms: int, end_ms: int) -> List[Dict[str, Any]]:
        url = f"{self.base_url}/fapi/v1/klines"
        out: List[Dict[str, Any]] = []
        cursor = start_ms
        step_ms = INTERVAL_MS[interval]

        while cursor < end_ms:
            params = {
                "symbol": symbol,
                "interval": interval,
                "startTime": cursor,
                "endTime": end_ms,
                "limit": MAX_KLINES_PER_CALL,
            }
            resp = self.session.get(url, params=params, timeout=self.timeout)
            resp.raise_for_status()
            rows = resp.json()
            if not rows:
                break

            parsed: List[Dict[str, Any]] = []
            for row in rows:
                parsed.append(
                    {
                        "open_time": int(row[0]),
                        "open": safe_float(row[1]),
                        "high": safe_float(row[2]),
                        "low": safe_float(row[3]),
                        "close": safe_float(row[4]),
                        "volume": safe_float(row[5]),
                        "close_time": int(row[6]),
                        "quote_asset_volume": safe_float(row[7]),
                        "trade_count": int(row[8]),
                    }
                )
            out.extend(parsed)

            last_open = int(rows[-1][0])
            next_cursor = last_open + step_ms
            if next_cursor <= cursor:
                break
            cursor = next_cursor
            time.sleep(0.03)

        deduped: List[Dict[str, Any]] = []
        seen = set()
        for row in out:
            key = row["open_time"]
            if key in seen:
                continue
            seen.add(key)
            deduped.append(row)
        return deduped


@dataclass
class PositionState:
    row: Dict[str, Any]
    entry_time_ms: int


@dataclass
class SymbolState:
    open_orders: List[Dict[str, Any]] = field(default_factory=list)
    open_positions: List[PositionState] = field(default_factory=list)
    closed_orders: List[Dict[str, Any]] = field(default_factory=list)
    closed_positions: List[Dict[str, Any]] = field(default_factory=list)
    fills: List[Dict[str, Any]] = field(default_factory=list)
    trade_reviews: List[Dict[str, Any]] = field(default_factory=list)


@dataclass
class BacktestConfig:
    symbols: List[str]
    start_ms: int
    end_ms: int
    decision_interval: str = DEFAULT_DECISION_INTERVAL
    warmup_bars: int = DEFAULT_WARMUP_BARS
    initial_balance: float = 1000.0
    fee_rate: float = DEFAULT_FEE_RATE
    slippage_pct: float = DEFAULT_SLIPPAGE_PCT
    output_dir: str = os.path.join("data", "backtests", "latest")
    top_n: int = 0
    use_optimizer: bool = False
    intrabar_policy: str = "worst"
    decision_on_close: bool = True
    max_hold_bars: int = 0


class Backtester:
    def __init__(self, cfg: BacktestConfig) -> None:
        self.cfg = cfg
        self.client = BinanceHistoricalClient()
        self.symbol_data: Dict[str, Dict[str, List[Dict[str, Any]]]] = {}
        self.symbol_state: Dict[str, SymbolState] = {s: SymbolState() for s in cfg.symbols}
        self.balance_usdt = cfg.initial_balance
        self.equity_curve: List[Dict[str, Any]] = []
        self.weights = optimizer.load_optimizer_weights() if cfg.use_optimizer else {}
        self.summary: Dict[str, Any] = {}

    def load_data(self) -> None:
        max_interval_ms = max(INTERVAL_MS["1h"], INTERVAL_MS["4h"], INTERVAL_MS["1d"])
        warmup_start_ms = self.cfg.start_ms - (self.cfg.warmup_bars * max_interval_ms)
        warmup_start_ms = max(0, warmup_start_ms)

        valid_symbols: List[str] = []

        for symbol in self.cfg.symbols:
            data = {
                "1h": self.client.get_klines_range(symbol, "1h", warmup_start_ms, self.cfg.end_ms),
                "4h": self.client.get_klines_range(symbol, "4h", warmup_start_ms, self.cfg.end_ms),
                "1d": self.client.get_klines_range(symbol, "1d", warmup_start_ms, self.cfg.end_ms),
            }

            rows_1h = data["1h"]
            rows_4h = data["4h"]
            rows_1d = data["1d"]

            if not rows_1h:
                print(f"[backtest] SKIP {symbol}: no 1h data in requested historical window")
                continue

            usable_1h = self._filter_closed(rows_1h, self.cfg.start_ms, "1h")
            usable_4h = self._filter_closed(rows_4h, self.cfg.start_ms, "4h")
            usable_1d = self._filter_closed(rows_1d, self.cfg.start_ms, "1d")

            if len(usable_1h) < 60:
                print(f"[backtest] SKIP {symbol}: insufficient 1h warmup/history before test start ({len(usable_1h)} bars)")
                continue
            if len(usable_4h) < 60:
                print(f"[backtest] SKIP {symbol}: insufficient 4h warmup/history before test start ({len(usable_4h)} bars)")
                continue
            if len(usable_1d) < 60:
                print(f"[backtest] SKIP {symbol}: insufficient 1d warmup/history before test start ({len(usable_1d)} bars)")
                continue

            self.symbol_data[symbol] = data
            valid_symbols.append(symbol)

        self.cfg.symbols = valid_symbols
        self.symbol_state = {s: self.symbol_state.get(s, SymbolState()) for s in self.cfg.symbols}

        if not self.cfg.symbols:
            raise RuntimeError(
                "No symbols with sufficient historical data for the requested window. "
                "Try older symbols, reduce top-n, move the start date forward, or reduce warmup bars."
            )

    def _filter_closed(self, rows: List[Dict[str, Any]], ts_ms: int, interval: str) -> List[Dict[str, Any]]:
        step = INTERVAL_MS[interval]
        return [r for r in rows if int(r["open_time"]) + step <= ts_ms]

    def _build_tf_snapshot(self, rows: List[Dict[str, Any]]) -> Dict[str, Any]:
        indicators = market.build_indicators(rows)
        indicators["regime"] = market.detect_regime(indicators)
        return indicators

    def _sum_quote_volume(self, rows_1h: List[Dict[str, Any]]) -> float:
        tail = rows_1h[-24:] if len(rows_1h) >= 24 else rows_1h
        return sum(safe_float(r.get("quote_asset_volume")) for r in tail)

    def _estimate_spread_pct(self, row_1h: Dict[str, Any]) -> float:
        close = safe_float(row_1h.get("close"))
        high = safe_float(row_1h.get("high"))
        low = safe_float(row_1h.get("low"))
        if close <= 0:
            return 0.0
        candle_range_pct = ((high - low) / close) * 100.0
        return min(CONFIG.FILTER.MAX_SPREAD_PCT, max(0.01, candle_range_pct * 0.03))

    def build_market_context_at(self, symbol: str, ts_ms: int) -> Optional[Dict[str, Any]]:
        data = self.symbol_data[symbol]
        rows_1h = self._filter_closed(data["1h"], ts_ms, "1h")
        rows_4h = self._filter_closed(data["4h"], ts_ms, "4h")
        rows_1d = self._filter_closed(data["1d"], ts_ms, "1d")

        if len(rows_1h) < 60 or len(rows_4h) < 60 or len(rows_1d) < 60:
            return None

        latest_1h = rows_1h[-1]
        close = safe_float(latest_1h["close"])
        spread_pct = self._estimate_spread_pct(latest_1h)
        bid = close * (1 - (spread_pct / 100.0) / 2)
        ask = close * (1 + (spread_pct / 100.0) / 2)

        return {
            "symbol": symbol,
            "last_price": close,
            "bid": bid,
            "ask": ask,
            "spread_pct": spread_pct,
            "volume_24h_usdt": self._sum_quote_volume(rows_1h),
            "funding_rate_pct": 0.0,
            "tf": {
                "1H": self._build_tf_snapshot(rows_1h),
                "4H": self._build_tf_snapshot(rows_4h),
                "1D": self._build_tf_snapshot(rows_1d),
            },
        }

    def _current_decision_rows(self, symbol: str) -> List[Dict[str, Any]]:
        return self.symbol_data[symbol][self.cfg.decision_interval]

    def _decision_points(self) -> List[int]:
        any_symbol = self.cfg.symbols[0]
        rows = self._current_decision_rows(any_symbol)
        step = INTERVAL_MS[self.cfg.decision_interval]
        return [int(r["open_time"]) + step for r in rows if self.cfg.start_ms <= int(r["open_time"]) + step <= self.cfg.end_ms]

    def _position_notional(self, position: Dict[str, Any]) -> float:
        return safe_float(position.get("entry")) * safe_float(position.get("qty"))

    def _position_fees(self, entry: float, exit_price: float, qty: float) -> float:
        return (entry * qty * self.cfg.fee_rate) + (exit_price * qty * self.cfg.fee_rate)

    def _fill_price(self, side: str, trigger: float) -> float:
        slip = self.cfg.slippage_pct / 100.0
        if side == "LONG":
            return trigger * (1 + slip)
        return trigger * (1 - slip)

    def _exit_price(self, side: str, raw: float, reason: str) -> float:
        slip = self.cfg.slippage_pct / 100.0
        if reason == "SL_HIT":
            if side == "LONG":
                return raw * (1 - slip)
            return raw * (1 + slip)
        if reason == "TP_HIT":
            if side == "LONG":
                return raw * (1 - slip)
            return raw * (1 + slip)
        return raw

    def _review_fieldnames(self) -> List[str]:
        return adaptive._review_fieldnames()

    def _record_closed_order_review(self, state: SymbolState, row: Dict[str, Any]) -> None:
        adaptive_row = {
            "review_id": f"ORDER-{row.get('order_id', '')}-{row.get('close_reason', row.get('status', ''))}",
            "source": "ORDER",
            "closed_at": row.get("closed_at") or row.get("updated_at") or _ms_to_utc_str(self.cfg.end_ms),
            "symbol": row.get("symbol", ""),
            "side": row.get("side", ""),
            "setup_type": row.get("setup_type", ""),
            "setup_reason": row.get("setup_reason", ""),
            "tf_context": row.get("tf_context", ""),
            "score": row.get("score", 0),
            "rr": row.get("rr", 0),
            "entry": row.get("entry_trigger", 0),
            "sl": row.get("sl", 0),
            "tp": row.get("tp", 0),
            "close_reason": row.get("close_reason") or row.get("status", ""),
            "status_bucket": "ORDER_CANCELLED",
            "net_pnl_pct": 0,
            "net_pnl_usdt": 0,
            "fees_usdt": 0,
            "hold_minutes": 0,
            "order_id": row.get("order_id", ""),
            "position_id": "",
            "volume_24h_usdt": row.get("volume_24h_usdt", 0),
            "spread_pct": row.get("spread_pct", 0),
            "funding_rate_pct": row.get("funding_rate_pct", 0),
            "scenario_name": row.get("scenario_name", ""),
            "scenario_probability": row.get("scenario_probability", 0.0),
        }
        state.trade_reviews.append(adaptive_row)

    def _record_closed_position_review(self, state: SymbolState, row: Dict[str, Any]) -> None:
        adaptive_row = {
            "review_id": f"POSITION-{row.get('position_id', '')}-{row.get('close_reason', '')}",
            "source": "POSITION",
            "closed_at": row.get("closed_at", ""),
            "symbol": row.get("symbol", ""),
            "side": row.get("side", ""),
            "setup_type": row.get("setup_type", ""),
            "setup_reason": row.get("setup_reason", ""),
            "tf_context": row.get("tf_context", ""),
            "score": row.get("score", 0),
            "rr": row.get("rr", 0),
            "entry": row.get("entry", 0),
            "sl": row.get("sl", 0),
            "tp": row.get("tp", 0),
            "close_reason": row.get("close_reason", ""),
            "status_bucket": "POSITION_CLOSED",
            "net_pnl_pct": row.get("net_pnl_pct", 0),
            "net_pnl_usdt": row.get("net_pnl_usdt", 0),
            "fees_usdt": row.get("fees_usdt", 0),
            "hold_minutes": row.get("hold_minutes", 0),
            "order_id": row.get("order_id", ""),
            "position_id": row.get("position_id", ""),
            "volume_24h_usdt": row.get("volume_24h_usdt", 0),
            "spread_pct": row.get("spread_pct", 0),
            "funding_rate_pct": row.get("funding_rate_pct", 0),
            "scenario_name": row.get("scenario_name", ""),
            "scenario_probability": row.get("scenario_probability", 0.0),
        }
        state.trade_reviews.append(adaptive_row)

    def _mark_order_closed(self, state: SymbolState, order_row: Dict[str, Any], reason: str, ts_ms: int) -> None:
        order_row["status"] = reason
        order_row["close_reason"] = reason
        order_row["closed_at"] = _ms_to_utc_str(ts_ms)
        order_row["updated_at"] = _ms_to_utc_str(ts_ms)
        state.closed_orders.append(dict(order_row))
        self._record_closed_order_review(state, order_row)

    def _close_position(self, state: SymbolState, position_state: PositionState, reason: str, close_price: float, ts_ms: int) -> None:
        row = dict(position_state.row)
        entry = safe_float(row.get("entry"))
        qty = safe_float(row.get("qty"))
        side = row.get("side", "")
        exit_price = self._exit_price(side, close_price, reason)

        if side == "LONG":
            gross_pnl_usdt = (exit_price - entry) * qty
        else:
            gross_pnl_usdt = (entry - exit_price) * qty

        fees_usdt = self._position_fees(entry, exit_price, qty)
        net_pnl_usdt = gross_pnl_usdt - fees_usdt
        notional = entry * qty
        net_pnl_pct = (net_pnl_usdt / notional) * 100.0 if notional > 0 else 0.0
        pnl_pct = ((exit_price - entry) / entry * 100.0) if side == "LONG" else ((entry - exit_price) / entry * 100.0)

        row["live_price"] = exit_price
        row["pnl_pct"] = pnl_pct
        row["fees_usdt"] = fees_usdt
        row["net_pnl_usdt"] = net_pnl_usdt
        row["net_pnl_pct"] = net_pnl_pct
        row["status"] = "CLOSED"
        row["closed_at"] = _ms_to_utc_str(ts_ms)
        row["close_reason"] = reason
        row["close_price"] = exit_price
        row["updated_at"] = _ms_to_utc_str(ts_ms)
        row["hold_minutes"] = max(0.0, (ts_ms - position_state.entry_time_ms) / 60000.0)

        self.balance_usdt += net_pnl_usdt
        state.closed_positions.append(row)
        self._record_closed_position_review(state, row)

    def _update_position_live_metrics(self, position: Dict[str, Any], close_price: float) -> None:
        entry = safe_float(position.get("entry"))
        qty = safe_float(position.get("qty"))
        if entry <= 0 or qty <= 0 or close_price <= 0:
            return

        side = position.get("side", "")
        position["live_price"] = close_price
        if side == "LONG":
            gross_pnl_usdt = (close_price - entry) * qty
            position["highest_price"] = max(safe_float(position.get("highest_price")), close_price)
            low_seen = safe_float(position.get("lowest_price"))
            position["lowest_price"] = close_price if low_seen <= 0 else min(low_seen, close_price)
            pnl_pct = ((close_price - entry) / entry) * 100.0
        else:
            gross_pnl_usdt = (entry - close_price) * qty
            position["lowest_price"] = min(safe_float(position.get("lowest_price")) or close_price, close_price)
            position["highest_price"] = max(safe_float(position.get("highest_price")), close_price)
            pnl_pct = ((entry - close_price) / entry) * 100.0

        fees_usdt = self._position_fees(entry, close_price, qty)
        net_pnl_usdt = gross_pnl_usdt - fees_usdt
        notional = entry * qty
        position["pnl_pct"] = pnl_pct
        position["fees_usdt"] = fees_usdt
        position["net_pnl_usdt"] = net_pnl_usdt
        position["net_pnl_pct"] = (net_pnl_usdt / notional) * 100.0 if notional > 0 else 0.0

    def _maybe_partial(self, position: Dict[str, Any], close_price: float) -> None:
        if str(position.get("partial_taken")) == "1":
            return
        progress_r = calc_progress_r(
            entry=safe_float(position["entry"]),
            sl=safe_float(position["sl"]),
            live_price=close_price,
            side=position["side"],
        )
        if progress_r < CONFIG.TRADE.PARTIAL_TP_AT_R:
            return
        current_qty = safe_float(position.get("qty"))
        close_qty = current_qty * CONFIG.TRADE.PARTIAL_CLOSE_RATIO
        remaining_qty = current_qty - close_qty
        if remaining_qty <= 0:
            return
        position["qty"] = remaining_qty
        position["partial_taken"] = 1
        position["status"] = "PARTIAL_TP_DONE"

    def _maybe_break_even(self, position: Dict[str, Any], close_price: float) -> None:
        if str(position.get("break_even_armed")) == "1":
            return
        progress_r = calc_progress_r(
            entry=safe_float(position["entry"]),
            sl=safe_float(position["sl"]),
            live_price=close_price,
            side=position["side"],
        )
        if progress_r < CONFIG.TRADE.BREAK_EVEN_TRIGGER_R:
            return
        position["sl"] = safe_float(position.get("entry"))
        position["break_even_armed"] = 1
        position["status"] = "BREAK_EVEN_ARMED"

    def _maybe_trail(self, position: Dict[str, Any], close_price: float) -> None:
        if not CONFIG.TRADE.ENABLE_TRAILING:
            return
        progress_r = calc_progress_r(
            entry=safe_float(position["entry"]),
            sl=safe_float(position["sl"]),
            live_price=close_price,
            side=position["side"],
        )
        if progress_r < CONFIG.TRADE.TRAIL_AFTER_R:
            return

        entry = safe_float(position["entry"])
        current_sl = safe_float(position["sl"])
        base_risk = abs(entry - current_sl)
        if base_risk <= 0:
            return

        trail_buffer = base_risk * 0.5
        if position["side"] == "LONG":
            new_sl = max(current_sl, close_price - trail_buffer)
        else:
            new_sl = min(current_sl, close_price + trail_buffer)

        if new_sl == current_sl:
            return
        position["sl"] = new_sl
        position["trailing_active"] = 1
        position["status"] = "TRAILING_ACTIVE"

    def _position_close_signal(self, position: Dict[str, Any], candle: Dict[str, Any]) -> Optional[Tuple[str, float]]:
        side = position.get("side", "")
        sl = safe_float(position.get("sl"))
        tp = safe_float(position.get("tp"))
        high = safe_float(candle.get("high"))
        low = safe_float(candle.get("low"))

        if side == "LONG":
            hit_sl = low <= sl if sl > 0 else False
            hit_tp = high >= tp if tp > 0 else False
        else:
            hit_sl = high >= sl if sl > 0 else False
            hit_tp = low <= tp if tp > 0 else False

        if hit_sl and hit_tp:
            if self.cfg.intrabar_policy == "best":
                return "TP_HIT", tp
            return "SL_HIT", sl
        if hit_sl:
            return "SL_HIT", sl
        if hit_tp:
            return "TP_HIT", tp
        return None

    def _maybe_timeout_position(self, position_state: PositionState, ts_ms: int) -> Optional[Tuple[str, float]]:
        if self.cfg.max_hold_bars <= 0:
            return None
        held_ms = ts_ms - position_state.entry_time_ms
        max_hold_ms = self.cfg.max_hold_bars * INTERVAL_MS[self.cfg.decision_interval]
        if held_ms < max_hold_ms:
            return None
        return "TIME_EXIT", safe_float(position_state.row.get("live_price")) or safe_float(position_state.row.get("entry"))

    def _process_orders_for_candle(self, symbol: str, ts_ms: int, candle: Dict[str, Any], market_ctx: Dict[str, Any]) -> None:
        state = self.symbol_state[symbol]
        survivors: List[Dict[str, Any]] = []

        for row in state.open_orders:
            status = normalize_status(row.get("status"))
            if status in live_order.FINAL_ORDER_STATUSES:
                self._mark_order_closed(state, row, status, ts_ms)
                continue

            low = safe_float(candle.get("low"))
            high = safe_float(candle.get("high"))
            zone_low = min(safe_float(row.get("entry_zone_low")), safe_float(row.get("entry_zone_high")))
            zone_high = max(safe_float(row.get("entry_zone_low")), safe_float(row.get("entry_zone_high")))
            trigger = safe_float(row.get("entry_trigger"))
            side = row.get("side", "")

            row["live_price"] = safe_float(candle.get("close"))
            row["updated_at"] = _ms_to_utc_str(ts_ms)

            if status in {"WATCHING", "PLANNED"} and high >= zone_low and low <= zone_high:
                row["zone_touched"] = 1
                row["status"] = "READY"
                status = "READY"

            if status == "READY":
                symbol_meta = binance.get_symbol_meta(symbol)
                ok, reason = live_order.maybe_reject_invalid_symbol_meta(row, symbol_meta)
                if not ok:
                    row["status"] = "REJECTED"
                    row["exchange_status"] = reason
                    self._mark_order_closed(state, row, "REJECTED", ts_ms)
                    continue

                qty = risk.calc_position_size(
                    entry=trigger,
                    sl=safe_float(row.get("sl")),
                    account_balance=self.balance_usdt,
                    risk_pct=CONFIG.TRADE.RISK_PER_TRADE_PCT,
                    symbol_meta=symbol_meta,
                )
                if qty <= 0:
                    row["status"] = "REJECTED"
                    row["exchange_status"] = "QTY_LE_ZERO"
                    self._mark_order_closed(state, row, "REJECTED", ts_ms)
                    continue

                row["submitted_qty"] = qty
                row["status"] = "NEW"
                row["exchange_status"] = "BACKTEST_NEW"

            if normalize_status(row.get("status")) == "NEW":
                should_fill = (side == "LONG" and low <= trigger) or (side == "SHORT" and high >= trigger)
                if should_fill:
                    fill_price = self._fill_price(side, trigger)
                    row["status"] = "FILLED"
                    row["exchange_status"] = "BACKTEST_FILLED"
                    row["executed_qty"] = safe_float(row.get("submitted_qty"))
                    row["avg_fill_price"] = fill_price
                    row["updated_at"] = _ms_to_utc_str(ts_ms)
                    state.fills.append(
                        {
                            "symbol": symbol,
                            "side": side,
                            "qty": row["executed_qty"],
                            "price": fill_price,
                            "order_id": row.get("order_id", ""),
                            "exchange_order_id": row.get("exchange_order_id", ""),
                            "filled_at": _ms_to_utc_str(ts_ms),
                        }
                    )
                else:
                    survivors.append(row)
                    continue

            if normalize_status(row.get("status")) == "FILLED":
                position = self._build_position_from_filled_order(row, ts_ms)
                if position is not None:
                    state.open_positions.append(position)
                row["closed_at"] = _ms_to_utc_str(ts_ms)
                row["close_reason"] = "FILLED"
                state.closed_orders.append(dict(row))
                continue

            survivors.append(row)

        state.open_orders = [r for r in survivors if normalize_status(r.get("status")) not in live_order.FINAL_ORDER_STATUSES]

    def _build_position_from_filled_order(self, order_row: Dict[str, Any], ts_ms: int) -> Optional[PositionState]:
        position = {
            "position_id": f"bt-{order_row['symbol']}-{order_row['side']}-{ts_ms}",
            "order_id": order_row.get("order_id", ""),
            "symbol": order_row.get("symbol", ""),
            "side": order_row.get("side", ""),
            "entry": safe_float(order_row.get("avg_fill_price")) or safe_float(order_row.get("entry_trigger")),
            "qty": safe_float(order_row.get("executed_qty")) or safe_float(order_row.get("submitted_qty")),
            "sl": safe_float(order_row.get("sl")),
            "tp": safe_float(order_row.get("tp")),
            "rr": safe_float(order_row.get("rr")),
            "score": order_row.get("score", 0),
            "tf_context": order_row.get("tf_context", ""),
            "setup_type": order_row.get("setup_type", ""),
            "setup_reason": order_row.get("setup_reason", ""),
            "opened_at": _ms_to_utc_str(ts_ms),
            "updated_at": _ms_to_utc_str(ts_ms),
            "status": "PROTECTION_ARMED",
            "live_price": safe_float(order_row.get("avg_fill_price")) or safe_float(order_row.get("entry_trigger")),
            "pnl_pct": 0.0,
            "net_pnl_pct": 0.0,
            "net_pnl_usdt": 0.0,
            "fees_usdt": 0.0,
            "sl_order_id": "bt-sl",
            "tp_order_id": "bt-tp",
            "protection_armed": 1,
            "partial_taken": 0,
            "break_even_armed": 0,
            "trailing_active": 0,
            "highest_price": safe_float(order_row.get("avg_fill_price")) or safe_float(order_row.get("entry_trigger")),
            "lowest_price": safe_float(order_row.get("avg_fill_price")) or safe_float(order_row.get("entry_trigger")),
            "initial_qty": safe_float(order_row.get("executed_qty")) or safe_float(order_row.get("submitted_qty")),
            "initial_risk": abs((safe_float(order_row.get("avg_fill_price")) or safe_float(order_row.get("entry_trigger"))) - safe_float(order_row.get("sl"))) * (safe_float(order_row.get("executed_qty")) or safe_float(order_row.get("submitted_qty"))),
            "volume_24h_usdt": order_row.get("volume_24h_usdt", 0),
            "spread_pct": order_row.get("spread_pct", 0),
            "funding_rate_pct": order_row.get("funding_rate_pct", 0),
            "scenario_name": order_row.get("scenario_name", ""),
            "scenario_probability": order_row.get("scenario_probability", 0.0),
        }
        if not position["symbol"] or not position["side"] or safe_float(position["entry"]) <= 0 or safe_float(position["qty"]) <= 0:
            return None
        return PositionState(row=position, entry_time_ms=ts_ms)

    def _process_positions_for_candle(self, symbol: str, ts_ms: int, candle: Dict[str, Any]) -> None:
        state = self.symbol_state[symbol]
        survivors: List[PositionState] = []

        for position_state in state.open_positions:
            position = position_state.row
            signal = self._position_close_signal(position, candle)
            if signal is not None:
                reason, price = signal
                self._close_position(state, position_state, reason, price, ts_ms)
                continue

            self._update_position_live_metrics(position, safe_float(candle.get("close")))
            self._maybe_partial(position, safe_float(candle.get("close")))
            self._maybe_break_even(position, safe_float(candle.get("close")))
            self._maybe_trail(position, safe_float(candle.get("close")))
            timeout_signal = self._maybe_timeout_position(position_state, ts_ms)
            if timeout_signal is not None:
                reason, price = timeout_signal
                self._close_position(state, position_state, reason, price, ts_ms)
                continue

            position["updated_at"] = _ms_to_utc_str(ts_ms)
            if normalize_status(position.get("status")) not in {"PROTECTION_ARMED", "PARTIAL_TP_DONE", "BREAK_EVEN_ARMED", "TRAILING_ACTIVE", "OPEN_POSITION"}:
                position["status"] = "OPEN_POSITION"
            survivors.append(position_state)

        state.open_positions = survivors

    def _active_symbols(self) -> set[str]:
        out = set()
        for symbol, state in self.symbol_state.items():
            if state.open_orders or state.open_positions:
                out.add(symbol)
        return out

    def _create_candidate(self, symbol: str, market_ctx: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        candidate = live_order.build_order_candidate(symbol, market_ctx)
        if not candidate:
            return None

        candidate["score"] = live_order.score_candidate(candidate, market_ctx)
        candidate["expected_net_pnl_pct"] = live_order.estimate_expected_net_pnl_pct(candidate)
        candidate["stop_net_loss_pct"] = live_order.estimate_stop_net_loss_pct(candidate)

        if self.cfg.use_optimizer:
            candidate = optimizer.apply_optimizer_to_candidate(candidate, weights=self.weights)
        else:
            candidate = adaptive.apply_adaptive_scoring(candidate, reviews=[])

        if int(candidate.get("adaptive_blocked", 0)) == 1:
            return None

        ok, _ = live_order.passes_order_filters(candidate)
        if not ok:
            return None

        state = self.symbol_state[symbol]
        ok, _ = risk.can_open_new_order(candidate, state.open_orders, [p.row for p in state.open_positions])
        if not ok:
            return None

        ok, _ = live_order.check_duplicate_order(candidate, state.open_orders, [p.row for p in state.open_positions])
        if not ok:
            return None

        candidate["created_at"] = _ms_to_utc_str(market_ctx["tf"]["1H"].get("last_open_time_ms", self.cfg.start_ms)) if False else _ms_to_utc_str(int(self.current_ts_ms))
        candidate["updated_at"] = _ms_to_utc_str(int(self.current_ts_ms))
        candidate["status"] = "WATCHING"
        candidate["zone_touched"] = 0
        return candidate

    def _record_equity(self, ts_ms: int) -> None:
        open_pnl = 0.0
        open_positions_count = 0
        for state in self.symbol_state.values():
            for position_state in state.open_positions:
                open_pnl += safe_float(position_state.row.get("net_pnl_usdt"))
                open_positions_count += 1

        self.equity_curve.append(
            {
                "timestamp": _ms_to_utc_str(ts_ms),
                "equity_usdt": round(self.balance_usdt + open_pnl, 8),
                "balance_usdt": round(self.balance_usdt, 8),
                "open_pnl_usdt": round(open_pnl, 8),
                "closed_pnl_usdt": round(self.balance_usdt - self.cfg.initial_balance, 8),
                "fees_usdt": round(self._sum_fees(), 8),
                "open_positions": open_positions_count,
                "note": "BACKTEST",
            }
        )

    def _sum_fees(self) -> float:
        total = 0.0
        for state in self.symbol_state.values():
            for row in state.closed_positions:
                total += safe_float(row.get("fees_usdt"))
            for position_state in state.open_positions:
                total += safe_float(position_state.row.get("fees_usdt"))
        return total

    def run(self) -> Dict[str, Any]:
        self.load_data()
        decision_points = self._decision_points()
        if not decision_points:
            raise RuntimeError("No decision points found inside selected period")

        symbol_to_1h = {s: {int(r['open_time']) + INTERVAL_MS['1h']: r for r in self.symbol_data[s]['1h']} for s in self.cfg.symbols}

        for ts_ms in decision_points:
            self.current_ts_ms = ts_ms
            for symbol in self.cfg.symbols:
                candle = symbol_to_1h[symbol].get(ts_ms)
                if not candle:
                    continue
                market_ctx = self.build_market_context_at(symbol, ts_ms)
                if market_ctx is None:
                    continue
                self._process_orders_for_candle(symbol, ts_ms, candle, market_ctx)
                self._process_positions_for_candle(symbol, ts_ms, candle)

            active_symbols = self._active_symbols()
            for symbol in self.cfg.symbols:
                if symbol in active_symbols:
                    continue
                state = self.symbol_state[symbol]
                if len(state.open_orders) >= CONFIG.TRADE.MAX_OPEN_ORDERS:
                    continue
                market_ctx = self.build_market_context_at(symbol, ts_ms)
                if market_ctx is None:
                    continue
                candidate = self._create_candidate(symbol, market_ctx)
                if candidate:
                    state.open_orders.append(candidate)

            self._record_equity(ts_ms)

        self._force_close_remaining()
        self.summary = self._build_summary()
        self._persist_outputs()
        return self.summary

    def _force_close_remaining(self) -> None:
        final_ts = self.cfg.end_ms
        for symbol in self.cfg.symbols:
            state = self.symbol_state[symbol]
            data_1h = self.symbol_data[symbol]["1h"]
            last_close = safe_float(data_1h[-1]["close"]) if data_1h else 0.0
            for row in state.open_orders:
                if normalize_status(row.get("status")) in {"WATCHING", "READY", "NEW", "PARTIALLY_FILLED", "PLANNED"}:
                    self._mark_order_closed(state, row, "EXPIRED", final_ts)
            state.open_orders = []

            for position_state in list(state.open_positions):
                self._close_position(state, position_state, "TIME_EXIT", last_close or safe_float(position_state.row.get("entry")), final_ts)
            state.open_positions = []

    def _build_summary(self) -> Dict[str, Any]:
        closed_positions = [row for s in self.symbol_state.values() for row in s.closed_positions]
        closed_orders = [row for s in self.symbol_state.values() for row in s.closed_orders]
        trade_reviews = [row for s in self.symbol_state.values() for row in s.trade_reviews]

        tp_hits = sum(1 for row in closed_positions if str(row.get("close_reason", "")).upper() == "TP_HIT")
        sl_hits = sum(1 for row in closed_positions if str(row.get("close_reason", "")).upper() == "SL_HIT")
        time_exits = sum(1 for row in closed_positions if str(row.get("close_reason", "")).upper() == "TIME_EXIT")
        cancelled_orders = sum(1 for row in closed_orders if str(row.get("close_reason", "")).upper() in {"CANCELLED", "EXPIRED", "REJECTED", "FAILED"})
        net_pnl_usdt = sum(safe_float(row.get("net_pnl_usdt")) for row in closed_positions)
        fees_usdt = sum(safe_float(row.get("fees_usdt")) for row in closed_positions)
        trades = len(closed_positions)
        wins = sum(1 for row in closed_positions if safe_float(row.get("net_pnl_usdt")) > 0)
        losses = sum(1 for row in closed_positions if safe_float(row.get("net_pnl_usdt")) < 0)
        max_equity = self.cfg.initial_balance
        max_drawdown_pct = 0.0
        for row in self.equity_curve:
            equity = safe_float(row.get("equity_usdt"))
            max_equity = max(max_equity, equity)
            if max_equity > 0:
                dd = (max_equity - equity) / max_equity * 100.0
                max_drawdown_pct = max(max_drawdown_pct, dd)

        opt_snapshot = optimizer.build_optimizer_snapshot(reviews=trade_reviews)
        opt_summary = optimizer.summarize_snapshot(opt_snapshot)

        return {
            "generated_at": _ms_to_utc_str(self.cfg.end_ms),
            "symbols": self.cfg.symbols,
            "start": _ms_to_utc_str(self.cfg.start_ms),
            "end": _ms_to_utc_str(self.cfg.end_ms),
            "warmup_bars": self.cfg.warmup_bars,
            "initial_balance_usdt": self.cfg.initial_balance,
            "final_balance_usdt": round(self.balance_usdt, 8),
            "net_pnl_usdt": round(net_pnl_usdt, 8),
            "net_return_pct": round(((self.balance_usdt - self.cfg.initial_balance) / self.cfg.initial_balance) * 100.0, 4) if self.cfg.initial_balance > 0 else 0.0,
            "fees_usdt": round(fees_usdt, 8),
            "trades": trades,
            "wins": wins,
            "losses": losses,
            "win_rate": round((wins / trades) * 100.0, 4) if trades > 0 else 0.0,
            "tp_hits": tp_hits,
            "sl_hits": sl_hits,
            "time_exits": time_exits,
            "cancelled_orders": cancelled_orders,
            "max_drawdown_pct": round(max_drawdown_pct, 4),
            "review_count": len(trade_reviews),
            "optimizer_summary": opt_summary,
        }

    def _persist_outputs(self) -> None:
        out_dir = self.cfg.output_dir
        _ensure_dir(out_dir)

        closed_orders = [row for s in self.symbol_state.values() for row in s.closed_orders]
        closed_positions = [row for s in self.symbol_state.values() for row in s.closed_positions]
        fills = [row for s in self.symbol_state.values() for row in s.fills]
        reviews = [row for s in self.symbol_state.values() for row in s.trade_reviews]

        closed_order_fields = list(storage.OPEN_ORDER_FIELDS)
        for extra in ["closed_at", "close_reason"]:
            if extra not in closed_order_fields:
                closed_order_fields.append(extra)

        closed_position_fields = list(storage.CLOSED_POSITION_FIELDS)
        if "hold_minutes" not in closed_position_fields:
            closed_position_fields.append("hold_minutes")
        for extra in ["volume_24h_usdt", "spread_pct", "funding_rate_pct"]:
            if extra not in closed_position_fields:
                closed_position_fields.append(extra)

        _write_csv(os.path.join(out_dir, "closed_orders.csv"), closed_orders, closed_order_fields)
        _write_csv(os.path.join(out_dir, "closed_positions.csv"), closed_positions, closed_position_fields)
        _write_csv(os.path.join(out_dir, "fills.csv"), fills, storage.FILL_FIELDS)
        _write_csv(os.path.join(out_dir, "equity.csv"), self.equity_curve, storage.EQUITY_FIELDS)
        _write_csv(os.path.join(out_dir, "trade_reviews.csv"), reviews, self._review_fieldnames())
        _write_json(os.path.join(out_dir, "summary.json"), self.summary)

        snapshot = optimizer.build_optimizer_snapshot(reviews=reviews)
        _write_json(os.path.join(out_dir, "optimizer_snapshot.json"), snapshot)
        _write_json(
            os.path.join(out_dir, "optimizer_weights.json"),
            {
                "generated_at": snapshot.get("generated_at"),
                "review_count": snapshot.get("review_count"),
                "setup_weights": snapshot.get("setup_weights"),
            },
        )


def _resolve_symbols(args: argparse.Namespace) -> List[str]:
    if args.symbols:
        return [s.strip().upper() for s in args.symbols.split(",") if s.strip()]
    if args.symbol_file:
        with open(args.symbol_file, "r", encoding="utf-8") as f:
            return [line.strip().upper() for line in f if line.strip()]
    if args.top_n > 0:
        return binance.get_top_symbols_by_volume(args.top_n)
    raise ValueError("Provide --symbols, --symbol-file, or --top-n")


def build_arg_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description="Historical backtester with warmup bars for indicator safety")
    p.add_argument("--symbols", default="", help="Comma separated symbols, e.g. BTCUSDT,ETHUSDT")
    p.add_argument("--symbol-file", default="", help="Text file with one symbol per line")
    p.add_argument("--top-n", type=int, default=0, help="Use current top N Binance futures symbols by volume")
    p.add_argument("--start", required=True, help="UTC start datetime, e.g. 2025-01-01T00:00:00Z")
    p.add_argument("--end", required=True, help="UTC end datetime, e.g. 2025-03-01T00:00:00Z")
    p.add_argument("--warmup-bars", type=int, default=DEFAULT_WARMUP_BARS, help="Preload bars per timeframe before test start")
    p.add_argument("--initial-balance", type=float, default=1000.0)
    p.add_argument("--output-dir", default=os.path.join("data", "backtests", f"run_{datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')}"))
    p.add_argument("--fee-rate", type=float, default=DEFAULT_FEE_RATE, help="Per-side fee rate. 0.0004 = 4 bps")
    p.add_argument("--slippage-pct", type=float, default=DEFAULT_SLIPPAGE_PCT, help="Entry/exit slippage in percent")
    p.add_argument("--use-optimizer", action="store_true", help="Apply existing optimizer weights during candidate scoring")
    p.add_argument("--intrabar-policy", choices=["worst", "best"], default="worst", help="If both SL and TP touch in same candle")
    p.add_argument("--max-hold-bars", type=int, default=0, help="Optional force exit after N decision bars")
    return p


def main() -> None:
    args = build_arg_parser().parse_args()
    start_dt = _parse_dt(args.start)
    end_dt = _parse_dt(args.end)
    if end_dt <= start_dt:
        raise ValueError("--end must be after --start")

    symbols = _resolve_symbols(args)
    cfg = BacktestConfig(
        symbols=symbols,
        start_ms=_utc_ms(start_dt),
        end_ms=_utc_ms(end_dt),
        warmup_bars=args.warmup_bars,
        initial_balance=args.initial_balance,
        fee_rate=args.fee_rate,
        slippage_pct=args.slippage_pct,
        output_dir=args.output_dir,
        top_n=args.top_n,
        use_optimizer=bool(args.use_optimizer),
        intrabar_policy=args.intrabar_policy,
        max_hold_bars=args.max_hold_bars,
    )

    bt = Backtester(cfg)
    summary = bt.run()
    print(json.dumps(summary, indent=2, ensure_ascii=False))


if __name__ == "__main__":
    main()
