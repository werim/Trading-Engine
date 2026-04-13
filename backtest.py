import argparse
import csv
import math
import os
from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, DefaultDict, Dict, Iterable, List, Optional, Sequence, Tuple

from binance_real import BinanceFuturesClient
from config import CONFIG
from order import candidate_to_order
from utils import clamp

"""
örnek giriş:

python backtest.py \
  --symbols BTCUSDT,ETHUSDT,SOLUSDT,BNBUSDT,XRPUSDT,DOGEUSDT,ADAUSDT,LINKUSDT,AVAXUSDT \
  --start-date 2025-04-01 \
  --end-date 2026-04-13 \
  --interval 1d \
  --rr-values 1.4,1.6,1.8,2.0,2.2 \
  --sl-multipliers 0.8,1.0,1.2,1.4 \
  --drawdown-penalty 0.7 \
  --best-env-out backtest/4h_backtest.env
  
"""

@dataclass(frozen=True)
class TrailingSetting:
    name: str
    enabled: bool
    break_even_trigger_r: float
    trail_after_r: float
    trail_factor: float


@dataclass(frozen=True)
class BacktestConfig:
    rr_target: float
    sl_multiplier: float
    trailing: TrailingSetting


@dataclass
class Candle:
    open_time: int
    open: float
    high: float
    low: float
    close: float
    volume: float


@dataclass
class PositionState:
    symbol: str
    side: str
    entry: float
    sl: float
    tp: float
    qty: float
    opened_at: int
    rr: float
    score: int
    setup_type: str
    break_even_armed: bool = False
    highest_price: float = 0.0
    lowest_price: float = 0.0


def _ema(values: List[float], period: int) -> List[float]:
    if not values:
        return []
    alpha = 2.0 / (period + 1)
    out = [values[0]]
    for val in values[1:]:
        out.append((alpha * val) + ((1.0 - alpha) * out[-1]))
    return out


def _atr(candles: List[Candle], period: int) -> float:
    if len(candles) < period + 1:
        return 0.0

    trs: List[float] = []
    for idx in range(1, len(candles)):
        cur = candles[idx]
        prev = candles[idx - 1]
        tr = max(cur.high - cur.low, abs(cur.high - prev.close), abs(cur.low - prev.close))
        trs.append(tr)

    if len(trs) < period:
        return 0.0
    return sum(trs[-period:]) / period


def _compute_rr(entry: float, sl: float, tp: float) -> float:
    risk = abs(entry - sl)
    reward = abs(tp - entry)
    return reward / risk if risk > 0 else 0.0


def _pct_change(entry: float, price: float, side: str) -> float:
    if entry <= 0:
        return 0.0
    if side == "LONG":
        return ((price - entry) / entry) * 100.0
    return ((entry - price) / entry) * 100.0


def _fees_pct() -> float:
    return CONFIG.TRADE.MAKER_FEE_PCT + CONFIG.TRADE.TAKER_FEE_PCT + CONFIG.TRADE.ROUND_TRIP_SLIPPAGE_PCT


def _parse_kline_row(row: Sequence[Any]) -> Candle:
    return Candle(
        open_time=int(row[0]),
        open=float(row[1]),
        high=float(row[2]),
        low=float(row[3]),
        close=float(row[4]),
        volume=float(row[5]),
    )


def _cache_path(symbol: str, interval: str, start_ms: int, end_ms: int) -> str:
    os.makedirs("data/backtest_cache", exist_ok=True)
    return f"data/backtest_cache/{symbol}_{interval}_{start_ms}_{end_ms}.csv"


def _read_cached_klines(path: str) -> List[Candle]:
    if not os.path.exists(path):
        return []

    rows: List[Candle] = []
    with open(path, "r", encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f)
        for r in reader:
            rows.append(
                Candle(
                    open_time=int(r["open_time"]),
                    open=float(r["open"]),
                    high=float(r["high"]),
                    low=float(r["low"]),
                    close=float(r["close"]),
                    volume=float(r["volume"]),
                )
            )
    return rows


def _write_cached_klines(path: str, rows: List[Candle]) -> None:
    with open(path, "w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=["open_time", "open", "high", "low", "close", "volume"])
        writer.writeheader()
        for c in rows:
            writer.writerow(
                {
                    "open_time": c.open_time,
                    "open": c.open,
                    "high": c.high,
                    "low": c.low,
                    "close": c.close,
                    "volume": c.volume,
                }
            )


def _fetch_historical_klines(
        client: BinanceFuturesClient,
        symbol: str,
        interval: str,
        start_ms: int,
        end_ms: int,
) -> List[Candle]:
    cache_file = _cache_path(symbol, interval, start_ms, end_ms)
    cached = _read_cached_klines(cache_file)
    if cached:
        return cached

    out: List[Candle] = []
    cursor = start_ms

    while cursor < end_ms:
        rows = client._request(
            "GET",
            "/fapi/v1/klines",
            {
                "symbol": symbol,
                "interval": interval,
                "startTime": cursor,
                "endTime": end_ms,
                "limit": 1000,
            },
        )

        if not rows:
            break

        chunk = [_parse_kline_row(r) for r in rows]
        out.extend(chunk)

        last_open = chunk[-1].open_time
        if last_open <= cursor:
            break
        cursor = last_open + 1

        if len(rows) < 1000:
            break

    out = sorted({c.open_time: c for c in out}.values(), key=lambda c: c.open_time)
    _write_cached_klines(cache_file, out)
    return out


def _symbol_meta(client: BinanceFuturesClient, symbols: Sequence[str]) -> Dict[str, Dict[str, float]]:
    info = client.get_exchange_info()
    wanted = set(symbols)
    out: Dict[str, Dict[str, float]] = {}

    for sym in info.get("symbols", []):
        s = sym.get("symbol")
        if s not in wanted:
            continue
        min_qty = 0.001
        step = 0.001
        for f in sym.get("filters", []):
            if f.get("filterType") == "LOT_SIZE":
                min_qty = float(f.get("minQty", 0.001))
                step = float(f.get("stepSize", 0.001))
        out[s] = {"min_qty": min_qty, "qty_step": step}

    return out


def _round_step(value: float, step: float) -> float:
    if step <= 0:
        return value
    return math.floor(value / step) * step


def _qty_for_trade(entry: float, meta: Dict[str, float]) -> float:
    notional = CONFIG.TRADE.USDT_PER_TRADE * CONFIG.TRADE.LEVERAGE
    raw = notional / entry if entry > 0 else 0.0
    qty = _round_step(raw, meta.get("qty_step", 0.001))
    return max(qty, meta.get("min_qty", 0.001))


def _build_setup(symbol: str, history: List[Candle], cfg: BacktestConfig) -> Optional[Dict[str, Any]]:
    if len(history) < max(CONFIG.TRADE.EMA_SLOW, 55):
        return None

    closes = [x.close for x in history]
    highs = [x.high for x in history]
    lows = [x.low for x in history]

    e20 = _ema(closes, CONFIG.TRADE.EMA_FAST)[-1]
    e50 = _ema(closes, CONFIG.TRADE.EMA_MID)[-1]
    e200 = _ema(closes, CONFIG.TRADE.EMA_SLOW)[-1]
    atr = _atr(history, CONFIG.TRADE.ATR_PERIOD)
    if atr <= 0:
        return None

    last = history[-1].close
    prev = history[-2].close
    recent_high = max(highs[-CONFIG.TRADE.BREAKOUT_LOOKBACK:])
    recent_low = min(lows[-CONFIG.TRADE.BREAKOUT_LOOKBACK:])

    side = ""
    setup_type = ""
    score = 0
    entry = last

    up_trend = last > e20 > e50 > e200
    down_trend = last < e20 < e50 < e200

    if up_trend and last >= recent_high * 0.998:
        side = "LONG"
        setup_type = "BREAKOUT"
        score = 7
    elif down_trend and last <= recent_low * 1.002:
        side = "SHORT"
        setup_type = "BREAKOUT"
        score = 7
    elif up_trend and last > e50 and prev >= e20 * 0.995:
        side = "LONG"
        setup_type = "PULLBACK"
        score = 6
        entry = e20
    elif down_trend and last < e50 and prev <= e20 * 1.005:
        side = "SHORT"
        setup_type = "PULLBACK"
        score = 6
        entry = e20
    else:
        return None

    risk = atr * cfg.sl_multiplier
    if side == "LONG":
        sl = entry - risk
        tp = entry + (risk * cfg.rr_target)
    else:
        sl = entry + risk
        tp = entry - (risk * cfg.rr_target)

    rr = _compute_rr(entry, sl, tp)
    if rr < CONFIG.TRADE.RR_MIN:
        return None

    fake_market = {
        "price": last,
        "spread_pct": 0.0,
        "funding_rate_pct": 0.0,
    }

    setup = {
        "symbol": symbol,
        "side": side,
        "entry_zone_low": entry - (atr * 0.15),
        "entry_zone_high": entry + (atr * 0.15),
        "entry_trigger": entry,
        "sl": sl,
        "tp": tp,
        "rr": round(rr, 2),
        "score": score,
        "tf_context": "BT_1H",
        "setup_type": setup_type,
        "setup_reason": f"BT_{setup_type}_{side}",
        "expected_net_pnl_pct": round((risk / entry) * cfg.rr_target * 100.0 - _fees_pct(), 4),
        "stop_net_loss_pct": round(-((risk / entry) * 100.0) - _fees_pct(), 4),
        "spread_pct": fake_market["spread_pct"],
        "funding_rate_pct": fake_market["funding_rate_pct"],
    }
    return candidate_to_order(setup, last)


def _fill_order(order: Dict[str, Any], candle: Candle) -> bool:
    low = min(order["entry_zone_low"], order["entry_zone_high"])
    high = max(order["entry_zone_low"], order["entry_zone_high"])
    return candle.low <= high and candle.high >= low


def _close_price_for_hit(side: str, reason: str, sl: float, tp: float) -> float:
    if reason == "SL_HIT":
        return sl
    if reason == "TP_HIT":
        return tp
    return tp if side == "LONG" else sl


def _evaluate_close_reason(pos: PositionState, candle: Candle) -> Optional[str]:
    sl = pos.sl
    tp = pos.tp
    if pos.side == "LONG":
        sl_hit = candle.low <= sl
        tp_hit = candle.high >= tp
    else:
        sl_hit = candle.high >= sl
        tp_hit = candle.low <= tp

    if sl_hit and tp_hit:
        # conservative assumption for robustness
        return "SL_HIT"
    if sl_hit:
        return "SL_HIT"
    if tp_hit:
        return "TP_HIT"
    return None


def _update_trailing(pos: PositionState, candle: Candle, cfg: BacktestConfig) -> None:
    if not cfg.trailing.enabled:
        return

    if pos.highest_price == 0:
        pos.highest_price = pos.entry
    if pos.lowest_price == 0:
        pos.lowest_price = pos.entry

    pos.highest_price = max(pos.highest_price, candle.high)
    pos.lowest_price = min(pos.lowest_price, candle.low)

    risk = abs(pos.entry - pos.sl)
    if risk <= 0:
        return

    if pos.side == "LONG":
        favorable_move = pos.highest_price - pos.entry
    else:
        favorable_move = pos.entry - pos.lowest_price

    progress_r = favorable_move / risk if risk > 0 else 0.0

    if (not pos.break_even_armed) and progress_r >= cfg.trailing.break_even_trigger_r:
        pos.sl = pos.entry
        pos.break_even_armed = True

    if progress_r >= cfg.trailing.trail_after_r:
        if pos.side == "LONG":
            candidate_sl = candle.close - (risk * cfg.trailing.trail_factor)
            if candidate_sl > pos.sl:
                pos.sl = candidate_sl
        else:
            candidate_sl = candle.close + (risk * cfg.trailing.trail_factor)
            if candidate_sl < pos.sl:
                pos.sl = candidate_sl


def _simulate_symbol(
        symbol: str,
        candles: List[Candle],
        cfg: BacktestConfig,
        meta: Dict[str, float],
) -> List[Dict[str, Any]]:
    trades: List[Dict[str, Any]] = []
    open_order: Optional[Dict[str, Any]] = None
    position: Optional[PositionState] = None

    warmup = max(CONFIG.TRADE.EMA_SLOW, CONFIG.TRADE.BREAKOUT_LOOKBACK, 60)

    for idx in range(warmup, len(candles)):
        c = candles[idx]
        history = candles[:idx]

        if position:
            _update_trailing(position, c, cfg)
            reason = _evaluate_close_reason(position, c)
            if reason:
                close_price = _close_price_for_hit(position.side, reason, position.sl, position.tp)
                gross = _pct_change(position.entry, close_price, position.side)
                net = gross - _fees_pct()
                notional = position.entry * position.qty
                trades.append(
                    {
                        "symbol": symbol,
                        "side": position.side,
                        "entry": position.entry,
                        "sl": position.sl,
                        "tp": position.tp,
                        "rr": position.rr,
                        "score": position.score,
                        "setup_type": position.setup_type,
                        "opened_at": position.opened_at,
                        "closed_at": c.open_time,
                        "close_reason": reason,
                        "close_price": close_price,
                        "net_pnl_pct": net,
                        "net_pnl_usdt": (net / 100.0) * notional,
                        "duration_bars": idx - next(
                            (i for i, x in enumerate(candles) if x.open_time == position.opened_at), idx),
                        "highest_price": position.highest_price,
                        "lowest_price": position.lowest_price,
                    }
                )
                position = None

        if position is None and open_order is not None:
            if _fill_order(open_order, c):
                entry = float(open_order["entry_trigger"])
                qty = _qty_for_trade(entry, meta)
                position = PositionState(
                    symbol=symbol,
                    side=str(open_order["side"]),
                    entry=entry,
                    sl=float(open_order["sl"]),
                    tp=float(open_order["tp"]),
                    qty=qty,
                    opened_at=c.open_time,
                    rr=float(open_order["rr"]),
                    score=int(float(open_order["score"])),
                    setup_type=str(open_order["setup_type"]),
                    highest_price=entry,
                    lowest_price=entry,
                )
                open_order = None

        if open_order is None and position is None:
            setup_order = _build_setup(symbol, history, cfg)
            if setup_order is not None:
                open_order = setup_order

    return trades


def _equity_curve(trades: List[Dict[str, Any]], starting_equity: float = 100.0) -> Tuple[
    List[float], List[float], float]:
    eq = [starting_equity]
    dd = [0.0]
    peak = starting_equity
    max_dd = 0.0

    for t in sorted(trades, key=lambda x: x["closed_at"]):
        next_eq = eq[-1] * (1.0 + (float(t["net_pnl_pct"]) / 100.0))
        eq.append(next_eq)
        peak = max(peak, next_eq)
        d = ((peak - next_eq) / peak) * 100.0 if peak > 0 else 0.0
        dd.append(d)
        max_dd = max(max_dd, d)

    return eq, dd, max_dd


def _streaks(values: List[float]) -> Tuple[int, int]:
    max_win = 0
    max_loss = 0
    cur_win = 0
    cur_loss = 0
    for v in values:
        if v > 0:
            cur_win += 1
            cur_loss = 0
        else:
            cur_loss += 1
            cur_win = 0
        max_win = max(max_win, cur_win)
        max_loss = max(max_loss, cur_loss)
    return max_win, max_loss


def _diagnostics(trades: List[Dict[str, Any]]) -> Dict[str, Any]:
    per_symbol: DefaultDict[str, List[float]] = defaultdict(list)
    per_month: DefaultDict[str, float] = defaultdict(float)
    durations: List[float] = []

    for t in trades:
        pnl = float(t["net_pnl_pct"])
        per_symbol[t["symbol"]].append(pnl)
        dt = datetime.fromtimestamp(int(t["closed_at"]) / 1000.0, tz=timezone.utc)
        per_month[dt.strftime("%Y-%m")] += pnl
        durations.append(float(t.get("duration_bars", 0)))

    symbol_stats: Dict[str, Dict[str, float]] = {}
    for sym, vals in sorted(per_symbol.items()):
        wins = [v for v in vals if v > 0]
        losses = [v for v in vals if v <= 0]
        gross_profit = sum(wins)
        gross_loss = abs(sum(losses))
        pf = gross_profit / gross_loss if gross_loss > 0 else 999.0
        mw, ml = _streaks(vals)
        symbol_stats[sym] = {
            "trades": float(len(vals)),
            "win_rate_pct": (len(wins) / len(vals) * 100.0) if vals else 0.0,
            "net_pnl_pct": sum(vals),
            "avg_win_pct": (sum(wins) / len(wins)) if wins else 0.0,
            "avg_loss_pct": (sum(losses) / len(losses)) if losses else 0.0,
            "profit_factor": pf,
            "max_win_streak": float(mw),
            "max_loss_streak": float(ml),
        }

    expectancy = 0.0
    all_vals = [float(t["net_pnl_pct"]) for t in trades]
    if all_vals:
        expectancy = sum(all_vals) / len(all_vals)

    return {
        "per_symbol": symbol_stats,
        "monthly_net_pnl_pct": dict(sorted(per_month.items())),
        "expectancy_pct": expectancy,
        "avg_trade_duration_bars": (sum(durations) / len(durations)) if durations else 0.0,
    }


def evaluate_configuration(
        symbols: Sequence[str],
        candles_by_symbol: Dict[str, List[Candle]],
        cfg: BacktestConfig,
        drawdown_penalty: float,
        meta_by_symbol: Dict[str, Dict[str, float]],
) -> Dict[str, Any]:
    all_trades: List[Dict[str, Any]] = []
    for symbol in symbols:
        candles = candles_by_symbol.get(symbol, [])
        if len(candles) < 120:
            continue
        all_trades.extend(
            _simulate_symbol(symbol, candles, cfg, meta_by_symbol.get(symbol, {"min_qty": 0.001, "qty_step": 0.001})))

    net = sum(float(t["net_pnl_pct"]) for t in all_trades)
    eq, dd_curve, max_dd = _equity_curve(all_trades)

    diagnostics = _diagnostics(all_trades)
    win_count = sum(1 for t in all_trades if float(t["net_pnl_pct"]) > 0)
    win_rate = (win_count / len(all_trades) * 100.0) if all_trades else 0.0

    return {
        "config": {
            "rr_target": cfg.rr_target,
            "sl_multiplier": cfg.sl_multiplier,
            "trailing": {
                "name": cfg.trailing.name,
                "enabled": cfg.trailing.enabled,
                "break_even_trigger_r": cfg.trailing.break_even_trigger_r,
                "trail_after_r": cfg.trailing.trail_after_r,
                "trail_factor": cfg.trailing.trail_factor,
            },
        },
        "trades": len(all_trades),
        "win_rate_pct": round(win_rate, 4),
        "net_pnl_pct": round(net, 4),
        "max_drawdown_pct": round(max_dd, 4),
        "fitness": round(net - (drawdown_penalty * max_dd), 4),
        "equity_curve": [round(x, 5) for x in eq],
        "drawdown_curve": [round(x, 5) for x in dd_curve],
        "diagnostics": diagnostics,
    }


def optimize_grid_search(
        symbols: Sequence[str],
        candles_by_symbol: Dict[str, List[Candle]],
        rr_values: Sequence[float],
        sl_multipliers: Sequence[float],
        trailing_settings: Sequence[TrailingSetting],
        drawdown_penalty: float,
        meta_by_symbol: Dict[str, Dict[str, float]],
) -> Dict[str, Any]:
    results: List[Dict[str, Any]] = []

    for rr in rr_values:
        for slm in sl_multipliers:
            for trailing in trailing_settings:
                cfg = BacktestConfig(rr_target=rr, sl_multiplier=slm, trailing=trailing)
                res = evaluate_configuration(symbols, candles_by_symbol, cfg, drawdown_penalty, meta_by_symbol)
                results.append(res)

    if not results:
        raise RuntimeError("No optimization results generated.")

    results.sort(
        key=lambda r: (
            r["fitness"],
            r["net_pnl_pct"],
            r["win_rate_pct"],
            -r["max_drawdown_pct"],
        ),
        reverse=True,
    )

    return {
        "best": results[0],
        "top_results": results[: min(len(results), 12)],
        "evaluated_configs": len(results),
    }


def write_best_env(path: str, best: Dict[str, Any]) -> None:
    trailing = best["config"]["trailing"]
    diagnostics = best.get("diagnostics", {})
    per_symbol = diagnostics.get("per_symbol", {})

    lines = [
        # Core optimized params
        f"RR_MIN={best['config']['rr_target']:.4f}",
        f"BACKTEST_OPT_SL_MULTIPLIER={best['config']['sl_multiplier']:.4f}",

        # Trailing config
        f"ENABLE_TRAILING={'1' if trailing['enabled'] else '0'}",
        f"BREAK_EVEN_TRIGGER_R={trailing['break_even_trigger_r']:.4f}",
        f"TRAIL_AFTER_R={trailing['trail_after_r']:.4f}",
        f"BACKTEST_TRAIL_FACTOR={trailing['trail_factor']:.4f}",

        # Performance summary
        f"BACKTEST_OPT_NET_PNL_PCT={best['net_pnl_pct']:.4f}",
        f"BACKTEST_OPT_MAX_DRAWDOWN_PCT={best['max_drawdown_pct']:.4f}",
        f"BACKTEST_OPT_FITNESS={best['fitness']:.4f}",

        # New 🔥
        f"BACKTEST_OPT_TRADES={best.get('trades', 0)}",
        f"BACKTEST_OPT_WIN_RATE_PCT={best.get('win_rate_pct', 0):.2f}",
        f"BACKTEST_OPT_EXPECTANCY_PCT={diagnostics.get('expectancy_pct', 0):.4f}",
        f"BACKTEST_OPT_PROFIT_FACTOR={diagnostics.get('profit_factor', 0):.4f}",
    ]

    # Per-symbol breakdown
    for sym, stats in per_symbol.items():
        sym_clean = sym.replace("USDT", "")
        lines.extend([
            f"BACKTEST_{sym_clean}_TRADES={int(stats.get('trades', 0))}",
            f"BACKTEST_{sym_clean}_WIN_RATE_PCT={stats.get('win_rate_pct', 0):.2f}",
            f"BACKTEST_{sym_clean}_NET_PNL_PCT={stats.get('net_pnl_pct', 0):.4f}",
        ])

    with open(path, "w", encoding="utf-8") as f:
        f.write("\n".join(lines) + "\n")


def _parse_float_list(raw: str) -> List[float]:
    return [float(x.strip()) for x in raw.split(",") if x.strip()]


def _parse_symbols(client: BinanceFuturesClient, raw: str, max_symbols: int) -> List[str]:
    if raw.strip():
        return [x.strip().upper() for x in raw.split(",") if x.strip()]

    tickers = client.get_24h_ticker()
    filtered = [
        t for t in tickers if str(t.get("symbol", "")).endswith("USDT") and float(
            t.get("quoteVolume", 0.0)) >= CONFIG.TRADE.MIN_VOLUME_USDT_24H
    ]
    filtered.sort(key=lambda r: float(r.get("quoteVolume", 0.0)), reverse=True)
    return [str(x["symbol"]) for x in filtered[:max_symbols]]


def _date_to_ms(date_str: str) -> int:
    dt = datetime.strptime(date_str, "%Y-%m-%d").replace(tzinfo=timezone.utc)
    return int(dt.timestamp() * 1000)


def _default_trailing_grid() -> List[TrailingSetting]:
    return [
        TrailingSetting("off", False, 99.0, 99.0, 1.0),
        TrailingSetting("tight", True, 0.8, 1.1, 0.6),
        TrailingSetting("balanced", True, 1.0, 1.5, 0.8),
        TrailingSetting("loose", True, 1.2, 1.8, 1.0),
    ]


def main() -> None:
    parser = argparse.ArgumentParser(description="Historical replay backtester + optimizer")
    parser.add_argument("--symbols", default="",
                        help="Comma separated symbols. If empty, uses top liquid USDT contracts.")
    parser.add_argument("--max-symbols", type=int, default=8)
    parser.add_argument("--interval", default="1h")
    parser.add_argument("--start-date", required=True, help="UTC date: YYYY-MM-DD")
    parser.add_argument("--end-date", required=True, help="UTC date: YYYY-MM-DD")
    parser.add_argument("--rr-values", default="2.0,2.4,2.8,3.2")
    parser.add_argument("--sl-multipliers", default="0.8,1.0,1.2")
    parser.add_argument("--drawdown-penalty", type=float, default=0.7)
    parser.add_argument("--best-env-out", default="data/best_backtest.env")
    args = parser.parse_args()

    drawdown_penalty = clamp(args.drawdown_penalty, 0.0, 5.0)
    rr_values = _parse_float_list(args.rr_values)
    sl_multipliers = _parse_float_list(args.sl_multipliers)

    client = BinanceFuturesClient()
    symbols = _parse_symbols(client, args.symbols, args.max_symbols)
    start_ms = _date_to_ms(args.start_date)
    end_ms = _date_to_ms(args.end_date)

    candles_by_symbol: Dict[str, List[Candle]] = {}
    for s in symbols:
        candles_by_symbol[s] = _fetch_historical_klines(client, s, args.interval, start_ms, end_ms)

    trailing_grid = _default_trailing_grid()
    meta = _symbol_meta(client, symbols)

    result = optimize_grid_search(
        symbols=symbols,
        candles_by_symbol=candles_by_symbol,
        rr_values=rr_values,
        sl_multipliers=sl_multipliers,
        trailing_settings=trailing_grid,
        drawdown_penalty=drawdown_penalty,
        meta_by_symbol=meta,
    )

    best = result["best"]
    write_best_env(args.best_env_out, best)

    print("=== ADVANCED BACKTEST + OPTIMIZER COMPLETE ===")
    print(f"Period: {args.start_date} -> {args.end_date}")
    print(f"Symbols: {', '.join(symbols)}")
    print(f"Evaluated configs: {result['evaluated_configs']}")
    print(f"Best config: {best['config']}")
    print("=== PERFORMANCE BREAKDOWN ===")
    print(f"Trades: {best['trades']}")
    print(f"Win Rate: {best['win_rate_pct']}%")
    print(f"Net PnL %: {best['net_pnl_pct']}")
    print(f"Max Drawdown %: {best['max_drawdown_pct']}")
    print(f"Fitness: {best['fitness']}")
    print("\n=== PER SYMBOL PNL ===")

    per_symbol = best["diagnostics"]["per_symbol"]

    for sym, stats in per_symbol.items():
        print(
            f"{sym} | Trades: {int(stats['trades'])} | "
            f"WinRate: {stats['win_rate_pct']:.2f}% | "
            f"NetPnL: {stats['net_pnl_pct']:.2f}%"
        )


if __name__ == "__main__":
    main()
