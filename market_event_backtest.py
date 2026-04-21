from __future__ import annotations

import argparse
import csv
import json
import math
import os
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Dict, Iterable, List, Optional, Tuple

import requests


HORIZONS = {
    "5m": 5,
    "15m": 15,
    "1h": 60,
    "4h": 240,
    "24h": 1440,
}


@dataclass
class MarketEvent:
    symbol: str
    event_time_ms: int
    source: str
    event_type: str
    direction: str
    price_at_event: float
    volume_zscore: float
    return_1m_pct: float
    range_pct: float
    realized_vol_15m: float
    mention_volume: float = 0.0
    sentiment_score: float = 0.0
    funding_rate: float = 0.0
    open_interest: float = 0.0


class BinanceDataClient:
    def __init__(self) -> None:
        testnet = os.getenv("BINANCE_TESTNET", "0") == "1"
        self.base = "https://testnet.binancefuture.com" if testnet else "https://fapi.binance.com"
        self.session = requests.Session()

    def get_klines(self, symbol: str, interval: str, start_ms: int, end_ms: int) -> List[Dict[str, Any]]:
        out: List[Dict[str, Any]] = []
        cursor = start_ms
        step_ms = 60_000

        while cursor < end_ms:
            resp = self.session.get(
                f"{self.base}/fapi/v1/klines",
                params={
                    "symbol": symbol,
                    "interval": interval,
                    "startTime": cursor,
                    "endTime": end_ms,
                    "limit": 1500,
                },
                timeout=(5, 30),
            )
            resp.raise_for_status()
            rows = resp.json()
            if not rows:
                break

            for row in rows:
                out.append(
                    {
                        "open_time": int(row[0]),
                        "open": float(row[1]),
                        "high": float(row[2]),
                        "low": float(row[3]),
                        "close": float(row[4]),
                        "volume": float(row[5]),
                    }
                )

            next_cursor = int(rows[-1][0]) + step_ms
            if next_cursor <= cursor:
                break
            cursor = next_cursor

        return out


def _parse_time_ms(raw: str) -> int:
    value = raw.strip()
    if not value:
        return 0
    if value.isdigit():
        return int(value)
    if value.endswith("Z"):
        value = value[:-1] + "+00:00"
    try:
        dt = datetime.fromisoformat(value)
    except ValueError:
        return 0
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return int(dt.timestamp() * 1000)


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


def _event_price_window(
    klines: List[Dict[str, Any]],
    event_ms: int,
    horizon_minutes: int,
) -> Tuple[float, List[Dict[str, Any]]]:
    if not klines:
        return 0.0, []

    event_price = 0.0
    window: List[Dict[str, Any]] = []
    end_ms = event_ms + horizon_minutes * 60_000

    for row in klines:
        ts = int(row["open_time"])
        if ts <= event_ms:
            event_price = float(row["close"])
        if event_ms <= ts <= end_ms:
            window.append(row)

    return event_price, window


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


def detect_market_events(
    symbol: str,
    klines: List[Dict[str, Any]],
    volume_z_threshold: float = 3.0,
    abs_return_threshold_pct: float = 0.8,
    range_threshold_pct: float = 1.2,
    vol15_threshold: float = 0.01,
    cooldown_minutes: int = 15,
) -> List[MarketEvent]:
    events: List[MarketEvent] = []
    if len(klines) < 60:
        return events

    last_event_ms = -10**18

    for i in range(30, len(klines)):
        row = klines[i]
        ts = int(row["open_time"])

        if ts - last_event_ms < cooldown_minutes * 60_000:
            continue

        prev_20 = klines[max(0, i - 20):i]
        prev_15 = klines[max(0, i - 15):i]
        if len(prev_20) < 10 or len(prev_15) < 10:
            continue

        volumes = [float(r["volume"]) for r in prev_20]
        vol_mean = _mean(volumes)
        vol_std = _std(volumes)
        volume = float(row["volume"])
        volume_z = 0.0 if vol_std <= 1e-12 else (volume - vol_mean) / vol_std

        open_p = float(row["open"])
        high_p = float(row["high"])
        low_p = float(row["low"])
        close_p = float(row["close"])

        ret_1m_pct = _safe_pct_change(open_p, close_p)
        range_pct = ((high_p - low_p) / max(open_p, 1e-12)) * 100.0

        closes_15 = [float(r["close"]) for r in prev_15] + [close_p]
        realized_vol_15m = _std(_log_returns(closes_15))

        flags: List[str] = []
        direction = "NEUTRAL"

        if volume_z >= volume_z_threshold:
            flags.append("volume_spike")

        if ret_1m_pct >= abs_return_threshold_pct:
            flags.append("price_spike_up")
            direction = "UP"
        elif ret_1m_pct <= -abs_return_threshold_pct:
            flags.append("price_spike_down")
            direction = "DOWN"

        if range_pct >= range_threshold_pct:
            flags.append("volatility_burst")

        if realized_vol_15m >= vol15_threshold:
            flags.append("volatility_burst")

        if ("volume_spike" in flags) and (
            "price_spike_up" in flags or "price_spike_down" in flags or "volatility_burst" in flags
        ):
            flags.append("combined_shock")

        flags = list(dict.fromkeys(flags))
        if not flags:
            continue

        event_type = "combined_shock" if "combined_shock" in flags else flags[0]
        BAD_EVENTS = {
            "volume_spike",
            "volatility_burst",
        }

        if event_type in BAD_EVENTS:
            continue

        events.append(
            MarketEvent(
                symbol=symbol,
                event_time_ms=ts,
                source="market",
                event_type=event_type,
                direction=direction,
                price_at_event=close_p,
                volume_zscore=volume_z,
                return_1m_pct=ret_1m_pct,
                range_pct=range_pct,
                realized_vol_15m=realized_vol_15m,
            )
        )
        last_event_ms = ts

    return events


def enrich_events(events: List[MarketEvent], market_by_symbol: Dict[str, List[Dict[str, Any]]]) -> List[Dict[str, Any]]:
    enriched: List[Dict[str, Any]] = []

    for ev in events:
        klines = market_by_symbol.get(ev.symbol, [])
        if not klines:
            continue

        base = {
            "symbol": ev.symbol,
            "event_time_ms": ev.event_time_ms,
            "event_time": datetime.fromtimestamp(ev.event_time_ms / 1000, tz=timezone.utc).isoformat(),
            "source": ev.source,
            "event_type": ev.event_type,
            "direction": ev.direction,
            "headline_text": ev.event_type,
            "sentiment_score": ev.sentiment_score,
            "mention_volume": ev.mention_volume,
            "funding_rate": ev.funding_rate,
            "open_interest": ev.open_interest,
            "price_at_event_signal": ev.price_at_event,
            "volume_zscore": ev.volume_zscore,
            "return_1m_pct": ev.return_1m_pct,
            "range_pct": ev.range_pct,
            "realized_vol_15m": ev.realized_vol_15m,
            "volume_bucket": _bucket_volume_zscore(ev.volume_zscore),
            "return_bucket": _bucket_return_1m(ev.return_1m_pct),
        }

        for horizon_name, horizon_minutes in HORIZONS.items():
            event_price, window = _event_price_window(klines, ev.event_time_ms, horizon_minutes)
            if event_price <= 0 or not window:
                continue

            highs = [float(r["high"]) for r in window]
            lows = [float(r["low"]) for r in window]
            closes = [float(r["close"]) for r in window]

            max_up_move_pct = ((max(highs) - event_price) / event_price) * 100.0
            max_down_move_pct = ((min(lows) - event_price) / event_price) * 100.0
            close_return_pct = ((closes[-1] - event_price) / event_price) * 100.0
            realized_volatility = _std(_log_returns(closes))

            if abs(max_up_move_pct) >= abs(max_down_move_pct):
                dominant_move = max_up_move_pct
            else:
                dominant_move = max_down_move_pct

            followed_through = int(
                (dominant_move > 0 and close_return_pct > 0) or
                (dominant_move < 0 and close_return_pct < 0)
            )
            fully_reversed = int(
                (dominant_move > 0 and close_return_pct <= 0) or
                (dominant_move < 0 and close_return_pct >= 0)
            )

            enriched.append(
                {
                    **base,
                    "horizon": horizon_name,
                    "price_at_event": event_price,
                    "max_up_move_pct": max_up_move_pct,
                    "max_down_move_pct": max_down_move_pct,
                    "close_return_pct": close_return_pct,
                    "realized_volatility": realized_volatility,
                    "followed_through": followed_through,
                    "fully_reversed": fully_reversed,
                    "jump_flag": int(abs(max_up_move_pct) >= 1.0 or abs(max_down_move_pct) >= 1.0),
                }
            )

    return enriched


def _group_stats(rows: Iterable[Dict[str, Any]], group_keys: List[str]) -> List[Dict[str, Any]]:
    buckets: Dict[Tuple[Any, ...], List[Dict[str, Any]]] = {}
    for row in rows:
        key = tuple(row.get(k) for k in group_keys)
        buckets.setdefault(key, []).append(row)

    stats: List[Dict[str, Any]] = []
    for key, group in buckets.items():
        n = len(group)
        if n == 0:
            continue

        avg_close = sum(float(g.get("close_return_pct", 0.0)) for g in group) / n
        avg_up = sum(float(g.get("max_up_move_pct", 0.0)) for g in group) / n
        avg_down = sum(float(g.get("max_down_move_pct", 0.0)) for g in group) / n
        jump_prob = sum(int(g.get("jump_flag", 0)) for g in group) / n
        continuation_prob = sum(int(g.get("followed_through", 0)) for g in group) / n
        reversal_prob = sum(int(g.get("fully_reversed", 0)) for g in group) / n

        stat = {k: v for k, v in zip(group_keys, key)}
        stat.update(
            {
                "sample_size": n,
                "avg_close_return_pct": avg_close,
                "avg_max_up_move_pct": avg_up,
                "avg_max_down_move_pct": avg_down,
                "jump_probability": jump_prob,
                "continuation_probability": continuation_prob,
                "reversal_probability": reversal_prob,
                "expectancy_close_return_pct": avg_close,
            }
        )
        stats.append(stat)

    stats.sort(key=lambda x: (x.get("symbol", ""), x.get("sample_size", 0)), reverse=False)
    return stats


def write_outputs(enriched: List[Dict[str, Any]], output_dir: str) -> None:
    os.makedirs(output_dir, exist_ok=True)

    with open(os.path.join(output_dir, "events_enriched.json"), "w", encoding="utf-8") as f:
        json.dump(enriched, f, ensure_ascii=False, indent=2)

    if enriched:
        fieldnames = list(enriched[0].keys())
        with open(os.path.join(output_dir, "events_enriched.csv"), "w", encoding="utf-8", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(enriched)

    stats_by_symbol_event = _group_stats(
        enriched,
        ["symbol", "event_type", "direction", "volume_bucket", "return_bucket", "horizon"],
    )
    stats_by_symbol_simple = _group_stats(
        enriched,
        ["symbol", "event_type", "horizon"],
    )

    summary = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "num_rows": len(enriched),
        "stats_by_symbol_event": stats_by_symbol_event,
        "stats_by_symbol_simple": stats_by_symbol_simple,
    }

    with open(os.path.join(output_dir, "market_event_stats.json"), "w", encoding="utf-8") as f:
        json.dump(summary, f, ensure_ascii=False, indent=2)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Market event backtester based on price/volume shocks")
    parser.add_argument("--symbols", default="BTCUSDT,ETHUSDT", help="Comma-separated symbols")
    parser.add_argument("--start", required=True, help="Start datetime ISO8601")
    parser.add_argument("--end", required=True, help="End datetime ISO8601")
    parser.add_argument("--output-dir", default="data/market_event_backtest", help="Output directory")
    parser.add_argument("--volume-z", type=float, default=3.0, help="Volume z-score threshold")
    parser.add_argument("--abs-return-pct", type=float, default=0.8, help="1m absolute return threshold percent")
    parser.add_argument("--range-pct", type=float, default=1.2, help="1m candle range threshold percent")
    parser.add_argument("--vol15", type=float, default=0.01, help="15m realized volatility threshold")
    parser.add_argument("--cooldown-minutes", type=int, default=15, help="Minimum minutes between events per symbol")
    return parser.parse_args()


def main() -> None:
    args = parse_args()

    symbols = [s.strip().upper() for s in args.symbols.split(",") if s.strip()]
    start_ms = _parse_time_ms(args.start)
    end_ms = _parse_time_ms(args.end)

    if start_ms <= 0 or end_ms <= 0 or end_ms <= start_ms:
        raise ValueError("Invalid --start/--end values")

    client = BinanceDataClient()
    market_by_symbol: Dict[str, List[Dict[str, Any]]] = {}
    all_events: List[MarketEvent] = []

    fetch_start_ms = start_ms - (60 * 60 * 1000)
    fetch_end_ms = end_ms + (24 * 60 * 60 * 1000)

    for symbol in symbols:
        print(f"[market_event_backtest] loading {symbol} 1m klines...")
        klines = client.get_klines(symbol, "1m", fetch_start_ms, fetch_end_ms)
        market_by_symbol[symbol] = klines

        in_window = [k for k in klines if start_ms <= int(k["open_time"]) <= end_ms]
        events = detect_market_events(
            symbol=symbol,
            klines=in_window,
            volume_z_threshold=args.volume_z,
            abs_return_threshold_pct=args.abs_return_pct,
            range_threshold_pct=args.range_pct,
            vol15_threshold=args.vol15,
            cooldown_minutes=args.cooldown_minutes,
        )
        print(f"[market_event_backtest] {symbol}: detected {len(events)} events")
        all_events.extend(events)

    if not all_events:
        print("No market events found in selected window.")
        return

    enriched = enrich_events(all_events, market_by_symbol)
    if not enriched:
        print("Events detected, but no enriched rows could be generated.")
        return

    write_outputs(enriched, args.output_dir)
    print(f"Wrote {len(enriched)} enriched event/horizon rows to {args.output_dir}")


if __name__ == "__main__":
    main()