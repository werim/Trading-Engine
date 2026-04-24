from __future__ import annotations

import argparse
import csv
import json
import math
import os
import calendar
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Dict, Iterable, List, Optional, Tuple

import feedparser
import requests


HORIZONS = {
    "5m": 5 * 60,
    "15m": 15 * 60,
    "1h": 60 * 60,
    "4h": 4 * 60 * 60,
    "24h": 24 * 60 * 60,
}

RSS_FEEDS = [
    "https://www.coindesk.com/arc/outboundfeeds/rss/?outputType=xml",
    "https://cointelegraph.com/rss",
    "https://cryptopotato.com/feed/",
    "https://cryptoslate.com/feed/",
    "https://cryptonews.com/news/feed/",
]


@dataclass
class Event:
    symbol: str
    event_time_ms: int
    source: str
    text: str
    sentiment_score: float
    mention_volume: float
    event_type: str
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
                params={"symbol": symbol, "interval": interval, "startTime": cursor, "endTime": end_ms, "limit": 1500},
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
                    }
                )
            cursor = int(rows[-1][0]) + step_ms
        return out


class EventCollector:
    def __init__(self) -> None:
        self.session = requests.Session()

    def from_csv(self, path: str) -> List[Event]:
        if not path or not os.path.exists(path):
            return []
        events: List[Event] = []
        with open(path, "r", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for row in reader:
                symbol = str(row.get("symbol", "")).upper()
                if not symbol:
                    continue
                event_time = _parse_time_ms(str(row.get("event_time", "")))
                if event_time <= 0:
                    continue
                events.append(
                    Event(
                        symbol=symbol,
                        event_time_ms=event_time,
                        source=str(row.get("source", "csv")),
                        text=str(row.get("headline", row.get("text", ""))),
                        sentiment_score=float(row.get("sentiment_score", 0.0) or 0.0),
                        mention_volume=float(row.get("mention_volume", 0.0) or 0.0),
                        event_type=str(row.get("event_type", "news")),
                        funding_rate=float(row.get("funding_rate", 0.0) or 0.0),
                        open_interest=float(row.get("open_interest", 0.0) or 0.0),
                    )
                )
        return events

    def from_cryptopanic(self, symbols: List[str], since_ms: int) -> List[Event]:
        key = os.getenv("CRYPTOPANIC_API_KEY", "")
        if not key:
            return []

        events: List[Event] = []
        for symbol in symbols:
            resp = self.session.get(
                "https://cryptopanic.com/api/v1/posts/",
                params={"auth_token": key, "currencies": symbol.replace("USDT", ""), "kind": "news"},
                timeout=(5, 20),
            )
            if resp.status_code >= 400:
                continue
            payload = resp.json()
            for row in payload.get("results", []):
                published = _parse_time_ms(str(row.get("published_at", "")))
                if published < since_ms:
                    continue
                title = str(row.get("title", ""))
                sentiment = _infer_sentiment_from_votes(row)
                events.append(
                    Event(
                        symbol=symbol,
                        event_time_ms=published,
                        source="cryptopanic",
                        text=title,
                        sentiment_score=sentiment,
                        mention_volume=float(row.get("votes", {}).get("important", 0.0) or 0.0),
                        event_type="news",
                    )
                )
        return events

    def from_rss(self, symbols: List[str], since_ms: int) -> List[Event]:
        symbol_set = set(symbols)
        events: List[Event] = []
        seen: set[tuple[str, int, str]] = set()

        for url in RSS_FEEDS:
            try:
                resp = self.session.get(url, timeout=(5, 20), headers={"User-Agent": "Mozilla/5.0"})
                resp.raise_for_status()
                feed = feedparser.parse(resp.content)
            except Exception:
                continue

            entries = getattr(feed, "entries", []) or []
            for entry in entries[:200]:
                title = str(getattr(entry, "title", "") or "")
                if not title:
                    continue

                published_parsed = (
                    getattr(entry, "published_parsed", None)
                    or getattr(entry, "updated_parsed", None)
                    or getattr(entry, "created_parsed", None)
                )
                timestamp_ms = _struct_time_to_utc_ms(published_parsed)
                if timestamp_ms <= 0:
                    timestamp_ms = _parse_time_ms(
                        str(getattr(entry, "published", "") or getattr(entry, "updated", "") or "")
                    )
                if timestamp_ms <= 0 or timestamp_ms < since_ms:
                    continue

                title_lc = title.lower()
                symbol = ""
                if "bitcoin" in title_lc or "btc" in title_lc:
                    symbol = "BTCUSDT"
                elif "ethereum" in title_lc or "eth" in title_lc:
                    symbol = "ETHUSDT"
                if not symbol or symbol not in symbol_set:
                    continue

                mention_volume = 1.0
                if "etf" in title_lc or "sec" in title_lc:
                    mention_volume += 3.0

                key = (symbol, timestamp_ms, title)
                if key in seen:
                    continue
                seen.add(key)

                events.append(
                    Event(
                        symbol=symbol,
                        event_time_ms=timestamp_ms,
                        source="rss",
                        text=title,
                        sentiment_score=0.0,
                        mention_volume=mention_volume,
                        event_type="news",
                    )
                )

        return events


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


def _struct_time_to_utc_ms(value: Any) -> int:
    if value is None:
        return 0
    try:
        return int(calendar.timegm(value) * 1000)
    except (TypeError, ValueError, OverflowError):
        return 0


def _infer_sentiment_from_votes(row: Dict[str, Any]) -> float:
    votes = row.get("votes", {})
    positive = float(votes.get("positive", 0.0) or 0.0)
    negative = float(votes.get("negative", 0.0) or 0.0)
    total = positive + negative
    if total <= 0:
        return 0.0
    return (positive - negative) / total


def _sentiment_bucket(score: float) -> str:
    if score >= 0.15:
        return "positive"
    if score <= -0.15:
        return "negative"
    return "neutral"


def _derivatives_bucket(funding_rate: float, open_interest_delta_pct: float) -> str:
    if funding_rate >= 0.03 and open_interest_delta_pct >= 1.5:
        return "euphoric"
    if funding_rate <= -0.03 and open_interest_delta_pct >= 1.5:
        return "panic"
    if open_interest_delta_pct >= 1.0:
        return "leveraged"
    return "balanced"


def _std(values: List[float]) -> float:
    if len(values) < 2:
        return 0.0
    mean = sum(values) / len(values)
    var = sum((x - mean) ** 2 for x in values) / (len(values) - 1)
    return math.sqrt(max(var, 0.0))


def _event_price_window(klines: List[Dict[str, Any]], event_ms: int, horizon_s: int) -> Tuple[float, List[Dict[str, Any]]]:
    if not klines:
        return 0.0, []
    event_price = 0.0
    window: List[Dict[str, Any]] = []
    end_ms = event_ms + horizon_s * 1000

    for row in klines:
        ts = int(row["open_time"])
        if ts <= event_ms:
            event_price = float(row["close"])
        if event_ms <= ts <= end_ms:
            window.append(row)
    return event_price, window


def enrich_events(events: List[Event], market_by_symbol: Dict[str, List[Dict[str, Any]]]) -> List[Dict[str, Any]]:
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
            "headline_text": ev.text,
            "sentiment_score": ev.sentiment_score,
            "mention_volume": ev.mention_volume,
            "event_type": ev.event_type,
            "funding_rate": ev.funding_rate,
            "open_interest": ev.open_interest,
        }

        open_interest_delta_pct = 0.0
        if ev.open_interest > 0:
            open_interest_delta_pct = (ev.open_interest / max(ev.open_interest - 1.0, 1e-9) - 1.0) * 100.0

        for horizon_name, horizon_s in HORIZONS.items():
            event_price, window = _event_price_window(klines, ev.event_time_ms, horizon_s)
            if event_price <= 0 or not window:
                continue

            highs = [float(r["high"]) for r in window]
            lows = [float(r["low"]) for r in window]
            closes = [float(r["close"]) for r in window]

            max_up_move_pct = ((max(highs) - event_price) / event_price) * 100.0
            max_down_move_pct = ((min(lows) - event_price) / event_price) * 100.0
            close_return_pct = ((closes[-1] - event_price) / event_price) * 100.0
            realized_volatility = _std([
                0.0 if i == 0 else math.log(max(closes[i], 1e-9) / max(closes[i - 1], 1e-9))
                for i in range(1, len(closes))
            ])

            dominant_move = max_up_move_pct if abs(max_up_move_pct) >= abs(max_down_move_pct) else max_down_move_pct
            followed_through = int((dominant_move > 0 and close_return_pct > 0) or (dominant_move < 0 and close_return_pct < 0))
            fully_reversed = int((dominant_move > 0 and close_return_pct <= 0) or (dominant_move < 0 and close_return_pct >= 0))

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
                    "sentiment_bucket": _sentiment_bucket(ev.sentiment_score),
                    "derivatives_bucket": _derivatives_bucket(ev.funding_rate, open_interest_delta_pct),
                    "open_interest_change_pct": open_interest_delta_pct,
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
        sample_size = len(group)
        if sample_size == 0:
            continue
        avg_close = sum(float(g.get("close_return_pct", 0.0)) for g in group) / sample_size
        avg_up = sum(float(g.get("max_up_move_pct", 0.0)) for g in group) / sample_size
        avg_down = sum(float(g.get("max_down_move_pct", 0.0)) for g in group) / sample_size
        jump_prob = sum(int(g.get("jump_flag", 0)) for g in group) / sample_size
        continuation_prob = sum(int(g.get("followed_through", 0)) for g in group) / sample_size
        reversal_prob = sum(int(g.get("fully_reversed", 0)) for g in group) / sample_size

        stat = {k: v for k, v in zip(group_keys, key)}
        stat.update(
            {
                "sample_size": sample_size,
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

    return stats


def write_outputs(enriched: List[Dict[str, Any]], output_dir: str) -> None:
    os.makedirs(output_dir, exist_ok=True)

    events_json = os.path.join(output_dir, "events_enriched.json")
    with open(events_json, "w", encoding="utf-8") as f:
        json.dump(enriched, f, ensure_ascii=False, indent=2)

    csv_path = os.path.join(output_dir, "events_enriched.csv")
    if enriched:
        fieldnames = list(enriched[0].keys())
        with open(csv_path, "w", encoding="utf-8", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(enriched)

    symbol_event_stats = _group_stats(enriched, ["symbol", "event_type", "sentiment_bucket", "derivatives_bucket", "horizon"])
    symbol_sentiment_stats = _group_stats(enriched, ["symbol", "sentiment_bucket", "horizon"])

    summary = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "num_events": len(enriched),
        "symbol_event_stats": symbol_event_stats,
        "symbol_sentiment_stats": symbol_sentiment_stats,
    }

    with open(os.path.join(output_dir, "news_stats.json"), "w", encoding="utf-8") as f:
        json.dump(summary, f, ensure_ascii=False, indent=2)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="News/Sentiment post-event backtester for crypto symbols")
    parser.add_argument("--symbols", default="BTCUSDT,ETHUSDT", help="Comma-separated symbols")
    parser.add_argument("--start", required=True, help="Start datetime ISO8601")
    parser.add_argument("--end", required=True, help="End datetime ISO8601")
    parser.add_argument("--events-csv", default="data/news_backtest/events_raw.csv", help="Optional local event CSV")
    parser.add_argument("--output-dir", default="data/news_backtest", help="Output directory")
    parser.add_argument("--with-cryptopanic", action="store_true", help="Enable CryptoPanic API pull when key is present")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    symbols = [s.strip().upper() for s in args.symbols.split(",") if s.strip()]
    start_ms = _parse_time_ms(args.start)
    end_ms = _parse_time_ms(args.end)

    if start_ms <= 0 or end_ms <= 0 or end_ms <= start_ms:
        raise ValueError("Invalid --start/--end values")

    collector = EventCollector()
    events = collector.from_csv(args.events_csv)
    if args.with_cryptopanic:
        events.extend(collector.from_cryptopanic(symbols, start_ms))
    rss_events = collector.from_rss(symbols, start_ms)
    events.extend(rss_events)
    print(f"RSS events: {len(rss_events)}")

    events = [e for e in events if start_ms <= e.event_time_ms <= end_ms and e.symbol in symbols]
    if not events:
        print("No events found in selected window.")
        return

    market_client = BinanceDataClient()
    market_by_symbol: Dict[str, List[Dict[str, Any]]] = {}
    for symbol in symbols:
        market_by_symbol[symbol] = market_client.get_klines(symbol, "1m", start_ms - 3600_000, end_ms + 86_400_000)

    enriched = enrich_events(events, market_by_symbol)
    write_outputs(enriched, args.output_dir)
    print(f"Wrote {len(enriched)} enriched event/horizon rows to {args.output_dir}")


if __name__ == "__main__":
    main()
