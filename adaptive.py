from __future__ import annotations

from typing import Any, Dict, Iterable, List, Tuple
import csv
import os

from utils import safe_float, normalize_status, utc_now_str


MIN_SAMPLE_SIZE = 8
MAX_SCORE_ADJUSTMENT = 3
RECENT_LOOKBACK = 30


def _get_files_config() -> Any:
    try:
        from config import CONFIG
        return CONFIG.FILES
    except Exception:
        return None


def _resolve_path(attr_name: str, default_name: str) -> str:
    files = _get_files_config()
    if files is not None:
        value = getattr(files, attr_name, "")
        if value:
            return value
    return os.path.join("data", default_name)


def closed_positions_path() -> str:
    return _resolve_path("CLOSED_POSITIONS_CSV", "closed_positions.csv")


def closed_orders_path() -> str:
    return _resolve_path("CLOSED_ORDERS_CSV", "closed_orders.csv")


def trade_reviews_path() -> str:
    return _resolve_path("TRADE_REVIEWS_CSV", "trade_reviews.csv")


def _read_csv(path: str) -> List[Dict[str, Any]]:
    if not path or not os.path.exists(path):
        return []
    try:
        with open(path, "r", newline="") as f:
            return list(csv.DictReader(f))
    except Exception:
        return []


def _append_rows(path: str, fieldnames: List[str], rows: Iterable[Dict[str, Any]]) -> None:
    os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
    file_exists = os.path.exists(path)
    with open(path, "a", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
        if not file_exists:
            writer.writeheader()
        for row in rows:
            writer.writerow({k: row.get(k, "") for k in fieldnames})


def _tf_triplet(tf_context: str) -> Tuple[str, str, str]:
    values = {"1H": "", "4H": "", "1D": ""}
    for part in str(tf_context or "").split("|"):
        if "=" not in part:
            continue
        k, v = part.split("=", 1)
        values[k.strip()] = v.strip()
    return values["1H"], values["4H"], values["1D"]


def _candidate_key(candidate: Dict[str, Any]) -> Tuple[str, str, str, str, str, str]:
    tf1, tf4, tfD = _tf_triplet(candidate.get("tf_context", ""))
    return (
        str(candidate.get("setup_type", "")),
        str(candidate.get("setup_reason", "")),
        str(candidate.get("side", "")),
        tf1,
        tf4,
        tfD,
    )


def _review_fieldnames() -> List[str]:
    return [
        "review_id",
        "source",
        "closed_at",
        "symbol",
        "side",
        "setup_type",
        "setup_reason",
        "tf_context",
        "score",
        "rr",
        "entry",
        "sl",
        "tp",
        "close_reason",
        "status_bucket",
        "net_pnl_pct",
        "net_pnl_usdt",
        "fees_usdt",
        "hold_minutes",
        "order_id",
        "position_id",
        "volume_24h_usdt",
        "spread_pct",
        "funding_rate_pct",
    ]


def _minutes_between(opened_at: str, closed_at: str) -> float:
    from datetime import datetime

    fmt = "%Y-%m-%d %H:%M:%S UTC"
    try:
        start = datetime.strptime(str(opened_at), fmt)
        end = datetime.strptime(str(closed_at), fmt)
        return max(0.0, (end - start).total_seconds() / 60.0)
    except Exception:
        return 0.0


def record_closed_orders(rows: Iterable[Dict[str, Any]]) -> int:
    rows = list(rows)
    if not rows:
        return 0

    existing = _read_csv(trade_reviews_path())
    existing_ids = {
        (r.get("source"), r.get("order_id"), r.get("position_id"), r.get("close_reason"))
        for r in existing
    }

    out: List[Dict[str, Any]] = []
    for row in rows:
        status = normalize_status(row.get("status"))
        if status not in {"CANCELLED", "EXPIRED", "REJECTED", "FAILED"}:
            continue
        key = ("ORDER", str(row.get("order_id", "")), "", status)
        if key in existing_ids:
            continue
        out.append(
            {
                "review_id": f"ORDER-{row.get('order_id', '')}-{status}",
                "source": "ORDER",
                "closed_at": row.get("updated_at") or utc_now_str(),
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
                "close_reason": status,
                "status_bucket": "ORDER_CANCELLED",
                "net_pnl_pct": 0,
                "net_pnl_usdt": 0,
                "fees_usdt": 0,
                "hold_minutes": _minutes_between(row.get("created_at", ""), row.get("updated_at", "") or utc_now_str()),
                "order_id": row.get("order_id", ""),
                "position_id": "",
                "volume_24h_usdt": row.get("volume_24h_usdt", 0),
                "spread_pct": row.get("spread_pct", 0),
                "funding_rate_pct": row.get("funding_rate_pct", 0),
            }
        )

    if out:
        _append_rows(trade_reviews_path(), _review_fieldnames(), out)
    return len(out)


def record_closed_position(position: Dict[str, Any], close_reason: str, close_price: float) -> int:
    existing = _read_csv(trade_reviews_path())
    existing_ids = {
        (r.get("source"), r.get("order_id"), r.get("position_id"), r.get("close_reason"))
        for r in existing
    }
    key = ("POSITION", str(position.get("order_id", "")), str(position.get("position_id", "")), str(close_reason or ""))
    if key in existing_ids:
        return 0

    closed_at = utc_now_str()
    review = {
        "review_id": f"POSITION-{position.get('position_id', '')}-{close_reason}",
        "source": "POSITION",
        "closed_at": closed_at,
        "symbol": position.get("symbol", ""),
        "side": position.get("side", ""),
        "setup_type": position.get("setup_type", ""),
        "setup_reason": position.get("setup_reason", ""),
        "tf_context": position.get("tf_context", ""),
        "score": position.get("score", 0),
        "rr": position.get("rr", 0),
        "entry": position.get("entry", 0),
        "sl": position.get("sl", 0),
        "tp": position.get("tp", 0),
        "close_reason": close_reason,
        "status_bucket": "POSITION_CLOSED",
        "net_pnl_pct": position.get("net_pnl_pct", 0),
        "net_pnl_usdt": position.get("net_pnl_usdt", 0),
        "fees_usdt": position.get("fees_usdt", 0),
        "hold_minutes": _minutes_between(position.get("opened_at", ""), closed_at),
        "order_id": position.get("order_id", ""),
        "position_id": position.get("position_id", ""),
        "volume_24h_usdt": position.get("volume_24h_usdt", 0),
        "spread_pct": position.get("spread_pct", 0),
        "funding_rate_pct": position.get("funding_rate_pct", 0),
    }
    _append_rows(trade_reviews_path(), _review_fieldnames(), [review])
    return 1


def load_trade_reviews() -> List[Dict[str, Any]]:
    reviews = _read_csv(trade_reviews_path())
    if reviews:
        return reviews

    # fallback: synthesize from historical files if trade_reviews.csv does not exist yet
    synthetic: List[Dict[str, Any]] = []
    synthetic.extend(_read_csv(closed_orders_path()))
    synthetic.extend(_read_csv(closed_positions_path()))
    return synthetic


def _result_points(row: Dict[str, Any]) -> float:
    reason = str(row.get("close_reason") or row.get("status") or "").upper()
    net_pct = safe_float(row.get("net_pnl_pct"))

    if reason == "TP_HIT":
        return max(1.0, net_pct / 100.0 + 1.0)
    if reason == "SL_HIT":
        return min(-1.0, net_pct / 100.0 - 1.0)
    if reason in {"CANCELLED", "EXPIRED", "REJECTED", "FAILED"}:
        return -0.20
    if net_pct > 0:
        return max(0.2, net_pct / 100.0)
    if net_pct < 0:
        return min(-0.2, net_pct / 100.0)
    return 0.0


def _build_stats(reviews: List[Dict[str, Any]]) -> Dict[Tuple[str, str, str, str, str, str], Dict[str, Any]]:
    stats: Dict[Tuple[str, str, str, str, str, str], Dict[str, Any]] = {}
    for row in reviews:
        key = _candidate_key(row)
        bucket = stats.setdefault(
            key,
            {
                "trades": 0,
                "tp_hits": 0,
                "sl_hits": 0,
                "cancelled": 0,
                "net_pnl_sum": 0.0,
                "points_sum": 0.0,
                "avg_rr": 0.0,
                "rr_sum": 0.0,
            },
        )
        bucket["trades"] += 1
        reason = str(row.get("close_reason") or row.get("status") or "").upper()
        if reason == "TP_HIT":
            bucket["tp_hits"] += 1
        elif reason == "SL_HIT":
            bucket["sl_hits"] += 1
        elif reason in {"CANCELLED", "EXPIRED", "REJECTED", "FAILED"}:
            bucket["cancelled"] += 1
        bucket["net_pnl_sum"] += safe_float(row.get("net_pnl_pct"))
        bucket["points_sum"] += _result_points(row)
        bucket["rr_sum"] += safe_float(row.get("rr"))

    for bucket in stats.values():
        trades = max(bucket["trades"], 1)
        bucket["avg_net_pnl_pct"] = bucket["net_pnl_sum"] / trades
        bucket["expectancy"] = bucket["points_sum"] / trades
        bucket["tp_rate"] = bucket["tp_hits"] / trades
        bucket["sl_rate"] = bucket["sl_hits"] / trades
        bucket["cancel_rate"] = bucket["cancelled"] / trades
        bucket["avg_rr"] = bucket["rr_sum"] / trades
    return stats


def compute_adaptive_adjustment(candidate: Dict[str, Any], reviews: List[Dict[str, Any]] | None = None) -> Dict[str, Any]:
    if reviews is None:
        reviews = load_trade_reviews()
    stats = _build_stats(reviews)
    key = _candidate_key(candidate)
    bucket = stats.get(key)
    if not bucket:
        return {
            "adaptive_score_delta": 0,
            "adaptive_expectancy": 0.0,
            "adaptive_sample_size": 0,
            "adaptive_blocked": False,
            "adaptive_reason": "NO_DATA",
        }

    trades = int(bucket["trades"])
    expectancy = float(bucket["expectancy"])
    cancel_rate = float(bucket["cancel_rate"])
    sl_rate = float(bucket["sl_rate"])

    delta = 0
    blocked = False
    reason = "OK"

    if trades >= MIN_SAMPLE_SIZE:
        if expectancy <= -0.75 or sl_rate >= 0.70:
            delta -= 3
            blocked = True
            reason = "NEGATIVE_EXPECTANCY_BLOCK"
        elif expectancy <= -0.35:
            delta -= 2
            reason = "NEGATIVE_EXPECTANCY"
        elif expectancy < 0:
            delta -= 1
            reason = "WEAK_EXPECTANCY"
        elif expectancy >= 0.60:
            delta += 2
            reason = "STRONG_EXPECTANCY"
        elif expectancy >= 0.20:
            delta += 1
            reason = "POSITIVE_EXPECTANCY"

        if cancel_rate >= 0.55 and not blocked:
            delta -= 1
            reason = "HIGH_CANCEL_RATE"

    delta = max(-MAX_SCORE_ADJUSTMENT, min(MAX_SCORE_ADJUSTMENT, delta))
    return {
        "adaptive_score_delta": delta,
        "adaptive_expectancy": expectancy,
        "adaptive_sample_size": trades,
        "adaptive_blocked": blocked,
        "adaptive_reason": reason,
    }


def apply_adaptive_scoring(candidate: Dict[str, Any], reviews: List[Dict[str, Any]] | None = None) -> Dict[str, Any]:
    adj = compute_adaptive_adjustment(candidate, reviews=reviews)
    candidate["adaptive_score_delta"] = adj["adaptive_score_delta"]
    candidate["adaptive_expectancy"] = adj["adaptive_expectancy"]
    candidate["adaptive_sample_size"] = adj["adaptive_sample_size"]
    candidate["adaptive_reason"] = adj["adaptive_reason"]
    candidate["adaptive_blocked"] = 1 if adj["adaptive_blocked"] else 0
    candidate["score"] = int(safe_float(candidate.get("score")) + adj["adaptive_score_delta"])
    return candidate
