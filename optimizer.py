from __future__ import annotations

from typing import Any, Dict, Iterable, List, Tuple
import csv
import json
import math
import os

import adaptive
from utils import safe_float, normalize_status, utc_now_str


DEFAULT_MIN_SAMPLE = 12
DEFAULT_BLOCK_SAMPLE = 20
DEFAULT_RECENT_WEIGHT = 0.65
DEFAULT_LONG_WEIGHT = 0.35
DEFAULT_MAX_SCORE_DELTA = 3


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


def trade_reviews_path() -> str:
    return _resolve_path("TRADE_REVIEWS_CSV", "trade_reviews.csv")


def optimizer_snapshot_path() -> str:
    return _resolve_path("OPTIMIZER_SNAPSHOT_JSON", "optimizer_snapshot.json")


def optimizer_weights_path() -> str:
    return _resolve_path("OPTIMIZER_WEIGHTS_JSON", "optimizer_weights.json")


def _read_csv(path: str) -> List[Dict[str, Any]]:
    if not path or not os.path.exists(path):
        return []
    try:
        with open(path, "r", newline="") as f:
            return list(csv.DictReader(f))
    except Exception:
        return []


def _write_json(path: str, payload: Dict[str, Any]) -> None:
    os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
    with open(path, "w") as f:
        json.dump(payload, f, indent=2, sort_keys=True)


def _candidate_key(candidate: Dict[str, Any]) -> Tuple[str, str, str, str, str, str]:
    return adaptive._candidate_key(candidate)


def _key_to_str(key: Tuple[str, str, str, str, str, str]) -> str:
    return "||".join(key)


def _key_from_row(row: Dict[str, Any]) -> Tuple[str, str, str, str, str, str]:
    return adaptive._candidate_key(row)


def _result_points(row: Dict[str, Any]) -> float:
    return adaptive._result_points(row)


def load_reviews() -> List[Dict[str, Any]]:
    reviews = adaptive.load_trade_reviews()
    return [r for r in reviews if r]


def _tail(rows: List[Dict[str, Any]], n: int) -> List[Dict[str, Any]]:
    if n <= 0:
        return []
    if len(rows) <= n:
        return rows[:]
    return rows[-n:]


def _group_reviews(reviews: Iterable[Dict[str, Any]]) -> Dict[Tuple[str, str, str, str, str, str], List[Dict[str, Any]]]:
    grouped: Dict[Tuple[str, str, str, str, str, str], List[Dict[str, Any]]] = {}
    for row in reviews:
        key = _key_from_row(row)
        grouped.setdefault(key, []).append(row)
    return grouped


def _build_bucket(rows: List[Dict[str, Any]]) -> Dict[str, Any]:
    trades = len(rows)
    if trades == 0:
        return {
            "trades": 0,
            "tp_hits": 0,
            "sl_hits": 0,
            "cancelled": 0,
            "avg_net_pnl_pct": 0.0,
            "expectancy": 0.0,
            "tp_rate": 0.0,
            "sl_rate": 0.0,
            "cancel_rate": 0.0,
            "avg_rr": 0.0,
            "gross_points": 0.0,
        }

    tp_hits = 0
    sl_hits = 0
    cancelled = 0
    net_pnl_sum = 0.0
    rr_sum = 0.0
    points_sum = 0.0

    for row in rows:
        reason = str(row.get("close_reason") or row.get("status") or "").upper()
        if reason == "TP_HIT":
            tp_hits += 1
        elif reason == "SL_HIT":
            sl_hits += 1
        elif reason in {"CANCELLED", "EXPIRED", "REJECTED", "FAILED"}:
            cancelled += 1

        net_pnl_sum += safe_float(row.get("net_pnl_pct"))
        rr_sum += safe_float(row.get("rr"))
        points_sum += _result_points(row)

    return {
        "trades": trades,
        "tp_hits": tp_hits,
        "sl_hits": sl_hits,
        "cancelled": cancelled,
        "avg_net_pnl_pct": net_pnl_sum / trades,
        "expectancy": points_sum / trades,
        "tp_rate": tp_hits / trades,
        "sl_rate": sl_hits / trades,
        "cancel_rate": cancelled / trades,
        "avg_rr": rr_sum / trades,
        "gross_points": points_sum,
    }


def _blend_stats(long_bucket: Dict[str, Any], recent_bucket: Dict[str, Any], recent_weight: float = DEFAULT_RECENT_WEIGHT, long_weight: float = DEFAULT_LONG_WEIGHT) -> Dict[str, Any]:
    long_trades = int(long_bucket.get("trades", 0))
    recent_trades = int(recent_bucket.get("trades", 0))
    if long_trades == 0 and recent_trades == 0:
        return _build_bucket([])
    if long_trades == 0:
        return dict(recent_bucket)
    if recent_trades == 0:
        return dict(long_bucket)

    total_w = recent_weight + long_weight
    rw = recent_weight / total_w
    lw = long_weight / total_w

    blended = {
        "trades": long_trades,
        "recent_trades": recent_trades,
        "tp_hits": int(round(long_bucket.get("tp_hits", 0) * lw + recent_bucket.get("tp_hits", 0) * rw)),
        "sl_hits": int(round(long_bucket.get("sl_hits", 0) * lw + recent_bucket.get("sl_hits", 0) * rw)),
        "cancelled": int(round(long_bucket.get("cancelled", 0) * lw + recent_bucket.get("cancelled", 0) * rw)),
        "avg_net_pnl_pct": long_bucket.get("avg_net_pnl_pct", 0.0) * lw + recent_bucket.get("avg_net_pnl_pct", 0.0) * rw,
        "expectancy": long_bucket.get("expectancy", 0.0) * lw + recent_bucket.get("expectancy", 0.0) * rw,
        "tp_rate": long_bucket.get("tp_rate", 0.0) * lw + recent_bucket.get("tp_rate", 0.0) * rw,
        "sl_rate": long_bucket.get("sl_rate", 0.0) * lw + recent_bucket.get("sl_rate", 0.0) * rw,
        "cancel_rate": long_bucket.get("cancel_rate", 0.0) * lw + recent_bucket.get("cancel_rate", 0.0) * rw,
        "avg_rr": long_bucket.get("avg_rr", 0.0) * lw + recent_bucket.get("avg_rr", 0.0) * rw,
        "gross_points": long_bucket.get("gross_points", 0.0) * lw + recent_bucket.get("gross_points", 0.0) * rw,
    }
    return blended


def _score_delta_from_bucket(bucket: Dict[str, Any], min_sample: int = DEFAULT_MIN_SAMPLE, block_sample: int = DEFAULT_BLOCK_SAMPLE, max_delta: int = DEFAULT_MAX_SCORE_DELTA) -> Tuple[int, bool, str]:
    trades = int(bucket.get("trades", 0))
    expectancy = float(bucket.get("expectancy", 0.0))
    sl_rate = float(bucket.get("sl_rate", 0.0))
    cancel_rate = float(bucket.get("cancel_rate", 0.0))
    tp_rate = float(bucket.get("tp_rate", 0.0))

    if trades < min_sample:
        return 0, False, "INSUFFICIENT_SAMPLE"

    delta = 0
    blocked = False
    reason = "NEUTRAL"

    if trades >= block_sample and (expectancy <= -0.80 or sl_rate >= 0.72):
        delta = -max_delta
        blocked = True
        reason = "BLOCK_BAD_SETUP"
    elif expectancy <= -0.40:
        delta = -2
        reason = "NEGATIVE_EXPECTANCY"
    elif expectancy < 0:
        delta = -1
        reason = "WEAK_EXPECTANCY"
    elif expectancy >= 0.75 and tp_rate >= 0.45:
        delta = 2
        reason = "STRONG_EXPECTANCY"
    elif expectancy >= 0.20:
        delta = 1
        reason = "POSITIVE_EXPECTANCY"

    if cancel_rate >= 0.55 and not blocked:
        delta -= 1
        reason = "HIGH_CANCEL_RATE"

    delta = max(-max_delta, min(max_delta, delta))
    return delta, blocked, reason


def build_optimizer_snapshot(
    reviews: List[Dict[str, Any]] | None = None,
    recent_lookback: int = adaptive.RECENT_LOOKBACK,
    min_sample: int = DEFAULT_MIN_SAMPLE,
    block_sample: int = DEFAULT_BLOCK_SAMPLE,
) -> Dict[str, Any]:
    if reviews is None:
        reviews = load_reviews()

    grouped = _group_reviews(reviews)
    generated_at = utc_now_str()
    setup_stats: Dict[str, Any] = {}
    setup_weights: Dict[str, Any] = {}

    for key, rows in grouped.items():
        long_bucket = _build_bucket(rows)
        recent_bucket = _build_bucket(_tail(rows, recent_lookback))
        blended = _blend_stats(long_bucket, recent_bucket)
        delta, blocked, reason = _score_delta_from_bucket(
            blended,
            min_sample=min_sample,
            block_sample=block_sample,
            max_delta=DEFAULT_MAX_SCORE_DELTA,
        )

        key_str = _key_to_str(key)
        setup_stats[key_str] = {
            "key": {
                "setup_type": key[0],
                "setup_reason": key[1],
                "side": key[2],
                "tf_1h": key[3],
                "tf_4h": key[4],
                "tf_1d": key[5],
            },
            "long_term": long_bucket,
            "recent": recent_bucket,
            "blended": blended,
            "recommendation": {
                "score_delta": delta,
                "blocked": blocked,
                "reason": reason,
            },
        }
        setup_weights[key_str] = {
            "adaptive_score_delta": delta,
            "adaptive_blocked": blocked,
            "adaptive_reason": reason,
            "adaptive_expectancy": blended.get("expectancy", 0.0),
            "adaptive_sample_size": blended.get("trades", 0),
            "recent_sample_size": recent_bucket.get("trades", 0),
            "tp_rate": blended.get("tp_rate", 0.0),
            "sl_rate": blended.get("sl_rate", 0.0),
            "cancel_rate": blended.get("cancel_rate", 0.0),
            "avg_net_pnl_pct": blended.get("avg_net_pnl_pct", 0.0),
            "avg_rr": blended.get("avg_rr", 0.0),
        }

    snapshot = {
        "generated_at": generated_at,
        "review_count": len(reviews),
        "recent_lookback": recent_lookback,
        "min_sample": min_sample,
        "block_sample": block_sample,
        "setup_stats": setup_stats,
        "setup_weights": setup_weights,
    }
    return snapshot


def save_optimizer_outputs(snapshot: Dict[str, Any]) -> Dict[str, str]:
    snapshot_file = optimizer_snapshot_path()
    weights_file = optimizer_weights_path()

    _write_json(snapshot_file, snapshot)
    _write_json(
        weights_file,
        {
            "generated_at": snapshot.get("generated_at", utc_now_str()),
            "review_count": snapshot.get("review_count", 0),
            "setup_weights": snapshot.get("setup_weights", {}),
        },
    )
    return {
        "snapshot_file": snapshot_file,
        "weights_file": weights_file,
    }


def optimize_from_reviews(
    reviews: List[Dict[str, Any]] | None = None,
    recent_lookback: int = adaptive.RECENT_LOOKBACK,
    min_sample: int = DEFAULT_MIN_SAMPLE,
    block_sample: int = DEFAULT_BLOCK_SAMPLE,
    persist: bool = True,
) -> Dict[str, Any]:
    snapshot = build_optimizer_snapshot(
        reviews=reviews,
        recent_lookback=recent_lookback,
        min_sample=min_sample,
        block_sample=block_sample,
    )
    files = {}
    if persist:
        files = save_optimizer_outputs(snapshot)
    snapshot["files"] = files
    return snapshot


def load_optimizer_weights() -> Dict[str, Any]:
    path = optimizer_weights_path()
    if not os.path.exists(path):
        return {}
    try:
        with open(path, "r") as f:
            payload = json.load(f)
        return payload.get("setup_weights", {}) or {}
    except Exception:
        return {}


def compute_candidate_adjustment(candidate: Dict[str, Any], weights: Dict[str, Any] | None = None) -> Dict[str, Any]:
    if weights is None:
        weights = load_optimizer_weights()

    key_str = _key_to_str(_candidate_key(candidate))
    row = weights.get(key_str)
    if not row:
        return {
            "adaptive_score_delta": 0,
            "adaptive_expectancy": 0.0,
            "adaptive_sample_size": 0,
            "adaptive_blocked": False,
            "adaptive_reason": "NO_OPTIMIZER_DATA",
        }

    return {
        "adaptive_score_delta": int(row.get("adaptive_score_delta", 0)),
        "adaptive_expectancy": float(row.get("adaptive_expectancy", 0.0)),
        "adaptive_sample_size": int(row.get("adaptive_sample_size", 0)),
        "adaptive_blocked": bool(row.get("adaptive_blocked", False)),
        "adaptive_reason": str(row.get("adaptive_reason", "OPTIMIZED")),
    }


def apply_optimizer_to_candidate(candidate: Dict[str, Any], weights: Dict[str, Any] | None = None) -> Dict[str, Any]:
    adj = compute_candidate_adjustment(candidate, weights=weights)
    candidate["adaptive_score_delta"] = adj["adaptive_score_delta"]
    candidate["adaptive_expectancy"] = adj["adaptive_expectancy"]
    candidate["adaptive_sample_size"] = adj["adaptive_sample_size"]
    candidate["adaptive_blocked"] = 1 if adj["adaptive_blocked"] else 0
    candidate["adaptive_reason"] = adj["adaptive_reason"]
    candidate["score"] = int(safe_float(candidate.get("score", 0)) + adj["adaptive_score_delta"])
    return candidate


def summarize_snapshot(snapshot: Dict[str, Any], top_n: int = 10) -> Dict[str, List[Dict[str, Any]]]:
    items = []
    for key_str, row in (snapshot.get("setup_stats") or {}).items():
        rec = row.get("recommendation", {})
        blended = row.get("blended", {})
        items.append(
            {
                "key": key_str,
                "score_delta": int(rec.get("score_delta", 0)),
                "blocked": bool(rec.get("blocked", False)),
                "reason": rec.get("reason", ""),
                "expectancy": float(blended.get("expectancy", 0.0)),
                "trades": int(blended.get("trades", 0) or row.get("long_term", {}).get("trades", 0)),
                "tp_rate": float(blended.get("tp_rate", 0.0)),
                "sl_rate": float(blended.get("sl_rate", 0.0)),
                "cancel_rate": float(blended.get("cancel_rate", 0.0)),
            }
        )

    positives = sorted(
        [x for x in items if x["score_delta"] > 0 and not x["blocked"]],
        key=lambda x: (x["score_delta"], x["expectancy"], x["trades"]),
        reverse=True,
    )[:top_n]
    negatives = sorted(
        [x for x in items if x["score_delta"] < 0 or x["blocked"]],
        key=lambda x: (x["blocked"], abs(x["score_delta"]), abs(x["expectancy"]), x["trades"]),
        reverse=True,
    )[:top_n]
    return {
        "best_setups": positives,
        "worst_setups": negatives,
    }


if __name__ == "__main__":
    snapshot = optimize_from_reviews(persist=True)
    summary = summarize_snapshot(snapshot)
    print(json.dumps({
        "generated_at": snapshot.get("generated_at"),
        "review_count": snapshot.get("review_count"),
        "files": snapshot.get("files", {}),
        "summary": summary,
    }, indent=2))
