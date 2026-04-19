# trade_reviewer.py
from __future__ import annotations

from typing import Any, Dict, List


def review_trade_context(ctx: Dict[str, Any]) -> Dict[str, Any]:
    reasons: List[str] = []
    flags: List[str] = []

    confidence = 0.50
    trend_score = 0.0
    structure_score = 0.0
    execution_score = 0.0
    portfolio_score = 0.0

    side = str(ctx.get("side", ""))
    alignment = str(ctx.get("trend_alignment", "NONE"))
    entry_quality = str(ctx.get("entry_quality", "UNKNOWN"))
    spread_quality = str(ctx.get("spread_quality", "BAD"))
    volume_quality = str(ctx.get("volume_quality", "D"))
    rr = float(ctx.get("rr", 0.0) or 0.0)
    score = int(ctx.get("score", 0) or 0)
    trigger_distance_pct = float(ctx.get("trigger_distance_pct", 999.0) or 999.0)
    recent_attempts = int(ctx.get("recent_attempts", 0) or 0)
    recent_cancellations = int(ctx.get("recent_cancellations", 0) or 0)
    same_symbol_open_orders = int(ctx.get("same_symbol_open_orders", 0) or 0)
    same_symbol_open_positions = int(ctx.get("same_symbol_open_positions", 0) or 0)
    cooldown_active = bool(ctx.get("cooldown_active", False))

    # Hard veto
    if cooldown_active:
        return {
            "allow": False,
            "confidence": 0.01,
            "summary": "cooldown aktif",
            "reasons": ["symbol cooldown aktif"],
            "flags": ["COOLDOWN_ACTIVE"],
            "component_scores": {},
        }

    if same_symbol_open_positions > 0:
        return {
            "allow": False,
            "confidence": 0.01,
            "summary": "aynı symbol için açık pozisyon var",
            "reasons": ["same symbol open position exists"],
            "flags": ["SAME_SYMBOL_POSITION"],
            "component_scores": {},
        }

    if same_symbol_open_orders > 0:
        return {
            "allow": False,
            "confidence": 0.05,
            "summary": "aynı symbol için açık emir zaten var",
            "reasons": ["same symbol open order exists"],
            "flags": ["SAME_SYMBOL_ORDER"],
            "component_scores": {},
        }

    # Trend alignment
    if alignment == "FULL":
        trend_score += 1.0
        confidence += 0.22
        reasons.append("1H, 4H ve 1D trend hizalı")
    elif alignment == "PARTIAL":
        trend_score += 0.60
        confidence += 0.10
        reasons.append("trend kısmen hizalı")
        flags.append("PARTIAL_ALIGNMENT")
    elif alignment == "WEAK":
        trend_score += 0.20
        confidence -= 0.10
        reasons.append("trend hizası zayıf")
        flags.append("WEAK_ALIGNMENT")
    else:
        trend_score -= 0.60
        confidence -= 0.25
        reasons.append("trend hizası yok")
        flags.append("NO_ALIGNMENT")

    # Candidate score
    if score >= 9:
        structure_score += 1.0
        confidence += 0.15
        reasons.append("setup score çok güçlü")
    elif score >= 7:
        structure_score += 0.65
        confidence += 0.08
        reasons.append("setup score güçlü")
    elif score >= 5:
        structure_score += 0.35
        confidence += 0.02
        reasons.append("setup score yeterli ama elit değil")
    else:
        structure_score -= 0.40
        confidence -= 0.12
        reasons.append("setup score zayıf")
        flags.append("LOW_SCORE")

    # RR
    if rr >= 3.0:
        structure_score += 0.65
        confidence += 0.08
        reasons.append("RR güçlü")
    elif rr >= 2.0:
        structure_score += 0.30
        confidence += 0.03
        reasons.append("RR kabul edilebilir")
    else:
        structure_score -= 0.35
        confidence -= 0.08
        reasons.append("RR zayıf")
        flags.append("LOW_RR")

    # Entry quality
    if entry_quality == "OPTIMAL":
        execution_score += 1.0
        confidence += 0.12
        reasons.append("entry konumu optimal")
    elif entry_quality == "EARLY":
        execution_score += 0.25
        confidence -= 0.02
        reasons.append("entry biraz erken olabilir")
        flags.append("EARLY_ENTRY")
    elif entry_quality == "LATE":
        execution_score -= 0.35
        confidence -= 0.12
        reasons.append("entry geç kalmış olabilir")
        flags.append("LATE_ENTRY")
    elif entry_quality == "VERY_LATE":
        execution_score -= 0.80
        confidence -= 0.22
        reasons.append("entry çok geç")
        flags.append("VERY_LATE_ENTRY")
    else:
        reasons.append("entry kalitesi net değil")
        flags.append("UNKNOWN_ENTRY_QUALITY")

    # Spread
    if spread_quality == "GOOD":
        execution_score += 0.50
        confidence += 0.06
        reasons.append("spread iyi")
    elif spread_quality == "OK":
        execution_score += 0.20
        confidence += 0.02
        reasons.append("spread kabul edilebilir")
    elif spread_quality == "WEAK":
        execution_score -= 0.25
        confidence -= 0.08
        reasons.append("spread zayıf")
        flags.append("WEAK_SPREAD")
    else:
        execution_score -= 0.80
        confidence -= 0.20
        reasons.append("spread kötü")
        flags.append("BAD_SPREAD")

    # Volume
    if volume_quality in ("A+", "A"):
        structure_score += 0.35
        confidence += 0.05
        reasons.append("likidite güçlü")
    elif volume_quality == "B":
        structure_score += 0.15
        confidence += 0.02
        reasons.append("likidite yeterli")
    else:
        structure_score -= 0.20
        confidence -= 0.05
        reasons.append("likidite zayıf")
        flags.append("LOW_LIQUIDITY")

    # Symbol repetition / overtrading smell
    if recent_attempts >= 3:
        portfolio_score -= 0.50
        confidence -= 0.12
        reasons.append("aynı symbol yakın dönemde fazla denendi")
        flags.append("OVERTRADED_SYMBOL")
    elif recent_attempts == 2:
        portfolio_score -= 0.20
        confidence -= 0.05
        reasons.append("aynı symbol yakın dönemde tekrarlandı")
        flags.append("REPEATED_SYMBOL")

    if recent_cancellations >= 2:
        portfolio_score -= 0.35
        confidence -= 0.08
        reasons.append("yakın dönemde iptal sayısı yüksek")
        flags.append("RECENT_CANCEL_CLUSTER")

    # Distance from trigger
    if trigger_distance_pct > 0.45:
        execution_score -= 0.45
        confidence -= 0.10
        reasons.append("fiyat trigger noktasından fazla uzak")
        flags.append("FAR_FROM_TRIGGER")

    final_component_score = (
        trend_score * 0.30 +
        structure_score * 0.35 +
        execution_score * 0.25 +
        portfolio_score * 0.10
    )

    allow = confidence >= 0.56 and final_component_score >= 0.20

    if "BAD_SPREAD" in flags or "VERY_LATE_ENTRY" in flags or "NO_ALIGNMENT" in flags:
        allow = False

    summary_parts: List[str] = []
    if allow:
        summary_parts.append("trade alınabilir")
    else:
        summary_parts.append("trade zayıf veya riskli")

    if alignment == "FULL":
        summary_parts.append("trend hizası güçlü")
    elif alignment in ("PARTIAL", "WEAK"):
        summary_parts.append("trend desteği sınırlı")
    else:
        summary_parts.append("trend desteği yok")

    if entry_quality in ("LATE", "VERY_LATE"):
        summary_parts.append("entry geç")
    elif entry_quality == "OPTIMAL":
        summary_parts.append("entry iyi")

    summary = ", ".join(summary_parts)

    return {
        "allow": allow,
        "confidence": max(0.0, min(1.0, round(confidence, 4))),
        "summary": summary,
        "reasons": reasons,
        "flags": flags,
        "component_scores": {
            "trend_score": round(trend_score, 4),
            "structure_score": round(structure_score, 4),
            "execution_score": round(execution_score, 4),
            "portfolio_score": round(portfolio_score, 4),
            "final_component_score": round(final_component_score, 4),
        },
    }