# decision_trace.py
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional
import json


@dataclass
class DecisionTrace:
    symbol: str
    side: str
    allow: bool = False
    confidence: float = 0.0
    score: int = 0
    summary: str = ""
    reasons: List[str] = field(default_factory=list)
    flags: List[str] = field(default_factory=list)
    context: Dict[str, Any] = field(default_factory=dict)
    reviewer: Dict[str, Any] = field(default_factory=dict)
    policy: Dict[str, Any] = field(default_factory=dict)

    def add_reason(self, text: str) -> None:
        if text and text not in self.reasons:
            self.reasons.append(text)

    def add_flag(self, text: str) -> None:
        if text and text not in self.flags:
            self.flags.append(text)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "symbol": self.symbol,
            "side": self.side,
            "allow": self.allow,
            "confidence": round(self.confidence, 4),
            "score": self.score,
            "summary": self.summary,
            "reasons": self.reasons,
            "flags": self.flags,
            "context": self.context,
            "reviewer": self.reviewer,
            "policy": self.policy,
        }

    def to_json(self) -> str:
        return json.dumps(self.to_dict(), ensure_ascii=False, sort_keys=False)

    def short_log(self) -> str:
        reasons = " | ".join(self.reasons) if self.reasons else "-"
        flags = " | ".join(self.flags) if self.flags else "-"
        return (
            f"DECISION_TRACE {self.symbol} {self.side} "
            f"allow={self.allow} confidence={self.confidence:.2f} score={self.score} "
            f"summary={self.summary} reasons={reasons} flags={flags}"
        )