from typing import Dict, List, Any


def build_scenarios(symbol: str, market_ctx: Dict[str, Any], regime: str, liq: Dict[str, Any]) -> List[Dict[str, Any]]:
    tf = market_ctx["tf"]

    r1 = tf["1H"]["regime"]
    r4 = tf["4H"]["regime"]
    rD = tf["1D"]["regime"]

    scenarios: List[Dict[str, Any]] = []

    # 🔴 ANA: trend short continuation
    if r4 == "SHORT" or rD == "SHORT":
        scenarios.append({
            "name": "TREND_CONTINUATION_SHORT",
            "probability": 0.55,
            "side": "SHORT",
            "entry_zone": liq.get("resting_liquidity_above", []),
        })

    # 🔴 upper reject short
    scenarios.append({
        "name": "UPPER_REJECT_SHORT",
        "probability": 0.30,
        "side": "SHORT",
        "entry_zone": liq.get("trap_short_zone", []),
    })

    # ⚠️ bounce long artık zayıf
    if r4 == "LONG" and rD != "SHORT":
        scenarios.append({
            "name": "RANGE_LOWER_BOUNCE",
            "probability": 0.15,
            "side": "LONG",
            "entry_zone": liq.get("trap_long_zone", []),
        })

    return sorted(scenarios, key=lambda x: x["probability"], reverse=True)