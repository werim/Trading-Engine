from typing import Dict, List, Any


def build_scenarios(
    symbol: str,
    market_ctx: Dict[str, Any],
    regime: str,
    liq: Dict[str, Any],
) -> List[Dict[str, Any]]:
    tf = market_ctx["tf"]

    r1 = str(tf["1H"]["regime"]).upper()
    r4 = str(tf["4H"]["regime"]).upper()
    rD = str(tf["1D"]["regime"]).upper()

    scenarios: List[Dict[str, Any]] = []

    def add(
        name: str,
        probability: float,
        side: str,
        entry_zone: List[float],
        tags: List[str] | None = None,
    ) -> None:
        if not entry_zone:
            return
        scenarios.append({
            "name": name,
            "probability": probability,
            "side": side,
            "entry_zone": entry_zone,
            "tags": tags or [],
        })

    # A+ LONG SIDE
    if rD == "LONG" and r4 == "LONG":
        if r1 in {"LONG", "RANGE"}:
            add(
                "TREND_CONTINUATION_LONG",
                0.72 if r1 == "LONG" else 0.64,
                "LONG",
                liq.get("resting_liquidity_below", []) or liq.get("trap_long_zone", []),
                ["trend", "continuation", "a_plus"],
            )

        add(
            "BREAKOUT_LONG",
            0.61 if r1 == "LONG" else 0.52,
            "LONG",
            liq.get("breakout_above", []),
            ["trend", "breakout", "a_plus"],
        )

    elif rD == "LONG" and r4 == "RANGE":
        if r1 == "LONG":
            add(
                "PULLBACK_LONG",
                0.58,
                "LONG",
                liq.get("trap_long_zone", []) or liq.get("resting_liquidity_below", []),
                ["trend", "pullback"],
            )
            add(
                "BREAKOUT_LONG",
                0.54,
                "LONG",
                liq.get("breakout_above", []),
                ["trend", "breakout"],
            )

    # A+ SHORT SIDE
    if rD == "SHORT" and r4 == "SHORT":
        if r1 in {"SHORT", "RANGE"}:
            add(
                "TREND_CONTINUATION_SHORT",
                0.72 if r1 == "SHORT" else 0.64,
                "SHORT",
                liq.get("resting_liquidity_above", []) or liq.get("trap_short_zone", []),
                ["trend", "continuation", "a_plus"],
            )

        add(
            "BREAKDOWN_SHORT",
            0.61 if r1 == "SHORT" else 0.52,
            "SHORT",
            liq.get("breakout_below", []),
            ["trend", "breakdown", "a_plus"],
        )

    elif rD == "SHORT" and r4 == "RANGE":
        if r1 == "SHORT":
            add(
                "PULLBACK_SHORT",
                0.58,
                "SHORT",
                liq.get("trap_short_zone", []) or liq.get("resting_liquidity_above", []),
                ["trend", "pullback"],
            )
            add(
                "BREAKDOWN_SHORT",
                0.54,
                "SHORT",
                liq.get("breakout_below", []),
                ["trend", "breakdown"],
            )

    # RANGE ONLY WHEN BIGGER TFs ARE ALSO RANGE
    if rD == "RANGE" and r4 == "RANGE":
        if r1 != "SHORT":
            add(
                "RANGE_LOWER_BOUNCE",
                0.42,
                "LONG",
                liq.get("trap_long_zone", []),
                ["range", "mean_reversion"],
            )

        if r1 != "LONG":
            add(
                "RANGE_UPPER_REJECT",
                0.42,
                "SHORT",
                liq.get("trap_short_zone", []),
                ["range", "mean_reversion"],
            )

    scenarios.sort(key=lambda x: x["probability"], reverse=True)
    return scenarios
