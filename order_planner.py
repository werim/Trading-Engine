from typing import Dict, Any, List


def build_order_plan(symbol: str, market_ctx: Dict[str, Any], regime: str, liq: Dict[str, Any], scenarios: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    tf = market_ctx["tf"]

    r1 = tf["1H"]["regime"]
    r4 = tf["4H"]["regime"]
    rD = tf["1D"]["regime"]

    orders = []

    for s in scenarios:
        side = s["side"]
        name = s["name"]
        prob = s["probability"]
        zones = s.get("entry_zone", [])

        if not zones:
            continue

        for level in zones:
            entry = level

            # 🔴 SHORT (ana trade)
            if side == "SHORT":
                orders.append({
                    "side": "SHORT",
                    "entry": entry,
                    "sl": entry * 1.01,
                    "tp": entry * 0.97,
                    "scenario_name": name,
                    "scenario_probability": prob,
                    "size_mult": 1.0
                })

            # 🟢 LONG (sıkı filtre)
            if side == "LONG":

                # ❌ büyük trend short ise tamamen engelle
                if r4 == "SHORT" or rD == "SHORT":
                    continue

                # ❌ 1H bile short ise alma
                if r1 == "SHORT":
                    continue

                # ✅ sadece güçlü alignment varsa
                orders.append({
                    "side": "LONG",
                    "entry": entry,
                    "sl": entry * 0.99,
                    "tp": entry * 1.015,  # küçük TP (scalp)
                    "scenario_name": name,
                    "scenario_probability": prob,
                    "size_mult": 0.4  # küçültülmüş risk
                })

    return orders