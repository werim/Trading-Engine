import argparse
import csv
from dataclasses import dataclass
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple

from config import CONFIG
from utils import pct_change, safe_float


@dataclass(frozen=True)
class TrailingSetting:
    name: str
    enabled: bool
    trigger_r: float = 1.5
    lock_r: float = 0.7


@dataclass(frozen=True)
class BacktestConfig:
    rr_target: float
    sl_multiplier: float
    trailing: TrailingSetting


@dataclass
class TradeRecord:
    symbol: str
    side: str
    entry: float
    sl: float
    tp: float
    close_price: float
    close_reason: str


def _trade_cost_pct() -> float:
    return (
        CONFIG.TRADE.MAKER_FEE_PCT
        + CONFIG.TRADE.TAKER_FEE_PCT
        + CONFIG.TRADE.ROUND_TRIP_SLIPPAGE_PCT
    )


def _risk_pct(entry: float, sl: float, side: str) -> float:
    return abs(pct_change(entry, sl, side))


def _mfe_r(trade: TradeRecord, risk_pct: float, raw: Dict[str, Any]) -> float:
    if risk_pct <= 0:
        return 0.0

    side = trade.side.upper()
    if side == "LONG":
        highest = safe_float(raw.get("highest_price"), trade.entry)
        favorable_pct = max(0.0, pct_change(trade.entry, highest, side))
    else:
        lowest = safe_float(raw.get("lowest_price"), trade.entry)
        favorable_pct = max(0.0, pct_change(trade.entry, lowest, side))

    return favorable_pct / risk_pct if risk_pct > 0 else 0.0


def load_closed_positions(path: str) -> List[Dict[str, Any]]:
    with open(path, "r", encoding="utf-8", newline="") as f:
        return list(csv.DictReader(f))


def parse_trade(row: Dict[str, Any]) -> Optional[TradeRecord]:
    entry = safe_float(row.get("entry"))
    sl = safe_float(row.get("sl"))
    tp = safe_float(row.get("tp"))
    close_price = safe_float(row.get("close_price"))
    symbol = str(row.get("symbol", "")).strip()
    side = str(row.get("side", "")).upper().strip()
    close_reason = str(row.get("close_reason", "")).strip().upper()

    if not symbol or side not in {"LONG", "SHORT"}:
        return None
    if entry <= 0 or sl <= 0 or tp <= 0:
        return None
    if close_price <= 0:
        close_price = tp if close_reason == "TP_HIT" else sl

    return TradeRecord(
        symbol=symbol,
        side=side,
        entry=entry,
        sl=sl,
        tp=tp,
        close_price=close_price,
        close_reason=close_reason,
    )


def _simulate_trade_pnl_pct(trade: TradeRecord, raw: Dict[str, Any], cfg: BacktestConfig) -> float:
    cost_pct = _trade_cost_pct()

    base_risk = _risk_pct(trade.entry, trade.sl, trade.side)
    new_risk = base_risk * cfg.sl_multiplier
    if new_risk <= 0:
        return 0.0

    tp_net_pct = (new_risk * cfg.rr_target) - cost_pct
    sl_net_pct = (-new_risk) - cost_pct

    reason = trade.close_reason
    if reason == "TP_HIT":
        return tp_net_pct

    if reason == "SL_HIT":
        if not cfg.trailing.enabled:
            return sl_net_pct

        mfe_r = _mfe_r(trade, new_risk, raw)
        if mfe_r >= cfg.trailing.trigger_r:
            locked_r = max(0.0, mfe_r - cfg.trailing.lock_r)
            protected_pct = (locked_r * new_risk) - cost_pct
            return max(sl_net_pct, protected_pct)

        return sl_net_pct

    # fallback for unknown close reason: use directional close price
    directional_pct = pct_change(trade.entry, trade.close_price, trade.side)
    return directional_pct - cost_pct


def _equity_and_drawdown(returns_pct: Sequence[float], initial_equity: float = 100.0) -> Tuple[List[float], List[float], float]:
    equity_curve = [initial_equity]
    drawdowns = [0.0]
    peak = initial_equity
    max_dd = 0.0

    for ret in returns_pct:
        next_equity = equity_curve[-1] * (1.0 + (ret / 100.0))
        equity_curve.append(next_equity)

        peak = max(peak, next_equity)
        dd = 0.0 if peak <= 0 else ((peak - next_equity) / peak) * 100.0
        drawdowns.append(dd)
        max_dd = max(max_dd, dd)

    return equity_curve, drawdowns, max_dd


def _per_symbol_stats(symbol_returns: Dict[str, List[float]]) -> Dict[str, Dict[str, float]]:
    out: Dict[str, Dict[str, float]] = {}
    for symbol, values in sorted(symbol_returns.items()):
        wins = sum(1 for x in values if x > 0)
        losses = sum(1 for x in values if x <= 0)
        out[symbol] = {
            "trades": float(len(values)),
            "win_rate_pct": (wins / len(values) * 100.0) if values else 0.0,
            "net_pnl_pct": sum(values),
            "avg_pnl_pct": (sum(values) / len(values)) if values else 0.0,
            "loss_trades": float(losses),
        }
    return out


def evaluate_configuration(
    rows: Iterable[Dict[str, Any]],
    cfg: BacktestConfig,
    drawdown_penalty: float = 0.5,
) -> Dict[str, Any]:
    returns_pct: List[float] = []
    symbol_returns: Dict[str, List[float]] = {}

    for row in rows:
        trade = parse_trade(row)
        if trade is None:
            continue

        pnl_pct = _simulate_trade_pnl_pct(trade, row, cfg)
        returns_pct.append(pnl_pct)
        symbol_returns.setdefault(trade.symbol, []).append(pnl_pct)

    equity_curve, drawdowns, max_dd = _equity_and_drawdown(returns_pct)
    net_pnl_pct = sum(returns_pct)

    return {
        "config": {
            "rr_target": cfg.rr_target,
            "sl_multiplier": cfg.sl_multiplier,
            "trailing": {
                "name": cfg.trailing.name,
                "enabled": cfg.trailing.enabled,
                "trigger_r": cfg.trailing.trigger_r,
                "lock_r": cfg.trailing.lock_r,
            },
        },
        "trades": len(returns_pct),
        "net_pnl_pct": round(net_pnl_pct, 4),
        "max_drawdown_pct": round(max_dd, 4),
        "fitness": round(net_pnl_pct - (drawdown_penalty * max_dd), 4),
        "equity_curve": [round(x, 4) for x in equity_curve],
        "drawdown_curve": [round(x, 4) for x in drawdowns],
        "per_symbol": _per_symbol_stats(symbol_returns),
    }


def optimize_grid_search(
    rows: Iterable[Dict[str, Any]],
    rr_values: Sequence[float],
    sl_multipliers: Sequence[float],
    trailing_settings: Sequence[TrailingSetting],
    drawdown_penalty: float = 0.5,
) -> Dict[str, Any]:
    rows = list(rows)
    results: List[Dict[str, Any]] = []

    for rr in rr_values:
        for slm in sl_multipliers:
            for trailing in trailing_settings:
                cfg = BacktestConfig(rr_target=rr, sl_multiplier=slm, trailing=trailing)
                result = evaluate_configuration(rows, cfg, drawdown_penalty=drawdown_penalty)
                results.append(result)

    if not results:
        raise ValueError("No optimization results were generated; check input dataset and parameter grids.")

    results.sort(key=lambda r: (r["fitness"], r["net_pnl_pct"], -r["max_drawdown_pct"]), reverse=True)
    return {
        "best": results[0],
        "top_results": results[: min(10, len(results))],
        "evaluated_configs": len(results),
    }


def _parse_csv_floats(value: str) -> List[float]:
    return [float(part.strip()) for part in value.split(",") if part.strip()]


def _default_trailing_grid() -> List[TrailingSetting]:
    return [
        TrailingSetting(name="off", enabled=False),
        TrailingSetting(name="tight", enabled=True, trigger_r=1.0, lock_r=0.5),
        TrailingSetting(name="balanced", enabled=True, trigger_r=1.5, lock_r=0.7),
        TrailingSetting(name="loose", enabled=True, trigger_r=2.0, lock_r=1.0),
    ]


def main() -> None:
    parser = argparse.ArgumentParser(description="Advanced backtesting + optimization engine")
    parser.add_argument("--closed-positions", default="data/closed_positions.csv")
    parser.add_argument("--rr-values", default="2.0,2.4,2.8,3.2")
    parser.add_argument("--sl-multipliers", default="0.8,1.0,1.2")
    parser.add_argument("--drawdown-penalty", type=float, default=0.5)
    args = parser.parse_args()

    rows = load_closed_positions(args.closed_positions)
    rr_values = _parse_csv_floats(args.rr_values)
    sl_multipliers = _parse_csv_floats(args.sl_multipliers)
    trailing_settings = _default_trailing_grid()

    result = optimize_grid_search(
        rows=rows,
        rr_values=rr_values,
        sl_multipliers=sl_multipliers,
        trailing_settings=trailing_settings,
        drawdown_penalty=args.drawdown_penalty,
    )

    best = result["best"]
    print("=== BACKTEST OPTIMIZATION COMPLETE ===")
    print(f"Evaluated configs: {result['evaluated_configs']}")
    print(f"Best RR: {best['config']['rr_target']}")
    print(f"Best SL multiplier: {best['config']['sl_multiplier']}")
    print(f"Best trailing: {best['config']['trailing']}")
    print(f"Net PnL %: {best['net_pnl_pct']}")
    print(f"Max Drawdown %: {best['max_drawdown_pct']}")
    print(f"Fitness: {best['fitness']}")


if __name__ == "__main__":
    main()
