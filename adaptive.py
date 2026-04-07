#adaptive.py
from utils import read_csv
from config import CLOSED_POSITIONS_CSV


def safe_float(x, default=0.0):
    try:
        return float(x)
    except:
        return default


def get_recent_closed_positions(limit=10):
    rows = read_csv(CLOSED_POSITIONS_CSV)
    return rows[-limit:]


def calculate_mode():
    rows = get_recent_closed_positions(10)
    if not rows:
        return "NORMAL"

    pnls = [safe_float(r.get("pnl_pct", 0)) for r in rows]

    wins = [p for p in pnls if p > 0]
    total = len(pnls)
    win_rate = len(wins) / total if total else 0

    last5 = pnls[-5:] if len(pnls) >= 5 else pnls
    total_pnl_5 = sum(last5)

    # streak
    loss_streak = 0
    for p in reversed(pnls):
        if p <= 0:
            loss_streak += 1
        else:
            break

    # DEFENSIVE
    if total_pnl_5 < -4 or loss_streak >= 3 or win_rate < 0.35:
        return "DEFENSIVE"

    return "NORMAL"