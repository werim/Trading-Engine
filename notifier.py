from __future__ import annotations

import requests

from config import CONFIG
from logger import get_logger

log = get_logger("notifier", "logs/notifier.log")


def telegram_enabled() -> bool:
    return bool(CONFIG.TELEGRAM.BOT_TOKEN and CONFIG.TELEGRAM.CHAT_ID)


def send_telegram_message(text: str) -> bool:
    if not telegram_enabled():
        return False

    url = f"https://api.telegram.org/bot{CONFIG.TELEGRAM.BOT_TOKEN}/sendMessage"
    payload = {
        "chat_id": CONFIG.TELEGRAM.CHAT_ID,
        "text": text,
    }

    try:
        resp = requests.post(url, json=payload, timeout=(5, 10))
        ok = resp.status_code == 200
        if not ok:
            log.warning("TELEGRAM_SEND_FAIL status=%s body=%s", resp.status_code, resp.text[:300])
        return ok
    except Exception as exc:
        log.warning("TELEGRAM_SEND_ERROR error=%s", exc)
        return False


def notify_order_created(order: dict) -> None:
    text = (
        f"🟡 ORDER WATCHING\n"
        f"{order['symbol']} {order['side']}\n"
        f"Entry Zone: {order['entry_zone_low']} - {order['entry_zone_high']}\n"
        f"Trigger: {order['entry_trigger']}\n"
        f"SL: {order['sl']}\n"
        f"TP: {order['tp']}\n"
        f"Score: {order['score']}\n"
        f"RR: {order['rr']}"
    )
    send_telegram_message(text)


def notify_real_order_submitted(order: dict) -> None:
    text = (
        f"🟠 REAL ORDER ENTRY\n"
        f"{order['symbol']} {order['side']}\n"
        f"Type: {order['order_type']}\n"
        f"Entry: {order['entry_trigger']}\n"
        f"Qty: {order['submitted_qty']}\n"
        f"Exchange Order ID: {order.get('exchange_order_id', '')}\n"
        f"Exchange Status: {order.get('exchange_status', '')}"
    )
    send_telegram_message(text)


def notify_position_opened(position: dict) -> None:
    text = (
        f"🟢 POSITION OPENED\n"
        f"{position['symbol']} {position['side']}\n"
        f"Entry: {position['entry']}\n"
        f"Qty: {position['qty']}\n"
        f"SL: {position['sl']}\n"
        f"TP: {position['tp']}\n"
        f"RR: {position['rr']}\n"
        f"Score: {position['score']}"
    )
    send_telegram_message(text)


def notify_position_closed(position: dict, close_reason: str, close_price: float) -> None:
    text = (
        f"🔴 POSITION CLOSED\n"
        f"{position['symbol']} {position['side']}\n"
        f"Close Reason: {close_reason}\n"
        f"Close Price: {close_price}\n"
        f"Net PnL %: {position.get('net_pnl_pct', 0)}\n"
        f"Net PnL USDT: {position.get('net_pnl_usdt', 0)}"
    )
    send_telegram_message(text)