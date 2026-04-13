import os
import requests
from utils import log_message
from config import CONFIG


TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "").strip()


def send_telegram_message(text: str) -> bool:
    if not CONFIG.TRADE.TELEGRAM_ALERTS:
        return False

    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        log_message("[TELEGRAM] token/chat_id missing, message not sent")
        return False

    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    payload = {
        "chat_id": TELEGRAM_CHAT_ID,
        "text": text,
        "parse_mode": "HTML",
        "disable_web_page_preview": True,
    }

    try:
        r = requests.post(url, json=payload, timeout=10)
        if r.status_code == 200:
            return True
        log_message(f"[TELEGRAM] send failed status={r.status_code} body={r.text}")
        return False
    except Exception as e:
        log_message(f"[TELEGRAM] exception={e}")
        return False


def alert_break_even(pos: dict) -> None:
    text = (
        f"🟦 <b>BREAK EVEN ARMED</b>\n"
        f"<b>{pos['symbol']}</b> {pos['side']}\n"
        f"Entry: {pos['entry']}\n"
        f"Live: {pos['live_price']}\n"
        f"New SL: {pos['sl']}\n"
        f"Qty: {pos['qty']}"
    )
    send_telegram_message(text)


def alert_partial_tp(pos: dict, closed_qty: float, progress_r: float) -> None:
    text = (
        f"🟨 <b>PARTIAL TP HIT</b>\n"
        f"<b>{pos['symbol']}</b> {pos['side']}\n"
        f"Entry: {pos['entry']}\n"
        f"Live: {pos['live_price']}\n"
        f"Closed Qty: {round(closed_qty, 8)}\n"
        f"Remaining Qty: {pos['qty']}\n"
        f"Progress: {round(progress_r, 2)}R"
    )
    send_telegram_message(text)


def alert_trailing_update(pos: dict, old_sl: float, new_sl: float, progress_r: float) -> None:
    text = (
        f"🟪 <b>TRAILING SL UPDATED</b>\n"
        f"<b>{pos['symbol']}</b> {pos['side']}\n"
        f"Entry: {pos['entry']}\n"
        f"Live: {pos['live_price']}\n"
        f"Old SL: {round(old_sl, 8)}\n"
        f"New SL: {round(new_sl, 8)}\n"
        f"Progress: {round(progress_r, 2)}R"
    )
    send_telegram_message(text)


def alert_new_order(order: dict) -> None:
    text = (
        f"🟡 <b>NEW ORDER</b>\n"
        f"<b>{order['symbol']}</b> {order['side']}\n"
        f"Entry Zone: {order['entry_zone_low']} - {order['entry_zone_high']}\n"
        f"Trigger: {order['entry_trigger']}\n"
        f"SL: {order['sl']}\n"
        f"TP: {order['tp']}\n"
        f"RR: {order['rr']}\n"
        f"Score: {order['score']}\n"
        f"Expected Net: {order.get('expected_net_pnl_pct', 0)}%"
    )
    send_telegram_message(text)


def alert_position_opened(pos: dict) -> None:
    text = (
        f"🟢 <b>POSITION OPENED</b>\n"
        f"<b>{pos['symbol']}</b> {pos['side']}\n"
        f"Entry: {pos['entry']}\n"
        f"Qty: {pos['qty']}\n"
        f"SL: {pos['sl']}\n"
        f"TP: {pos['tp']}\n"
        f"RR: {pos['rr']}\n"
        f"Score: {pos['score']}"
    )
    send_telegram_message(text)


def alert_position_update(pos: dict) -> None:
    text = (
        f"📈 <b>POSITION UPDATE</b>\n"
        f"<b>{pos['symbol']}</b> {pos['side']}\n"
        f"Entry: {pos['entry']}\n"
        f"Live: {pos['live_price']}\n"
        f"Net PnL: {pos['net_pnl_pct']}%\n"
        f"Net USDT: {pos['net_pnl_usdt']}\n"
        f"SL: {pos['sl']}\n"
        f"TP: {pos['tp']}"
    )
    send_telegram_message(text)


def alert_position_closed(pos: dict, reason: str) -> None:
    text = (
        f"🔴 <b>POSITION CLOSED</b>\n"
        f"<b>{pos['symbol']}</b> {pos['side']}\n"
        f"Reason: {reason}\n"
        f"Entry: {pos['entry']}\n"
        f"Close: {pos['live_price']}\n"
        f"Net PnL: {pos['net_pnl_pct']}%\n"
        f"Net USDT: {pos['net_pnl_usdt']}"
    )
    send_telegram_message(text)