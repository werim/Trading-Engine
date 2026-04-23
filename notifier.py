from __future__ import annotations

import csv
import subprocess
import threading
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

import requests

from config import CONFIG
from logger import get_logger

log = get_logger("notifier", "logs/notifier.log")

BASE_DIR = Path(__file__).resolve().parent
ENV_PATH = BASE_DIR / ".env"

_ALLOWED_CHAT_ID = str(CONFIG.TELEGRAM.CHAT_ID or "").strip()
_POLL_THREAD: Optional[threading.Thread] = None
_STOP_FLAG = False
_LAST_UPDATE_ID = 0
_PENDING_REAL_CONFIRMATIONS: set[str] = set()


def telegram_enabled() -> bool:
    return bool(CONFIG.TELEGRAM.BOT_TOKEN and CONFIG.TELEGRAM.CHAT_ID)


def telegram_api_url(method: str) -> str:
    return f"https://api.telegram.org/bot{CONFIG.TELEGRAM.BOT_TOKEN}/{method}"


def _reply_keyboard() -> dict:
    return {
        "keyboard": [
            [{"text": "Status"}, {"text": "Run"}, {"text": "Stop"}],
            [{"text": "Restart"}, {"text": "Mode"}, {"text": "Help"}],
            [{"text": "Mode Paper"}, {"text": "Mode Real"}],
            [{"text": "Order Log"}, {"text": "Position Log"}],
            [{"text": "Open Orders"}, {"text": "Open Positions"}],
            [{"text": "Closed Orders"}, {"text": "Closed Positions"}],
            [{"text": "PnL Summary"}, {"text": "Today Trades"}],
            [{"text": "Last Closed"}, {"text": "Active Risk"}],
        ],
        "resize_keyboard": True,
        "one_time_keyboard": False,
    }


def send_telegram_message(text: str) -> bool:
    if not telegram_enabled():
        return False

    payload = {
        "chat_id": CONFIG.TELEGRAM.CHAT_ID,
        "text": text,
    }

    try:
        resp = requests.post(telegram_api_url("sendMessage"), json=payload, timeout=(5, 10))
        ok = resp.status_code == 200
        if not ok:
            log.warning("TELEGRAM_SEND_FAIL status=%s body=%s", resp.status_code, resp.text[:300])
        return ok
    except Exception as exc:
        log.warning("TELEGRAM_SEND_ERROR error=%s", exc)
        return False


def _send_reply(chat_id: str, text: str, with_menu: bool = True) -> bool:
    if not telegram_enabled():
        return False

    payload = {
        "chat_id": chat_id,
        "text": text[:4000],
    }
    if with_menu:
        payload["reply_markup"] = _reply_keyboard()

    try:
        resp = requests.post(telegram_api_url("sendMessage"), json=payload, timeout=(5, 10))
        ok = resp.status_code == 200
        if not ok:
            log.warning("TELEGRAM_REPLY_FAIL status=%s body=%s", resp.status_code, resp.text[:300])
        return ok
    except Exception as exc:
        log.warning("TELEGRAM_REPLY_ERROR error=%s", exc)
        return False


def _is_authorized_chat(chat_id: str) -> bool:
    return bool(_ALLOWED_CHAT_ID) and str(chat_id).strip() == _ALLOWED_CHAT_ID


def _run_script(script_name: str, timeout_sec: int = 30) -> tuple[bool, str]:
    script_path = BASE_DIR / script_name
    if not script_path.exists():
        return False, f"{script_name} not found"

    try:
        result = subprocess.run(
            ["bash", str(script_path)],
            cwd=str(BASE_DIR),
            capture_output=True,
            text=True,
            timeout=timeout_sec,
            check=False,
        )
        output = (result.stdout or "") + ("\n" + result.stderr if result.stderr else "")
        output = output.strip()
        if not output:
            output = f"{script_name} finished with code {result.returncode}"

        if result.returncode == 0:
            return True, output[:3500]
        return False, output[:3500]
    except subprocess.TimeoutExpired:
        return False, f"{script_name} timed out after {timeout_sec}s"
    except Exception as exc:
        return False, f"{script_name} failed: {exc}"


def _read_current_mode() -> str:
    try:
        mode = str(CONFIG.ENGINE.EXECUTION_MODE or "").strip().upper()
        if mode:
            return mode
    except Exception:
        pass

    if not ENV_PATH.exists():
        return "UNKNOWN"

    try:
        for raw in ENV_PATH.read_text(encoding="utf-8").splitlines():
            line = raw.strip()
            if not line or line.startswith("#"):
                continue
            if line.startswith("ENGINE_EXECUTION_MODE="):
                return line.split("=", 1)[1].strip().upper()
            if line.startswith("EXECUTION_MODE="):
                return line.split("=", 1)[1].strip().upper()
    except Exception as exc:
        log.warning("READ_MODE_ERROR error=%s", exc)

    return "UNKNOWN"


def _write_mode_to_env(new_mode: str) -> tuple[bool, str]:
    new_mode = new_mode.strip().upper()
    if new_mode not in {"REAL", "PAPER"}:
        return False, "Mode must be PAPER or REAL"

    if not ENV_PATH.exists():
        return False, ".env not found"

    try:
        lines = ENV_PATH.read_text(encoding="utf-8").splitlines()
        updated = False
        new_lines = []

        for raw in lines:
            stripped = raw.strip()
            if stripped.startswith("ENGINE_EXECUTION_MODE="):
                new_lines.append(f"ENGINE_EXECUTION_MODE={new_mode}")
                updated = True
            elif stripped.startswith("EXECUTION_MODE="):
                new_lines.append(f"EXECUTION_MODE={new_mode}")
                updated = True
            else:
                new_lines.append(raw)

        if not updated:
            new_lines.append(f"ENGINE_EXECUTION_MODE={new_mode}")

        ENV_PATH.write_text("\n".join(new_lines) + "\n", encoding="utf-8")
        return True, new_mode
    except Exception as exc:
        return False, f"Failed to update mode: {exc}"


def _safe_float(value, default: float = 0.0) -> float:
    try:
        if value is None or value == "":
            return default
        return float(value)
    except Exception:
        return default


def _utc_today_str() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%d")


def _parse_utc_loose(raw: str) -> Optional[datetime]:
    s = str(raw or "").strip()
    if not s:
        return None

    fmts = [
        "%Y-%m-%d %H:%M:%S UTC",
        "%Y-%m-%d %H:%M:%S",
        "%Y-%m-%dT%H:%M:%SZ",
        "%Y-%m-%d",
    ]
    for fmt in fmts:
        try:
            dt = datetime.strptime(s, fmt)
            if fmt == "%Y-%m-%d":
                dt = dt.replace(hour=0, minute=0, second=0)
            return dt.replace(tzinfo=timezone.utc)
        except Exception:
            continue
    return None


def _cfg_file_attr(attr_name: str, fallback_name: str) -> Path:
    try:
        value = getattr(CONFIG.FILES, attr_name)
        if value:
            return Path(value)
    except Exception:
        pass
    return BASE_DIR / "data" / fallback_name


def _open_orders_csv() -> Path:
    return _cfg_file_attr("OPEN_ORDERS_CSV", "open_orders.csv")


def _open_positions_csv() -> Path:
    return _cfg_file_attr("OPEN_POSITIONS_CSV", "open_positions.csv")


def _closed_orders_csv() -> Path:
    return _cfg_file_attr("CLOSED_ORDERS_CSV", "closed_orders.csv")


def _closed_positions_csv() -> Path:
    return _cfg_file_attr("CLOSED_POSITIONS_CSV", "closed_positions.csv")


def _read_csv_rows(path: Path) -> list[dict]:
    if not path.exists():
        return []

    try:
        with open(path, "r", newline="", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            return list(reader)
    except Exception as exc:
        log.warning("CSV_READ_ERROR path=%s error=%s", path, exc)
        return []


def _read_log_tail(path: Path, limit: int = 8) -> list[str]:
    if not path.exists():
        return []

    try:
        lines = path.read_text(encoding="utf-8", errors="ignore").splitlines()
        cleaned = [line for line in lines if line.strip()]
        return cleaned[-limit:]
    except Exception as exc:
        log.warning("LOG_READ_ERROR path=%s error=%s", path, exc)
        return []


def _format_rows(title: str, rows: list[dict], columns: list[str], limit: int = 8) -> str:
    total = len(rows)
    if total == 0:
        return f"{title}\nTotal: 0\nNo records."

    tail = rows[-limit:]
    lines = [f"{title}\nTotal: {total}\nShowing last {len(tail)}"]

    for i, row in enumerate(tail, start=1):
        parts = [f"{col}={row.get(col, '')}" for col in columns]
        lines.append(f"{i}. " + " | ".join(parts))

    return "\n".join(lines)[:3900]


def _format_log_lines(title: str, lines: list[str]) -> str:
    if not lines:
        return f"{title}\nTotal: 0\nNo records."

    out = [f"{title}\nTotal: {len(lines)} shown"]
    for idx, line in enumerate(lines, start=1):
        out.append(f"{idx}. {line}")
    return "\n".join(out)[:3900]


def _handle_menu(chat_id: str) -> None:
    current_mode = _read_current_mode()
    _send_reply(chat_id, f"Control menu ready.\nCurrent mode: {current_mode}", with_menu=True)


def _handle_help(chat_id: str) -> None:
    current_mode = _read_current_mode()
    text = (
        "Available commands:\n"
        "Status\n"
        "Run\n"
        "Stop\n"
        "Restart\n"
        "Mode\n"
        "Mode Paper\n"
        "Mode Real\n"
        "Confirm REAL\n"
        "Order Log\n"
        "Position Log\n"
        "Open Orders\n"
        "Open Positions\n"
        "Closed Orders\n"
        "Closed Positions\n"
        "PnL Summary\n"
        "Today Trades\n"
        "Last Closed\n"
        "Active Risk\n"
        "Help\n\n"
        f"Current mode: {current_mode}"
    )
    _send_reply(chat_id, text)


def _handle_status(chat_id: str) -> None:
    ok, output = _run_script("status.sh", timeout_sec=20)
    prefix = "STATUS OK\n" if ok else "STATUS FAIL\n"
    _send_reply(chat_id, prefix + output)


def _handle_run(chat_id: str) -> None:
    ok, output = _run_script("run.sh", timeout_sec=20)
    prefix = "RUN OK\n" if ok else "RUN FAIL\n"
    _send_reply(chat_id, prefix + output)


def _handle_stop(chat_id: str) -> None:
    ok, output = _run_script("stop.sh", timeout_sec=20)
    prefix = "STOP OK\n" if ok else "STOP FAIL\n"
    _send_reply(chat_id, prefix + output)


def _handle_restart(chat_id: str) -> None:
    ok1, out1 = _run_script("stop.sh", timeout_sec=20)
    time.sleep(1.0)
    ok2, out2 = _run_script("run.sh", timeout_sec=20)

    ok = ok1 and ok2
    text = (
        ("RESTART OK\n" if ok else "RESTART PARTIAL/FAIL\n")
        + f"[stop.sh]\n{out1}\n\n[run.sh]\n{out2}"
    )
    _send_reply(chat_id, text[:3500])


def _switch_mode(chat_id: str, new_mode: str) -> None:
    stop_ok, stop_out = _run_script("stop.sh", timeout_sec=20)
    write_ok, write_out = _write_mode_to_env(new_mode)

    if not write_ok:
        _send_reply(chat_id, f"MODE CHANGE FAIL\n{write_out}")
        return

    time.sleep(1.0)
    run_ok, run_out = _run_script("run.sh", timeout_sec=20)

    ok = stop_ok and run_ok
    text_out = (
        ("MODE SWITCH OK\n" if ok else "MODE SWITCH PARTIAL/FAIL\n")
        + f"New mode: {write_out}\n\n"
        + f"[stop.sh]\n{stop_out}\n\n"
        + f"[run.sh]\n{run_out}"
    )
    _send_reply(chat_id, text_out[:3500])


def _handle_mode(chat_id: str, text: str) -> None:
    normalized = text.strip().lower()

    if normalized in {"mode", "/mode"}:
        _send_reply(chat_id, f"Current mode: {_read_current_mode()}")
        return

    if normalized in {"mode paper", "/mode paper"}:
        _PENDING_REAL_CONFIRMATIONS.discard(chat_id)
        _switch_mode(chat_id, "PAPER")
        return

    if normalized in {"mode real", "/mode real"}:
        _PENDING_REAL_CONFIRMATIONS.add(chat_id)
        _send_reply(chat_id, "REAL mode requested.\nSend: Confirm REAL")
        return

    _send_reply(chat_id, "Use Mode, Mode Paper, or Mode Real")


def _handle_confirm_real(chat_id: str) -> None:
    if chat_id not in _PENDING_REAL_CONFIRMATIONS:
        _send_reply(chat_id, "No pending REAL mode request.")
        return

    _PENDING_REAL_CONFIRMATIONS.discard(chat_id)
    _switch_mode(chat_id, "REAL")


def _handle_order_log(chat_id: str) -> None:
    lines = _read_log_tail(BASE_DIR / "logs" / "order.log", limit=8)
    _send_reply(chat_id, _format_log_lines("ORDER LOG", lines))


def _handle_position_log(chat_id: str) -> None:
    lines = _read_log_tail(BASE_DIR / "logs" / "position.log", limit=8)
    _send_reply(chat_id, _format_log_lines("POSITION LOG", lines))


def _handle_open_orders(chat_id: str) -> None:
    rows = _read_csv_rows(_open_orders_csv())
    text = _format_rows(
        "OPEN ORDERS",
        rows,
        columns=[
            "symbol",
            "side",
            "status",
            "entry_trigger",
            "sl",
            "tp",
            "score",
            "rr",
            "created_at",
        ],
        limit=8,
    )
    _send_reply(chat_id, text)


def _handle_open_positions(chat_id: str) -> None:
    rows = _read_csv_rows(_open_positions_csv())
    text = _format_rows(
        "OPEN POSITIONS",
        rows,
        columns=[
            "symbol",
            "side",
            "status",
            "entry",
            "qty",
            "sl",
            "tp",
            "net_pnl_pct",
            "opened_at",
        ],
        limit=8,
    )
    _send_reply(chat_id, text)


def _handle_closed_orders(chat_id: str) -> None:
    rows = _read_csv_rows(_closed_orders_csv())
    text = _format_rows(
        "CLOSED ORDERS",
        rows,
        columns=[
            "symbol",
            "side",
            "status",
            "close_reason",
            "entry_trigger",
            "avg_fill_price",
            "score",
            "closed_at",
        ],
        limit=8,
    )
    _send_reply(chat_id, text)


def _handle_closed_positions(chat_id: str) -> None:
    rows = _read_csv_rows(_closed_positions_csv())
    text = _format_rows(
        "CLOSED POSITIONS",
        rows,
        columns=[
            "symbol",
            "side",
            "status",
            "close_reason",
            "entry",
            "close_price",
            "net_pnl_pct",
            "net_pnl_usdt",
            "closed_at",
        ],
        limit=8,
    )
    _send_reply(chat_id, text)


def _handle_pnl_summary(chat_id: str) -> None:
    rows = _read_csv_rows(_closed_positions_csv())
    total = len(rows)

    if total == 0:
        _send_reply(chat_id, "PNL SUMMARY\nTotal: 0\nNo closed positions.")
        return

    total_net_usdt = sum(_safe_float(r.get("net_pnl_usdt")) for r in rows)
    pnl_pcts = [_safe_float(r.get("net_pnl_pct")) for r in rows]
    wins = sum(1 for x in pnl_pcts if x > 0)
    losses = sum(1 for x in pnl_pcts if x < 0)
    breakeven = total - wins - losses
    avg_net_pct = sum(pnl_pcts) / total if total else 0.0

    text = (
        "PNL SUMMARY\n"
        f"Total: {total}\n"
        f"Wins: {wins}\n"
        f"Losses: {losses}\n"
        f"Breakeven: {breakeven}\n"
        f"Win Rate: {(wins / total * 100.0):.2f}%\n"
        f"Total Net PnL USDT: {total_net_usdt:.4f}\n"
        f"Average Net PnL %: {avg_net_pct:.4f}"
    )
    _send_reply(chat_id, text)


def _handle_today_trades(chat_id: str) -> None:
    rows = _read_csv_rows(_closed_positions_csv())
    today = _utc_today_str()

    today_rows = []
    for row in rows:
        raw = str(row.get("closed_at", "")).strip()
        if raw.startswith(today):
            today_rows.append(row)

    total = len(today_rows)
    total_net = sum(_safe_float(r.get("net_pnl_usdt")) for r in today_rows)
    wins = sum(1 for r in today_rows if _safe_float(r.get("net_pnl_pct")) > 0)
    losses = sum(1 for r in today_rows if _safe_float(r.get("net_pnl_pct")) < 0)

    lines = [
        "TODAY TRADES",
        f"Total: {total}",
        f"Wins: {wins}",
        f"Losses: {losses}",
        f"Net PnL USDT: {total_net:.4f}",
    ]

    for i, row in enumerate(today_rows[-8:], start=1):
        lines.append(
            f"{i}. symbol={row.get('symbol', '')} | side={row.get('side', '')} | "
            f"reason={row.get('close_reason', '')} | net_pnl_pct={row.get('net_pnl_pct', '')} | "
            f"net_pnl_usdt={row.get('net_pnl_usdt', '')} | closed_at={row.get('closed_at', '')}"
        )

    _send_reply(chat_id, "\n".join(lines)[:3900])


def _handle_last_closed(chat_id: str) -> None:
    rows = _read_csv_rows(_closed_positions_csv())
    if not rows:
        _send_reply(chat_id, "LAST CLOSED\nTotal: 0\nNo closed positions.")
        return

    row = rows[-1]
    text = (
        "LAST CLOSED\n"
        "Total: 1 shown\n"
        f"symbol={row.get('symbol', '')}\n"
        f"side={row.get('side', '')}\n"
        f"status={row.get('status', '')}\n"
        f"close_reason={row.get('close_reason', '')}\n"
        f"entry={row.get('entry', '')}\n"
        f"close_price={row.get('close_price', '')}\n"
        f"net_pnl_pct={row.get('net_pnl_pct', '')}\n"
        f"net_pnl_usdt={row.get('net_pnl_usdt', '')}\n"
        f"closed_at={row.get('closed_at', '')}"
    )
    _send_reply(chat_id, text)


def _handle_active_risk(chat_id: str) -> None:
    rows = _read_csv_rows(_open_positions_csv())
    total = len(rows)
    if total == 0:
        _send_reply(chat_id, "ACTIVE RISK\nTotal: 0\nNo open positions.")
        return

    total_risk_usdt = 0.0
    total_notional = 0.0
    lines = [f"ACTIVE RISK\nTotal: {total}\nShowing last {min(8, total)}"]

    for row in rows:
        entry = _safe_float(row.get("entry"))
        sl = _safe_float(row.get("sl"))
        qty = _safe_float(row.get("qty"))
        if entry > 0 and sl > 0 and qty > 0:
            total_risk_usdt += abs(entry - sl) * qty
            total_notional += entry * qty

    for i, row in enumerate(rows[-8:], start=1):
        entry = _safe_float(row.get("entry"))
        sl = _safe_float(row.get("sl"))
        qty = _safe_float(row.get("qty"))
        risk_usdt = abs(entry - sl) * qty if entry > 0 and sl > 0 and qty > 0 else 0.0

        lines.append(
            f"{i}. symbol={row.get('symbol', '')} | side={row.get('side', '')} | "
            f"qty={row.get('qty', '')} | entry={row.get('entry', '')} | sl={row.get('sl', '')} | "
            f"risk_usdt={risk_usdt:.4f} | pnl_pct={row.get('net_pnl_pct', '')}"
        )

    lines.insert(1, f"Total Risk USDT: {total_risk_usdt:.4f}")
    lines.insert(2, f"Total Notional: {total_notional:.4f}")

    _send_reply(chat_id, "\n".join(lines)[:3900])


def _handle_command(chat_id: str, text: str) -> None:
    cmd = text.strip()
    normalized = cmd.lower()

    log.info("TELEGRAM_COMMAND chat_id=%s text=%s", chat_id, cmd)

    if normalized in {"/start", "/menu", "menu"}:
        _handle_menu(chat_id)
    elif normalized in {"/help", "help"}:
        _handle_help(chat_id)
    elif normalized in {"/status", "status"}:
        _handle_status(chat_id)
    elif normalized in {"/run", "run"}:
        _handle_run(chat_id)
    elif normalized in {"/stop", "stop"}:
        _handle_stop(chat_id)
    elif normalized in {"/restart", "restart"}:
        _handle_restart(chat_id)
    elif normalized in {"/mode", "mode", "mode paper", "/mode paper", "mode real", "/mode real"}:
        _handle_mode(chat_id, cmd)
    elif normalized == "confirm real":
        _handle_confirm_real(chat_id)
    elif normalized in {"order log", "/order_log"}:
        _handle_order_log(chat_id)
    elif normalized in {"position log", "/position_log"}:
        _handle_position_log(chat_id)
    elif normalized in {"open orders", "/open_orders"}:
        _handle_open_orders(chat_id)
    elif normalized in {"open positions", "/open_positions"}:
        _handle_open_positions(chat_id)
    elif normalized in {"closed orders", "/closed_orders"}:
        _handle_closed_orders(chat_id)
    elif normalized in {"closed positions", "/closed_positions"}:
        _handle_closed_positions(chat_id)
    elif normalized in {"pnl summary", "/pnl_summary"}:
        _handle_pnl_summary(chat_id)
    elif normalized in {"today trades", "/today_trades"}:
        _handle_today_trades(chat_id)
    elif normalized in {"last closed", "/last_closed"}:
        _handle_last_closed(chat_id)
    elif normalized in {"active risk", "/active_risk"}:
        _handle_active_risk(chat_id)
    else:
        _send_reply(chat_id, "Unknown command. Tap a menu button or send Help")


def _poll_once() -> None:
    global _LAST_UPDATE_ID

    try:
        params = {
            "timeout": 20,
            "offset": _LAST_UPDATE_ID + 1,
        }
        resp = requests.get(telegram_api_url("getUpdates"), params=params, timeout=(5, 25))

        if resp.status_code == 409:
            log.warning("TELEGRAM_POLL_CONFLICT another bot instance is polling")
            time.sleep(5)
            return

        if resp.status_code != 200:
            log.warning("TELEGRAM_POLL_FAIL status=%s body=%s", resp.status_code, resp.text[:300])
            return

        data = resp.json()
        if not data.get("ok"):
            log.warning("TELEGRAM_POLL_NOT_OK body=%s", str(data)[:300])
            return

        for item in data.get("result", []):
            update_id = int(item.get("update_id", 0))
            if update_id > _LAST_UPDATE_ID:
                _LAST_UPDATE_ID = update_id

            message = item.get("message") or {}
            chat = message.get("chat") or {}
            chat_id = str(chat.get("id", "")).strip()
            text = str(message.get("text", "")).strip()

            if not chat_id or not text:
                continue

            if not _is_authorized_chat(chat_id):
                log.warning("TELEGRAM_UNAUTHORIZED_CHAT chat_id=%s", chat_id)
                continue

            _handle_command(chat_id, text)

    except Exception as exc:
        log.warning("TELEGRAM_POLL_ERROR error=%s", exc)


def telegram_command_loop(poll_interval_sec: float = 2.0) -> None:
    global _STOP_FLAG
    _STOP_FLAG = False

    if not telegram_enabled():
        log.info("TELEGRAM_COMMAND_LOOP_DISABLED")
        return

    log.info("TELEGRAM_COMMAND_LOOP_START")
    while not _STOP_FLAG:
        _poll_once()
        time.sleep(max(0.2, poll_interval_sec))


def start_telegram_command_listener() -> None:
    global _POLL_THREAD
    if _POLL_THREAD and _POLL_THREAD.is_alive():
        return

    if not telegram_enabled():
        log.info("TELEGRAM_LISTENER_NOT_STARTED missing token/chat_id")
        return

    _POLL_THREAD = threading.Thread(
        target=telegram_command_loop,
        name="telegram-command-listener",
        daemon=True,
    )
    _POLL_THREAD.start()
    log.info("TELEGRAM_LISTENER_STARTED")


def stop_telegram_command_listener() -> None:
    global _STOP_FLAG
    _STOP_FLAG = True
    log.info("TELEGRAM_LISTENER_STOP_REQUESTED")


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


if __name__ == "__main__":
    log.info("NOTIFIER_DAEMON_START")
    try:
        telegram_command_loop()
    except KeyboardInterrupt:
        log.info("NOTIFIER_DAEMON_STOP")