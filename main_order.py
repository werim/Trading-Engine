import time
from datetime import datetime, timedelta, timezone

import storage
from config import CONFIG
from logger import get_logger
from order import scan_once
from state import touch_engine_heartbeat

log = get_logger("order_runner", "logs/order.log")


def sleep_until_next_interval(minutes: int) -> None:
    now = datetime.now(timezone.utc)

    next_minute = (now.minute // minutes + 1) * minutes

    if next_minute >= 60:
        next_run = now.replace(minute=0, second=0, microsecond=0) + timedelta(hours=1)
    else:
        next_run = now.replace(minute=next_minute, second=0, microsecond=0)

    sleep_seconds = (next_run - now).total_seconds()

    log.info(
        "ORDER_NEXT_RUN_AT next_run=%s sleep_seconds=%.2f",
        next_run.strftime("%Y-%m-%d %H:%M:%S UTC"),
        sleep_seconds,
    )

    if sleep_seconds > 0:
        time.sleep(sleep_seconds)


def main() -> None:
    storage.initialize_storage()
    interval_min = 5

    log.info(
        "ORDER_ENGINE_START mode=%s interval=%sm",
        CONFIG.ENGINE.EXECUTION_MODE,
        interval_min,
    )

    # İlk çalışmayı da tam slot'a hizala
    sleep_until_next_interval(interval_min)

    while True:
        try:
            log.info("ORDER_SCAN_START")
            scan_once()
            touch_engine_heartbeat("order", {"mode": CONFIG.ENGINE.EXECUTION_MODE})
            log.info("ORDER_SCAN_DONE")
        except Exception as exc:
            log.exception("ORDER_LOOP_ERROR error=%s", exc)

        sleep_until_next_interval(interval_min)


if __name__ == "__main__":
    main()