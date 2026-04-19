import time

from config import CONFIG
from logger import get_logger
from order import scan_once
from state import touch_engine_heartbeat
import storage

log = get_logger("order", "logs/order.log")


def main() -> None:
    storage.initialize_storage()
    log.info("ORDER_ENGINE_START mode=%s loop=%s", CONFIG.ENGINE.EXECUTION_MODE, CONFIG.ENGINE.LOOP_SECONDS_ORDER)

    while True:
        try:
            scan_once()
            touch_engine_heartbeat("order", {"mode": CONFIG.ENGINE.EXECUTION_MODE})
        except Exception as exc:
            log.exception("ORDER_LOOP_ERROR error=%s", exc)

        time.sleep(CONFIG.ENGINE.LOOP_SECONDS_ORDER)


if __name__ == "__main__":
    main()