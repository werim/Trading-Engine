import time
import storage
from config import CONFIG
from logger import get_logger
from position import process_positions_once
from state import touch_engine_heartbeat

log = get_logger("position", "logs/position.log")


def main() -> None:
    log.info("POSITION_ENGINE_START mode=%s loop=%s", CONFIG.ENGINE.EXECUTION_MODE, CONFIG.ENGINE.LOOP_SECONDS_POSITION)
    storage.initialize_storage()
    while True:
        try:
            process_positions_once()
            touch_engine_heartbeat("position", {"mode": CONFIG.ENGINE.EXECUTION_MODE})
        except Exception as exc:
            log.exception("POSITION_LOOP_ERROR error=%s", exc)

        time.sleep(CONFIG.ENGINE.LOOP_SECONDS_POSITION)


if __name__ == "__main__":
    main()