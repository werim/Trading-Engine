from typing import Any, Dict

from config import CONFIG
from utils import read_json, utc_now_str, write_json_atomic


def load_engine_state() -> Dict[str, Any]:
    return read_json(CONFIG.FILES.ENGINE_STATE_JSON, default={}) or {}


def save_engine_state(state: Dict[str, Any]) -> None:
    write_json_atomic(CONFIG.FILES.ENGINE_STATE_JSON, state)


def touch_engine_heartbeat(component: str, extra: Dict[str, Any] | None = None) -> None:
    state = load_engine_state()
    state[component] = {
        "last_heartbeat": utc_now_str(),
        **(extra or {}),
    }
    save_engine_state(state)