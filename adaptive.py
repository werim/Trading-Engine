from config import CONFIG


def _ensure_score_file() -> None:
    try:
        with open(CONFIG.ADAPTIVE.SCORE_FILE, "x", encoding="utf-8") as f:
            f.write("0")
    except FileExistsError:
        pass


def get_score() -> int:
    _ensure_score_file()
    try:
        with open(CONFIG.ADAPTIVE.SCORE_FILE, "r", encoding="utf-8") as f:
            raw = f.read().strip()
        return int(raw or "0")
    except Exception:
        return 0


def set_score(value: int) -> None:
    _ensure_score_file()
    with open(CONFIG.ADAPTIVE.SCORE_FILE, "w", encoding="utf-8") as f:
        f.write(str(int(value)))


def increase_score(step: int = 1) -> int:
    value = get_score() + step
    set_score(value)
    return value


def decrease_score(step: int = 1) -> int:
    value = get_score() - step
    set_score(value)
    return value


def get_mode_settings() -> dict:
    return CONFIG.get_mode_settings(get_score())


def get_mode_name() -> str:
    return get_mode_settings().get("NAME", "BALANCED")


def get_execution_mode() -> str:
    return CONFIG.ENGINE.EXECUTION_MODE.upper()