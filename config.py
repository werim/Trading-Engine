import os
from dataclasses import dataclass

from dotenv import load_dotenv

load_dotenv()


def _get_bool(name: str, default: str = "0") -> bool:
    return os.getenv(name, default).strip() in {"1", "true", "True", "YES", "yes"}


def _get_int(name: str, default: str) -> int:
    return int(os.getenv(name, default))


def _get_float(name: str, default: str) -> float:
    return float(os.getenv(name, default))


@dataclass(frozen=True)
class EngineConfig:
    EXECUTION_MODE: str = os.getenv("EXECUTION_MODE", "PAPER").upper()
    LOOP_SECONDS_ORDER: int = _get_int("ENGINE_LOOP_SECONDS_ORDER", "15")
    LOOP_SECONDS_POSITION: int = _get_int("ENGINE_LOOP_SECONDS_POSITION", "5")
    MAX_SYMBOLS: int = _get_int("MAX_SYMBOLS", "100")
    LOG_LEVEL: str = os.getenv("LOG_LEVEL", "INFO")


@dataclass(frozen=True)
class TradeConfig:
    LEVERAGE: int = _get_int("LEVERAGE", "5")
    RISK_PER_TRADE_PCT: float = _get_float("RISK_PER_TRADE_PCT", "0.5")
    MAX_OPEN_ORDERS: int = _get_int("MAX_OPEN_ORDERS", "3")
    MAX_OPEN_POSITIONS: int = _get_int("MAX_OPEN_POSITIONS", "3")
    USE_LIMIT_ENTRY: bool = _get_bool("USE_LIMIT_ENTRY", "1")
    ENABLE_TRAILING: bool = _get_bool("ENABLE_TRAILING", "1")
    BREAK_EVEN_TRIGGER_R: float = _get_float("BREAK_EVEN_TRIGGER_R", "1.5")
    TRAIL_AFTER_R: float = _get_float("TRAIL_AFTER_R", "1.5")
    PARTIAL_TP_AT_R: float = _get_float("PARTIAL_TP_AT_R", "1.0")
    PARTIAL_CLOSE_RATIO: float = _get_float("PARTIAL_CLOSE_RATIO", "0.40")
    DEAD_TRADE_MAX_DEVIATION_PCT: float = _get_float("DEAD_TRADE_MAX_DEVIATION_PCT", "1.0")
    BREAKOUT_CONFIRM_PCT: float = _get_float("BREAKOUT_CONFIRM_PCT", "0.12")
    MAX_NEW_ORDERS_PER_SCAN: int = _get_int("MAX_NEW_ORDERS_PER_SCAN", "3")
    ORDER_COOLDOWN_MINUTES: int = _get_int("ORDER_COOLDOWN_MINUTES", "90")
    LIMIT_ENTRY_SLIPPAGE_PCT: float = _get_float("LIMIT_ENTRY_SLIPPAGE_PCT", "0.0")
    MARKET_ENTRY_SLIPPAGE_PCT: float = _get_float("MARKET_ENTRY_SLIPPAGE_PCT", "0.035")


@dataclass(frozen=True)
class FilterConfig:
    MIN_SCORE: int = _get_int("MIN_SCORE", "4")
    MIN_RR: float = _get_float("MIN_RR", "1.6")
    MIN_24H_VOLUME_USDT: float = _get_float("MIN_24H_VOLUME_USDT", "20000000")
    MAX_SPREAD_PCT: float = _get_float("MAX_SPREAD_PCT", "0.15")
    MIN_EXPECTED_NET_PNL_PCT: float = _get_float("MIN_EXPECTED_NET_PNL_PCT", "0.35")
    MAX_FUNDING_RATE_PCT: float = _get_float("MAX_FUNDING_RATE_PCT", "0.05")
    MIN_ADAPTIVE_EXPECTANCY: float = _get_float("MIN_ADAPTIVE_EXPECTANCY", "0.0")
    STRICT_EXPECTANCY_BLOCK: bool = _get_bool("STRICT_EXPECTANCY_BLOCK", "1")


@dataclass(frozen=True)
class StrategyConfig:
    BREAKOUT_SL_ATR_MULT: float = 1.2
    BREAKOUT_RR_MULT: float = 2.7
    PULLBACK_SL_ATR_MULT: float = 1.0
    PULLBACK_RR_MULT: float = 2.7
    RANGE_PULLBACK_SL_ATR_MULT: float = 0.9
    RANGE_PULLBACK_RR_MULT: float = 2.2
    RANGE_BREAKOUT_SL_ATR_MULT: float = 1.1
    RANGE_BREAKOUT_RR_MULT: float = 2.3


@dataclass(frozen=True)
class MarketDataConfig:
    USE_LOCAL_CACHE: bool = _get_bool("USE_LOCAL_MARKET_CACHE", "1")
    LOCAL_CACHE_BASE_URL: str = os.getenv("LOCAL_MARKET_CACHE_BASE_URL", "http://127.0.0.1:8000")
    LOCAL_CACHE_TIMEOUT_SECONDS: float = _get_float("LOCAL_MARKET_CACHE_TIMEOUT_SECONDS", "1.2")


@dataclass(frozen=True)
class FilesConfig:
    OPEN_ORDERS_CSV: str = "data/open_orders.csv"
    CLOSED_ORDERS_CSV: str = "data/closed_orders.csv"
    OPEN_POSITIONS_CSV: str = "data/open_positions.csv"
    CLOSED_POSITIONS_CSV: str = "data/closed_positions.csv"
    FILLS_CSV: str = "data/fills.csv"
    EQUITY_CSV: str = "data/equity.csv"
    SYMBOL_META_JSON: str = "data/symbol_meta.json"
    ENGINE_STATE_JSON: str = "data/engine_state.json"


@dataclass(frozen=True)
class TelegramConfig:
    BOT_TOKEN: str = os.getenv("TELEGRAM_BOT_TOKEN", "")
    CHAT_ID: str = os.getenv("TELEGRAM_CHAT_ID", "")


class CONFIG:
    ENGINE = EngineConfig()
    TRADE = TradeConfig()
    FILTER = FilterConfig()
    STRATEGY = StrategyConfig()
    MARKET_DATA = MarketDataConfig()
    FILES = FilesConfig()
    TELEGRAM = TelegramConfig()
