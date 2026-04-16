from dotenv import load_dotenv

load_dotenv()

from dataclasses import dataclass, field
import os


@dataclass
class EngineConfig:
    EXECUTION_MODE: str = os.getenv("EXECUTION_MODE", "PAPER").upper()  # PAPER | REAL
    BASE_DIR: str = os.getenv("BASE_DIR", ".")
    DATA_DIR: str = os.getenv("DATA_DIR", "data")
    LOG_DIR: str = os.getenv("LOG_DIR", "logs")


@dataclass
class BinanceConfig:
    API_KEY: str = os.getenv("BINANCE_API_KEY", "").strip()
    API_SECRET: str = os.getenv("BINANCE_API_SECRET", "").strip()
    BASE_URL: str = os.getenv("BINANCE_BASE_URL", "https://fapi.binance.com").strip()
    RECV_WINDOW: int = int(os.getenv("BINANCE_RECV_WINDOW", "5000"))
    TESTNET: bool = os.getenv("BINANCE_TESTNET", "0") == "1"
    EXCHANGE_INFO_TTL_SEC: int = int(os.getenv("EXCHANGE_INFO_TTL_SEC", "1800"))


@dataclass
class TradeConfig:
    # Universe
    QUOTE_ASSET: str = "USDT"
    MAX_SYMBOLS: int = int(os.getenv("MAX_SYMBOLS", "60"))
    SL_MULTIPLIER: float = float(os.getenv("SL_MULTIPLIER", "1.0"))
    TRAIL_FACTOR: float = float(os.getenv("TRAIL_FACTOR", "1.0"))

    # Timeframes
    HTF_INTERVAL: str = os.getenv("HTF_INTERVAL", "1d")
    MTF_INTERVAL: str = os.getenv("MTF_INTERVAL", "4h")
    LTF_INTERVAL: str = os.getenv("LTF_INTERVAL", "1h")
    MICRO_INTERVAL: str = os.getenv("MICRO_INTERVAL", "15m")
    KLINE_LIMIT: int = int(os.getenv("KLINE_LIMIT", "200"))

    # Indicators
    EMA_FAST: int = int(os.getenv("EMA_FAST", "20"))
    EMA_MID: int = int(os.getenv("EMA_MID", "50"))
    EMA_SLOW: int = int(os.getenv("EMA_SLOW", "200"))
    ATR_PERIOD: int = int(os.getenv("ATR_PERIOD", "14"))
    SWING_LOOKBACK: int = int(os.getenv("SWING_LOOKBACK", "8"))
    BREAKOUT_LOOKBACK: int = int(os.getenv("BREAKOUT_LOOKBACK", "20"))

    # Core filters
    SCORE_MIN: int = int(os.getenv("SCORE_MIN", "5"))
    RR_MIN: float = float(os.getenv("RR_MIN", "2.2"))
    RR_IDEAL: float = float(os.getenv("RR_IDEAL", "2.8"))
    MIN_EXPECTED_NET_PNL_PCT: float = float(os.getenv("MIN_EXPECTED_NET_PNL_PCT", "1.20"))
    MIN_STOP_PCT: float = float(os.getenv("MIN_STOP_PCT", "0.75"))
    MAX_STOP_PCT: float = float(os.getenv("MAX_STOP_PCT", "3.80"))
    MIN_TP_PCT: float = float(os.getenv("MIN_TP_PCT", "1.80"))
    MAX_TP_PCT: float = float(os.getenv("MAX_TP_PCT", "8.50"))
    MIN_VOLUME_USDT_24H: float = float(os.getenv("MIN_VOLUME_USDT_24H", "5000000"))
    MAX_SPREAD_PCT: float = float(os.getenv("MAX_SPREAD_PCT", "0.20"))
    MAX_FUNDING_ABS_PCT: float = float(os.getenv("MAX_FUNDING_ABS_PCT", "0.08"))

    # Old switch kept for compatibility, but no longer the main idea
    SKIP_RANGE_DAILY: bool = os.getenv("SKIP_RANGE_DAILY", "0") == "1"

    # Adaptive regime switches
    ENABLE_REGIME_FILTER: bool = os.getenv("ENABLE_REGIME_FILTER", "1") == "1"
    ALLOW_RANGE_PULLBACKS: bool = os.getenv("ALLOW_RANGE_PULLBACKS", "1") == "1"
    ALLOW_RANGE_BREAKOUTS: bool = os.getenv("ALLOW_RANGE_BREAKOUTS", "0") == "1"

    # Stricter rules during daily range
    RANGE_SCORE_MIN: int = int(os.getenv("RANGE_SCORE_MIN", "6"))
    RANGE_RR_MIN: float = float(os.getenv("RANGE_RR_MIN", "2.4"))
    RANGE_MIN_EXPECTED_NET_PNL_PCT: float = float(os.getenv("RANGE_MIN_EXPECTED_NET_PNL_PCT", "1.40"))

    # Execution / sizing
    MAX_OPEN_POSITIONS: int = int(os.getenv("MAX_OPEN_POSITIONS", "6"))
    USDT_PER_TRADE: float = float(os.getenv("USDT_PER_TRADE", "35"))
    LEVERAGE: int = int(os.getenv("LEVERAGE", "3"))
    USE_LIMIT_ENTRY: bool = os.getenv("USE_LIMIT_ENTRY", "1") == "1"
    ENTRY_BUFFER_BPS: float = float(os.getenv("ENTRY_BUFFER_BPS", "4"))
    SL_BUFFER_BPS: float = float(os.getenv("SL_BUFFER_BPS", "6"))
    TP_BUFFER_BPS: float = float(os.getenv("TP_BUFFER_BPS", "6"))

    # Costs
    MAKER_FEE_PCT: float = float(os.getenv("MAKER_FEE_PCT", "0.02"))
    TAKER_FEE_PCT: float = float(os.getenv("TAKER_FEE_PCT", "0.05"))
    ROUND_TRIP_SLIPPAGE_PCT: float = float(os.getenv("ROUND_TRIP_SLIPPAGE_PCT", "0.08"))

    # Position management
    BREAK_EVEN_TRIGGER_R: float = float(os.getenv("BREAK_EVEN_TRIGGER_R", "1.0"))
    TRAIL_AFTER_R: float = float(os.getenv("TRAIL_AFTER_R", "1.5"))
    PARTIAL_TP_AT_R: float = float(os.getenv("PARTIAL_TP_AT_R", "1.2"))
    PARTIAL_CLOSE_RATIO: float = float(os.getenv("PARTIAL_CLOSE_RATIO", "0.40"))
    ENABLE_TRAILING: bool = os.getenv("ENABLE_TRAILING", "1") == "1"

    # Loops
    ORDER_LOOP_SECONDS: int = int(os.getenv("ORDER_LOOP_SECONDS", "300"))
    POSITION_LOOP_SECONDS: int = int(os.getenv("POSITION_LOOP_SECONDS", "4"))

    # Logging and notifications
    TELEGRAM_ALERTS: bool = os.getenv("TELEGRAM_ALERTS", "1") == "1"
    ORDER_ALERT: bool = os.getenv("ORDER_ALERT", "1") == "1"


@dataclass
class FileConfig:
    OPEN_ORDERS_CSV: str = "data/open_orders.csv"
    OPEN_POSITIONS_CSV: str = "data/open_positions.csv"
    CLOSED_POSITIONS_CSV: str = "data/closed_positions.csv"
    SYMBOL_META_JSON: str = "data/symbol_meta.json"
    MARKET_CACHE_JSON: str = "data/market_cache.json"

    ORDER_LOG_FILE: str = "logs/order.log"
    POSITION_LOG_FILE: str = "logs/position.log"
    ENGINE_LOG_FILE: str = "logs/engine.log"

    # Order recreation suppression
    ORDER_RECREATE_COOLDOWN_MINUTES = 15
    ORDER_SAME_SETUP_COOLDOWN_MINUTES = 30
    ORDER_CANCEL_LOCK_WINDOW_MINUTES = 20
    ORDER_CANCEL_LOCK_COUNT = 2
    ORDER_CANCEL_LOCK_MINUTES = 45

    ORDER_REJECTED_COOLDOWN_MINUTES = 30
    ORDER_FAILED_SUBMIT_COOLDOWN_MINUTES = 15

    # Opsiyonel
    ORDER_REJECTED_COOLDOWN_MINUTES = 15
    ORDER_FAILED_SUBMIT_COOLDOWN_MINUTES = 10


@dataclass
class Config:
    ENGINE: EngineConfig = field(default_factory=EngineConfig)
    BINANCE: BinanceConfig = field(default_factory=BinanceConfig)
    TRADE: TradeConfig = field(default_factory=TradeConfig)
    FILES: FileConfig = field(default_factory=FileConfig)


class BINANCE:
    FUTURES_BASE_URL = "https://fapi.binance.com"
    REQUEST_TIMEOUT = 10
    EXCHANGE_INFO_TTL_SEC = 1800
    TICKER_CACHE_TTL_SEC = 2
    BOOK_TICKER_STALE_OK_SEC = 10
    NETWORK_COOLDOWN_SEC = 30
    MAX_LOG_SAME_ERROR_EVERY_SEC = 20
    USER_AGENT = "TradingEngine/1.0"


CONFIG = Config()
