#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from dataclasses import dataclass
from env import load_env

load_env()


@dataclass
class EngineConfig:
    EXECUTION_MODE: str = "REAL"      # PAPER | REAL
    STRATEGY_MODE: str = "BALANCED"   # AGGRESSIVE | BALANCED | SAFE | AUTO

    @property
    def MODE(self) -> str:
        return self.EXECUTION_MODE


@dataclass
class TradeConfig:

    # =========================
    # TIMEFRAMES
    # =========================
    HTF_INTERVAL: str = "1d"
    MTF_INTERVAL: str = "4h"
    LTF_INTERVAL: str = "1h"
    MICRO_INTERVAL: str = "15m"

    KLINE_LIMIT: int = 220

    # =========================
    # INDICATORS
    # =========================
    ATR_PERIOD: int = 14
    EMA_FAST: int = 20
    EMA_MID: int = 50

    # =========================
    # STRUCTURE / SETUP
    # =========================

    MIN_NET_PROFIT_PCT: float = 0.0035  # 0.35%
    MIN_NET_PROFIT_USDT: float = 0.35  # minimum expected net dollars per trade
    MIN_NET_RR: float = 1.25  # fee-adjusted RR floor

    NET_PROFIT_MODE: bool = True

    BINANCE_FEE_MAKER: float = 0.0002  # 0.02%
    BINANCE_FEE_TAKER: float = 0.0004  # 0.04%

    # how your entry/exit usually execute
    ENTRY_FEE_RATE: float = 0.0004  # stop-limit can still behave like taker often
    EXIT_FEE_RATE: float = 0.0004  # tp/sl execution usually assume taker for safety

    # minimum net profit after commissions
    MIN_NET_PROFIT_PCT: float = 0.003  # 0.30%

    # optional safety cushion for spread/slippage
    EXTRA_COST_PCT: float = 0.0005  # 0.05%

    SWING_LOOKBACK: int = 6
    BREAKOUT_LOOKBACK: int = 20

    PULLBACK_ZONE_BUFFER_PCT: float = 0.0015
    BREAKOUT_BUFFER_PCT: float = 0.0010
    SL_BUFFER_PCT: float = 0.0010
    TP_R_MULTIPLIER: float = 2.0

    # legacy / compatibility
    ZONE_PADDING_PCT: float = 0.0015
    SL_BUFFER_ATR_MULTIPLIER: float = 0.8
    TP_ATR_MULTIPLIER: float = 2.2

    # =========================
    # RISK / FILTERS
    # =========================
    RR_MIN: float = 1.6
    RR_DEFAULT: float = 2.0

    MIN_SL_PCT: float = 0.002
    MAX_SL_PCT: float = 0.05
    RISK_PER_TRADE_PCT: float = 0.01

    MIN_SCORE: int = 2
    BLOCK_DUPLICATE_SYMBOL_STATE: bool = False

    # =========================
    # SCAN / ORDER ENGINE
    # =========================
    DEFAULT_SCAN_TOP_VOL: int = 100
    SAFE_SCAN_TOP_VOL: int = 50

    MAX_OPEN_ORDERS: int = 30
    MAX_CANDIDATES_PER_CYCLE: int = 30

    REPRICE_THRESHOLD_PCT: float = 0.0015

    ORDER_USDT_SIZE: float = 100.0
    ENTRY_LONG_BUFFER_PCT: float = 0.0003
    ENTRY_SHORT_BUFFER_PCT: float = 0.0003
    WORKING_TYPE: str = "CONTRACT_PRICE"

    ALLOW_REPLACE_ARMED_ORDER: bool = True
    ALLOW_SIDE_FLIP_REPLACEMENT: bool = True

    # =========================
    # FILES / LOGS
    # =========================
    ORDER_LOG_FILE: str = "logs/order.log"
    POSITION_LOG_FILE: str = "logs/position.log"
    EVENT_LOG_FILE = "logs/event_log.csv"

    WS_URL: str = "wss://fstream.binance.com/ws/!ticker@arr"

    PAPER_OPEN_ORDERS_FILE: str = "data/open_orders.csv"
    PAPER_CLOSED_ORDERS_FILE: str = "data/closed_orders.csv"
    PAPER_OPEN_POSITIONS_FILE: str = "data/open_positions.csv"
    PAPER_CLOSED_POSITIONS_FILE: str = "data/closed_positions.csv"

    REAL_OPEN_ORDERS_FILE: str = "data/real_open_orders.csv"
    REAL_CLOSED_ORDERS_FILE: str = "data/real_closed_orders.csv"
    REAL_OPEN_POSITIONS_FILE: str = "data/real_open_positions.csv"
    REAL_CLOSED_POSITIONS_FILE: str = "data/real_closed_positions.csv"


@dataclass
class AlarmConfig:
    TOUCHED_ORDER_ENABLED: bool = True
    NEAR_TRIGGER_ENABLED: bool = True
    NEAR_TRIGGER_PCT: float = 0.002
    TOUCHED_EMOJI: str = "🟡"
    TRIGGER_EMOJI: str = "🟠"


@dataclass
class WSSSLConfig:
    VERIFY_CERT: bool = True
    ALLOW_INSECURE_FALLBACK: bool = True


@dataclass
class AdaptiveConfig:
    SCORE_FILE: str = "data/score.txt"

    AUTO_SAFE_THRESHOLD: int = -3
    AUTO_BALANCED_THRESHOLD: int = 2
    # score <= -3 => SAFE
    # -2 .. 1     => BALANCED
    # >= 2        => AGGRESSIVE


class Config:
    ENGINE = EngineConfig()
    TRADE = TradeConfig()
    ALARM = AlarmConfig()
    WS_SSL = WSSSLConfig()
    ADAPTIVE = AdaptiveConfig()


    def get_mode_settings(self, score: int = 0) -> dict:
        strategy_mode = self.ENGINE.STRATEGY_MODE.upper()

        if strategy_mode == "AGGRESSIVE":
            return {
                "NAME": "AGGRESSIVE",
                "MIN_SIGNAL_SCORE": 2,
                "MAX_OPEN_POSITIONS": 10,
                "MAX_SYMBOLS_SCAN": self.TRADE.DEFAULT_SCAN_TOP_VOL,   # 200
            }

        if strategy_mode == "BALANCED":
            return {
                "NAME": "BALANCED",
                "MIN_SIGNAL_SCORE": 4,
                "MAX_OPEN_POSITIONS": 7,
                "MAX_SYMBOLS_SCAN": self.TRADE.DEFAULT_SCAN_TOP_VOL,   # 200
            }

        if strategy_mode == "SAFE":
            return {
                "NAME": "SAFE",
                "MIN_SIGNAL_SCORE": 4,
                "MAX_OPEN_POSITIONS": 4,
                "MAX_SYMBOLS_SCAN": self.TRADE.SAFE_SCAN_TOP_VOL,      # 50
            }

        if strategy_mode == "AUTO":
            if score <= self.ADAPTIVE.AUTO_SAFE_THRESHOLD:
                return {
                    "NAME": "SAFE",
                    "MIN_SIGNAL_SCORE": 3,
                    "MAX_OPEN_POSITIONS": 4,
                    "MAX_SYMBOLS_SCAN": self.TRADE.SAFE_SCAN_TOP_VOL,  # 50
                }

            if score >= self.ADAPTIVE.AUTO_BALANCED_THRESHOLD:
                return {
                    "NAME": "AGGRESSIVE",
                    "MIN_SIGNAL_SCORE": 2,
                    "MAX_OPEN_POSITIONS": 10,
                    "MAX_SYMBOLS_SCAN": self.TRADE.DEFAULT_SCAN_TOP_VOL,  # 200
                }

            return {
                "NAME": "BALANCED",
                "MIN_SIGNAL_SCORE": 4,
                "MAX_OPEN_POSITIONS": 7,
                "MAX_SYMBOLS_SCAN": self.TRADE.DEFAULT_SCAN_TOP_VOL,   # 200
            }

        return {
            "NAME": "BALANCED",
            "MIN_SIGNAL_SCORE": 4,
            "MAX_OPEN_POSITIONS": 7,
            "MAX_SYMBOLS_SCAN": self.TRADE.DEFAULT_SCAN_TOP_VOL,       # 200
        }


CONFIG = Config()