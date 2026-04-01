"""Central configuration — loads all env vars from .env file."""

import os
import sys
from dataclasses import dataclass

from dotenv import load_dotenv

load_dotenv()

REQUIRED_VARS: list[str] = [
    "BINANCE_API_KEY",
    "BINANCE_API_SECRET",
]


@dataclass(frozen=True)
class Config:
    """Application configuration loaded from environment variables."""

    BINANCE_API_KEY: str
    BINANCE_API_SECRET: str
    DATABASE_PATH: str
    LOG_LEVEL: str

    # Exchange settings
    EXCHANGE: str = "binance_us"
    BASE_CURRENCY: str = "BTCUSD"
    MAKER_FEE: float = 0.0000       # Binance.US Tier 0: 0% maker
    TAKER_FEE: float = 0.000095     # Binance.US: 0.0095% taker
    ORDER_TYPE: str = "LIMIT"        # NEVER market orders

    # Market Maker Settings
    MM_CAPITAL_USD: float = 75.0
    MM_SPREAD_PCT: float = 0.0015      # 0.15% spread each side
    MM_ORDER_REFRESH_SEC: int = 30     # refresh orders every 30s
    MM_MAX_INVENTORY_PCT: float = 0.8  # max 80% BTC inventory
    MM_MIN_SPREAD_PCT: float = 0.0008  # min spread to place orders
    MM_PAPER_TRADING: bool = True      # start in paper mode


def load_config() -> Config:
    """Load and validate configuration from environment variables.

    Exits with error code 1 if any required variable is missing.
    """
    missing = [var for var in REQUIRED_VARS if not os.getenv(var)]
    if missing:
        for var in missing:
            print(f"ERROR: Required environment variable {var} is not set")
        print("\nCopy .env.example to .env and fill in your values.")
        sys.exit(1)

    return Config(
        BINANCE_API_KEY=os.environ["BINANCE_API_KEY"],
        BINANCE_API_SECRET=os.environ["BINANCE_API_SECRET"],
        DATABASE_PATH=os.getenv("DATABASE_PATH", "./onequant.db"),
        LOG_LEVEL=os.getenv("LOG_LEVEL", "INFO"),
    )


config: Config = load_config()
