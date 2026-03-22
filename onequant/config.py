"""Central configuration — loads all env vars from .env file."""

import os
import sys
from dataclasses import dataclass

from dotenv import load_dotenv

load_dotenv()

REQUIRED_VARS: list[str] = [
    "COINBASE_API_KEY",
    "COINBASE_API_SECRET",
]


@dataclass(frozen=True)
class Config:
    """Application configuration loaded from environment variables."""

    COINBASE_API_KEY: str
    COINBASE_API_SECRET: str
    DATABASE_PATH: str
    LOG_LEVEL: str


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
        COINBASE_API_KEY=os.environ["COINBASE_API_KEY"],
        COINBASE_API_SECRET=os.environ["COINBASE_API_SECRET"],
        DATABASE_PATH=os.getenv("DATABASE_PATH", "./onequant.db"),
        LOG_LEVEL=os.getenv("LOG_LEVEL", "INFO"),
    )


config: Config = load_config()
