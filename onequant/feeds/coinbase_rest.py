"""Coinbase Advanced Trade REST helpers.

Provides authenticated access to the Coinbase Advanced Trade API for
fetching the current BTC-USD price and account balances.
"""

import hashlib
import hmac
import logging
import time
from pathlib import Path
from typing import Any, Optional

import aiohttp

from config import config
from database.db import insert_system_log

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

BASE_URL: str = "https://api.coinbase.com"
PRODUCT_ID: str = "BTC-USD"
MODULE_NAME: str = "coinbase_rest"

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------

logger: logging.Logger = logging.getLogger(MODULE_NAME)


def _setup_logging() -> None:
    """Configure file and console logging for the REST module."""
    if logger.handlers:
        return
    logger.setLevel(logging.DEBUG)
    Path("logs").mkdir(exist_ok=True)

    fh = logging.FileHandler("logs/coinbase_rest.log")
    fh.setLevel(logging.DEBUG)

    ch = logging.StreamHandler()
    ch.setLevel(getattr(logging, config.LOG_LEVEL, logging.INFO))

    fmt = logging.Formatter("%(asctime)s [%(levelname)s] %(name)s — %(message)s")
    fh.setFormatter(fmt)
    ch.setFormatter(fmt)
    logger.addHandler(fh)
    logger.addHandler(ch)


# ---------------------------------------------------------------------------
# Auth helpers
# ---------------------------------------------------------------------------


def _auth_headers(method: str, path: str, body: str = "") -> dict[str, str]:
    """Generate Coinbase Advanced Trade authentication headers."""
    timestamp = str(int(time.time()))
    message = timestamp + method.upper() + path + body
    signature = hmac.new(
        config.COINBASE_API_SECRET.encode("utf-8"),
        message.encode("utf-8"),
        hashlib.sha256,
    ).hexdigest()
    return {
        "CB-ACCESS-KEY": config.COINBASE_API_KEY,
        "CB-ACCESS-SIGN": signature,
        "CB-ACCESS-TIMESTAMP": timestamp,
        "Content-Type": "application/json",
    }


async def _get(path: str) -> Optional[dict[str, Any]]:
    """Make an authenticated GET request. Returns JSON or None on error."""
    headers = _auth_headers("GET", path)
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(BASE_URL + path, headers=headers) as resp:
                if resp.status != 200:
                    text = await resp.text()
                    logger.error("GET %s → %s: %s", path, resp.status, text)
                    return None
                return await resp.json()
    except Exception as exc:
        msg = f"Request failed GET {path}: {exc}"
        logger.error(msg)
        try:
            await insert_system_log(MODULE_NAME, "ERROR", msg)
        except Exception:
            pass
        return None


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


async def get_current_price() -> Optional[float]:
    """Return the latest BTC-USD mid price, or None on failure."""
    _setup_logging()
    data = await _get(f"/api/v3/brokerage/market/products/{PRODUCT_ID}")
    if data is None:
        return None
    try:
        bid = float(data["price"])
        return bid
    except (KeyError, ValueError, TypeError) as exc:
        logger.error("Failed to parse price response: %s", exc)
        return None


async def get_account_balance() -> Optional[dict[str, float]]:
    """Return USD and BTC balances as {'USD': float, 'BTC': float}, or None.

    Intended for future phases — provides balance visibility.
    """
    _setup_logging()
    data = await _get("/api/v3/brokerage/accounts")
    if data is None:
        return None
    try:
        balances: dict[str, float] = {}
        for account in data.get("accounts", []):
            currency = account.get("currency", "")
            if currency in ("USD", "BTC"):
                balances[currency] = float(
                    account.get("available_balance", {}).get("value", 0)
                )
        return balances
    except (KeyError, ValueError, TypeError) as exc:
        logger.error("Failed to parse accounts response: %s", exc)
        return None
