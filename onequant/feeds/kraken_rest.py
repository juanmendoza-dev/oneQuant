"""Kraken REST API helpers.

Provides public and authenticated access to the Kraken REST API for
fetching the current BTC/USD price, OHLC candles, account balances,
and placing orders.
"""

import base64
import hashlib
import hmac
import logging
import time
import urllib.parse
from pathlib import Path
from typing import Any, Optional

import aiohttp

from config import config
from database.db import insert_system_log

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

BASE_URL: str = "https://api.kraken.com"
PAIR: str = "XBTUSD"
MODULE_NAME: str = "kraken_rest"

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

    fh = logging.FileHandler("logs/kraken_rest.log")
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


def _kraken_signature(urlpath: str, data: dict[str, Any], secret: str) -> str:
    """Generate Kraken API signature (HMAC-SHA512 of SHA256 nonce+postdata)."""
    postdata = urllib.parse.urlencode(data)
    encoded = (str(data["nonce"]) + postdata).encode()
    message = urlpath.encode() + hashlib.sha256(encoded).digest()
    mac = hmac.new(base64.b64decode(secret), message, hashlib.sha512)
    return base64.b64encode(mac.digest()).decode()


def _auth_headers(urlpath: str, data: dict[str, Any]) -> dict[str, str]:
    """Generate authenticated headers for Kraken private endpoints."""
    return {
        "API-Key": config.KRAKEN_API_KEY,
        "API-Sign": _kraken_signature(urlpath, data, config.KRAKEN_API_SECRET),
        "Content-Type": "application/x-www-form-urlencoded",
    }


# ---------------------------------------------------------------------------
# HTTP helpers
# ---------------------------------------------------------------------------


async def _public_get(path: str, params: Optional[dict] = None) -> Optional[dict[str, Any]]:
    """Make a public GET request to Kraken. Returns result dict or None."""
    _setup_logging()
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(BASE_URL + path, params=params) as resp:
                if resp.status != 200:
                    text = await resp.text()
                    logger.error("GET %s → %s: %s", path, resp.status, text)
                    return None
                data = await resp.json()
                errors = data.get("error", [])
                if errors:
                    logger.error("Kraken API error: %s", errors)
                    return None
                return data.get("result", {})
    except Exception as exc:
        logger.error("Request failed GET %s: %s", path, exc)
        return None


async def _private_post(path: str, extra_data: Optional[dict] = None) -> Optional[dict[str, Any]]:
    """Make an authenticated POST request to Kraken. Returns result dict or None."""
    _setup_logging()
    data: dict[str, Any] = {"nonce": str(int(time.time() * 1000))}
    if extra_data:
        data.update(extra_data)
    headers = _auth_headers(path, data)
    try:
        async with aiohttp.ClientSession() as session:
            async with session.post(
                BASE_URL + path, headers=headers, data=urllib.parse.urlencode(data)
            ) as resp:
                if resp.status != 200:
                    text = await resp.text()
                    logger.error("POST %s → %s: %s", path, resp.status, text)
                    return None
                result = await resp.json()
                errors = result.get("error", [])
                if errors:
                    logger.error("Kraken API error: %s", errors)
                    return None
                return result.get("result", {})
    except Exception as exc:
        msg = f"Request failed POST {path}: {exc}"
        logger.error(msg)
        try:
            await insert_system_log(MODULE_NAME, "ERROR", msg)
        except Exception:
            pass
        return None


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


async def get_ticker() -> Optional[float]:
    """Return the latest BTC/USD last trade price, or None on failure."""
    data = await _public_get("/0/public/Ticker", {"pair": PAIR})
    if data is None:
        return None
    try:
        # Kraken returns data keyed by pair name (XXBTZUSD or XBTUSD)
        pair_data = next(iter(data.values()))
        # "c" = last trade closed [price, lot volume]
        return float(pair_data["c"][0])
    except (KeyError, ValueError, TypeError, StopIteration) as exc:
        logger.error("Failed to parse ticker response: %s", exc)
        return None


async def get_ohlc(interval: int = 15) -> Optional[list[list]]:
    """Return recent OHLC candles for BTC/USD.

    Args:
        interval: Candle interval in minutes (1, 5, 15, 30, 60, 240, 1440, 10080, 21600).

    Returns:
        List of candles: [timestamp, open, high, low, close, vwap, volume, count]
        or None on failure.
    """
    data = await _public_get("/0/public/OHLC", {"pair": PAIR, "interval": interval})
    if data is None:
        return None
    try:
        # Kraken returns data keyed by pair name, plus "last" key
        for key, value in data.items():
            if key != "last" and isinstance(value, list):
                return value
        return None
    except (KeyError, TypeError) as exc:
        logger.error("Failed to parse OHLC response: %s", exc)
        return None


async def get_account_balance() -> Optional[dict[str, float]]:
    """Return account balances as {'USD': float, 'BTC': float}, or None."""
    data = await _private_post("/0/private/Balance")
    if data is None:
        return None
    try:
        balances: dict[str, float] = {}
        # Kraken uses ZUSD for USD, XXBT for BTC
        for key, value in data.items():
            if key in ("ZUSD", "USD"):
                balances["USD"] = float(value)
            elif key in ("XXBT", "XBT"):
                balances["BTC"] = float(value)
        return balances
    except (KeyError, ValueError, TypeError) as exc:
        logger.error("Failed to parse balance response: %s", exc)
        return None


async def place_order(
    direction: str,
    size: float,
    order_type: str = "limit",
    price: Optional[float] = None,
) -> Optional[dict[str, Any]]:
    """Place a BTC/USD order on Kraken.

    Args:
        direction: 'buy' or 'sell'.
        size: Order volume in BTC.
        order_type: 'limit' or 'market'.
        price: Required for limit orders.

    Returns:
        Order result dict or None on failure.
    """
    order_data: dict[str, Any] = {
        "pair": PAIR,
        "type": direction.lower(),
        "ordertype": order_type,
        "volume": str(size),
    }
    if order_type == "limit" and price is not None:
        order_data["price"] = str(price)

    return await _private_post("/0/private/AddOrder", order_data)
