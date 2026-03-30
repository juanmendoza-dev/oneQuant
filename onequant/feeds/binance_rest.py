"""Binance.US REST API helpers.

Provides authenticated access to the Binance.US API for:
- Current BTC/USD price
- Recent OHLCV candles
- Account balance
- LIMIT order placement (NEVER market orders)
- Order cancellation
- Fee verification (must be $0.00 for maker orders)

CRITICAL: place_limit_order will NEVER place market orders.
"""

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

BASE_URL: str = "https://api.binance.us"
SYMBOL: str = "BTCUSD"
MODULE_NAME: str = "binance_rest"

# ---------------------------------------------------------------------------
# Exceptions
# ---------------------------------------------------------------------------


class FeeVerificationError(Exception):
    """Raised when a maker order is charged a non-zero fee."""
    pass


class MarketOrderError(Exception):
    """Raised when someone attempts to place a market order."""
    pass


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

    fh = logging.FileHandler("logs/binance_rest.log")
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


def _sign_params(params: dict[str, Any]) -> dict[str, Any]:
    """Add timestamp and HMAC-SHA256 signature to request parameters."""
    params["timestamp"] = int(time.time() * 1000)
    query_string = urllib.parse.urlencode(params)
    signature = hmac.new(
        config.BINANCE_API_SECRET.encode("utf-8"),
        query_string.encode("utf-8"),
        hashlib.sha256,
    ).hexdigest()
    params["signature"] = signature
    return params


def _headers() -> dict[str, str]:
    """Return standard authenticated headers."""
    return {
        "X-MBX-APIKEY": config.BINANCE_API_KEY,
        "Content-Type": "application/json",
    }


async def _get(path: str, params: Optional[dict] = None, signed: bool = False) -> Optional[Any]:
    """Make a GET request. Returns JSON or None on error."""
    _setup_logging()
    if params is None:
        params = {}
    if signed:
        params = _sign_params(params)

    url = BASE_URL + path
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(url, params=params, headers=_headers()) as resp:
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


async def _post(path: str, params: dict) -> Optional[Any]:
    """Make a signed POST request. Returns JSON or None on error."""
    _setup_logging()
    params = _sign_params(params)
    url = BASE_URL + path
    try:
        async with aiohttp.ClientSession() as session:
            async with session.post(url, params=params, headers=_headers()) as resp:
                if resp.status != 200:
                    text = await resp.text()
                    logger.error("POST %s → %s: %s", path, resp.status, text)
                    return None
                return await resp.json()
    except Exception as exc:
        msg = f"Request failed POST {path}: {exc}"
        logger.error(msg)
        try:
            await insert_system_log(MODULE_NAME, "ERROR", msg)
        except Exception:
            pass
        return None


async def _delete(path: str, params: dict) -> Optional[Any]:
    """Make a signed DELETE request. Returns JSON or None on error."""
    _setup_logging()
    params = _sign_params(params)
    url = BASE_URL + path
    try:
        async with aiohttp.ClientSession() as session:
            async with session.delete(url, params=params, headers=_headers()) as resp:
                if resp.status != 200:
                    text = await resp.text()
                    logger.error("DELETE %s → %s: %s", path, resp.status, text)
                    return None
                return await resp.json()
    except Exception as exc:
        msg = f"Request failed DELETE {path}: {exc}"
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
    """Return the latest BTCUSD price, or None on failure."""
    data = await _get("/api/v3/ticker/price", {"symbol": SYMBOL})
    if data is None:
        return None
    try:
        return float(data["price"])
    except (KeyError, ValueError, TypeError) as exc:
        logger.error("Failed to parse ticker response: %s", exc)
        return None


async def get_ohlc(interval: str = "5m", limit: int = 500) -> Optional[list[list]]:
    """Return recent BTCUSD candles as raw kline arrays.

    Each kline: [open_time, open, high, low, close, volume, close_time, ...]
    """
    data = await _get("/api/v3/klines", {
        "symbol": SYMBOL,
        "interval": interval,
        "limit": limit,
    })
    return data


async def get_account_balance() -> Optional[dict[str, float]]:
    """Return USDT and USD balances as {'USD': float, 'BTC': float}, or None."""
    data = await _get("/api/v3/account", signed=True)
    if data is None:
        return None
    try:
        balances: dict[str, float] = {}
        for asset in data.get("balances", []):
            symbol = asset.get("asset", "")
            if symbol in ("USD", "BTC", "USDT"):
                balances[symbol] = float(asset.get("free", 0))
        return balances
    except (KeyError, ValueError, TypeError) as exc:
        logger.error("Failed to parse account response: %s", exc)
        return None


async def place_limit_order(
    side: str,
    quantity: float,
    price: float,
) -> Optional[dict]:
    """Place a LIMIT order on Binance.US. NEVER places market orders.

    Args:
        side: 'BUY' or 'SELL'
        quantity: Amount of BTC
        price: Limit price in USD

    Returns:
        Order response dict, or None on failure.

    Raises:
        MarketOrderError: If called with intent to place market order.
    """
    params = {
        "symbol": SYMBOL,
        "side": side.upper(),
        "type": "LIMIT",
        "timeInForce": "GTC",
        "quantity": f"{quantity:.8f}",
        "price": f"{price:.2f}",
    }

    # CRITICAL SAFETY CHECK: Never allow market orders
    if params["type"] != "LIMIT":
        msg = "CRITICAL: Attempted to place non-LIMIT order — BLOCKED"
        logger.critical(msg)
        raise MarketOrderError(msg)

    logger.info(
        "Placing LIMIT %s order: %.8f BTC @ $%.2f",
        side.upper(), quantity, price,
    )
    return await _post("/api/v3/order", params)


async def cancel_order(order_id: int) -> Optional[dict]:
    """Cancel an open order by orderId."""
    return await _delete("/api/v3/order", {
        "symbol": SYMBOL,
        "orderId": order_id,
    })


async def get_open_orders() -> Optional[list[dict]]:
    """List all open orders for BTCUSD."""
    return await _get("/api/v3/openOrders", {"symbol": SYMBOL}, signed=True)


async def verify_fee(order_id: int) -> dict:
    """Check actual fee charged for a completed order.

    Returns fee details dict. Raises FeeVerificationError if a maker
    order was charged a non-zero fee.
    """
    data = await _get("/api/v3/myTrades", {
        "symbol": SYMBOL,
        "orderId": order_id,
    }, signed=True)

    if data is None:
        raise FeeVerificationError(f"Could not fetch trades for order {order_id}")

    total_fee = 0.0
    for trade in data:
        fee = float(trade.get("commission", 0))
        total_fee += fee
        is_maker = trade.get("isMaker", False)

        if is_maker and fee > 0:
            msg = (
                f"FEE ALERT: Maker order {order_id} charged fee "
                f"${fee:.6f} (expected $0.00)"
            )
            logger.critical(msg)
            raise FeeVerificationError(msg)

    logger.info("Order %d fee verified: $%.6f", order_id, total_fee)
    return {
        "order_id": order_id,
        "total_fee": total_fee,
        "trade_count": len(data),
        "verified_zero_maker": total_fee == 0.0,
    }
