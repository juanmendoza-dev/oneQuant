import asyncio
import time
import logging
import os
import hashlib
import hmac
import base64
from pathlib import Path
from datetime import datetime, timezone
import aiohttp
from config import KALSHI_API_KEY, KALSHI_API_SECRET
from database.db import insert_kalshi_market

Path("logs").mkdir(exist_ok=True)
logger = logging.getLogger("kalshi")
logger.setLevel(logging.INFO)
_handler = logging.FileHandler(os.path.join("logs", "kalshi.log"))
_handler.setFormatter(logging.Formatter("%(asctime)s %(levelname)s %(message)s"))
logger.addHandler(_handler)

BASE_URL = "https://api.elections.kalshi.com/trade-api/v2"
SPREAD_WARN_THRESHOLD = 0.08  # 8 cents


def _sign_request(method: str, path: str, timestamp_ms: int) -> str:
    """Create HMAC signature for Kalshi API v2."""
    message = f"{timestamp_ms}{method}{path}"
    secret_bytes = base64.b64decode(KALSHI_API_SECRET)
    signature = hmac.new(secret_bytes, message.encode(), hashlib.sha256).hexdigest()
    return signature


def _get_auth_headers(method: str, path: str) -> dict:
    timestamp_ms = int(time.time() * 1000)
    signature = _sign_request(method, path, timestamp_ms)
    return {
        "KALSHI-ACCESS-KEY": KALSHI_API_KEY,
        "KALSHI-ACCESS-TIMESTAMP": str(timestamp_ms),
        "KALSHI-ACCESS-SIGNATURE": signature,
        "Content-Type": "application/json",
    }


async def poll_once(session: aiohttp.ClientSession):
    path = "/markets"
    params = {
        "status": "open",
        "series_ticker": "KXBTC",
        "limit": 100,
    }
    headers = _get_auth_headers("GET", path)
    url = f"{BASE_URL}{path}"
    now = int(time.time())

    async with session.get(url, headers=headers, params=params) as resp:
        resp.raise_for_status()
        data = await resp.json()

    markets = data.get("markets", [])
    count = 0
    for m in markets:
        ticker = m.get("ticker", "")
        title = m.get("title", "")
        # Filter for BTC direction markets
        if not any(kw in title.lower() for kw in ["bitcoin", "btc", "crypto"]):
            if not ticker.startswith("KXBTC"):
                continue

        yes_price = m.get("yes_ask", 0) / 100.0 if m.get("yes_ask") else 0
        no_price = m.get("no_ask", 0) / 100.0 if m.get("no_ask") else 0
        spread = abs(yes_price - (1 - no_price))
        volume = m.get("volume", 0)
        expiry = m.get("expiration_time", "")

        if spread > SPREAD_WARN_THRESHOLD:
            logger.warning("Wide spread on %s: $%.2f", ticker, spread)

        insert_kalshi_market(
            now, ticker, title, yes_price, no_price, spread, volume, expiry
        )
        count += 1

    logger.info("Polled %d BTC markets from Kalshi", count)


async def run():
    async with aiohttp.ClientSession() as session:
        while True:
            try:
                await poll_once(session)
            except Exception as e:
                logger.error("Kalshi poll error: %s", e)
            await asyncio.sleep(60)
