"""Coinbase Advanced Trade WebSocket — real-time BTC-USD price feed.

Connects to the Coinbase Advanced Trade WebSocket API, subscribes to the
BTC-USD ticker channel, and aggregates incoming ticks into 5m, 15m, and 1h
OHLCV candles. Completed candles are written to the btc_candles table.

Volume note: the ticker channel provides a rolling 24h volume snapshot, not
per-trade volume. Real-time candle volume is tracked as the delta of
volume_24_h between ticks. For accurate per-candle volume, use the
historical fetcher.
"""

import asyncio
import hashlib
import hmac
import json
import logging
import time
from pathlib import Path
from typing import Any

import websockets

from config import config
from database.db import insert_candle, insert_system_log

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

WS_URL: str = "wss://advanced-trade-ws.coinbase.com"
PRODUCT_ID: str = "BTC-USD"
CHANNEL: str = "ticker"
MODULE_NAME: str = "coinbase_ws"

TIMEFRAMES: dict[str, int] = {
    "5m": 300,
    "15m": 900,
    "1h": 3600,
}

RECONNECT_BASE_DELAY: float = 1.0
RECONNECT_MAX_DELAY: float = 60.0
ERROR_RETRY_DELAY: float = 30.0

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------

logger: logging.Logger = logging.getLogger(MODULE_NAME)


def _setup_logging() -> None:
    """Configure file and console logging for the WebSocket feed."""
    if logger.handlers:
        return
    logger.setLevel(logging.DEBUG)
    Path("logs").mkdir(exist_ok=True)

    fh = logging.FileHandler("logs/coinbase_ws.log")
    fh.setLevel(logging.DEBUG)

    ch = logging.StreamHandler()
    ch.setLevel(getattr(logging, config.LOG_LEVEL, logging.INFO))

    fmt = logging.Formatter("%(asctime)s [%(levelname)s] %(name)s — %(message)s")
    fh.setFormatter(fmt)
    ch.setFormatter(fmt)
    logger.addHandler(fh)
    logger.addHandler(ch)


# ---------------------------------------------------------------------------
# Authentication
# ---------------------------------------------------------------------------


def _sign_subscribe(
    api_key: str, api_secret: str, channel: str, product_ids: list[str]
) -> dict[str, Any]:
    """Build an authenticated subscribe message for Coinbase Advanced Trade WS."""
    timestamp = str(int(time.time()))
    message = timestamp + channel + ",".join(product_ids)
    signature = hmac.new(
        api_secret.encode("utf-8"),
        message.encode("utf-8"),
        hashlib.sha256,
    ).hexdigest()
    return {
        "type": "subscribe",
        "product_ids": product_ids,
        "channel": channel,
        "api_key": api_key,
        "timestamp": timestamp,
        "signature": signature,
    }


# ---------------------------------------------------------------------------
# Candle builder
# ---------------------------------------------------------------------------


class CandleBuilder:
    """Aggregates price ticks into running OHLCV candles for multiple timeframes."""

    def __init__(self) -> None:
        self._candles: dict[str, dict[str, Any]] = {}
        self._last_volume_24h: float = 0.0

    @staticmethod
    def _candle_start(ts: int, interval: int) -> int:
        """Align a unix timestamp to the start of its candle interval."""
        return ts - (ts % interval)

    async def on_tick(self, price: float, volume_24h: float, ts: float) -> None:
        """Process a single price tick from the ticker channel."""
        tick_ts = int(ts)
        volume_delta = max(0.0, volume_24h - self._last_volume_24h)
        self._last_volume_24h = volume_24h

        for tf, interval in TIMEFRAMES.items():
            start = self._candle_start(tick_ts, interval)

            if tf not in self._candles:
                self._candles[tf] = {
                    "timestamp": start,
                    "open": price,
                    "high": price,
                    "low": price,
                    "close": price,
                    "volume": volume_delta,
                }
                continue

            current = self._candles[tf]

            if start > current["timestamp"]:
                # Current candle is complete — persist it
                await insert_candle(
                    current["timestamp"],
                    tf,
                    current["open"],
                    current["high"],
                    current["low"],
                    current["close"],
                    current["volume"],
                )
                logger.info(
                    "Candle closed  %s  %s  O=%.2f H=%.2f L=%.2f C=%.2f V=%.4f",
                    tf,
                    current["timestamp"],
                    current["open"],
                    current["high"],
                    current["low"],
                    current["close"],
                    current["volume"],
                )
                # Start a new candle
                self._candles[tf] = {
                    "timestamp": start,
                    "open": price,
                    "high": price,
                    "low": price,
                    "close": price,
                    "volume": volume_delta,
                }
            else:
                current["high"] = max(current["high"], price)
                current["low"] = min(current["low"], price)
                current["close"] = price
                current["volume"] += volume_delta


# ---------------------------------------------------------------------------
# Main loop
# ---------------------------------------------------------------------------


async def run_coinbase_ws() -> None:
    """Connect to Coinbase WebSocket and stream BTC-USD ticks forever.

    Auto-reconnects with exponential backoff on disconnection.
    Never raises — all exceptions are caught and logged.
    """
    _setup_logging()
    builder = CandleBuilder()
    delay = RECONNECT_BASE_DELAY

    while True:
        try:
            async with websockets.connect(WS_URL, ping_interval=30) as ws:
                subscribe_msg = _sign_subscribe(
                    config.COINBASE_API_KEY,
                    config.COINBASE_API_SECRET,
                    CHANNEL,
                    [PRODUCT_ID],
                )
                await ws.send(json.dumps(subscribe_msg))
                logger.info("Connected to Coinbase WS — subscribed to %s %s", PRODUCT_ID, CHANNEL)
                delay = RECONNECT_BASE_DELAY  # reset on successful connect

                async for raw in ws:
                    try:
                        msg = json.loads(raw)
                        events = msg.get("events", [])
                        for event in events:
                            tickers = event.get("tickers", [])
                            for ticker in tickers:
                                price = float(ticker["price"])
                                vol_24h = float(ticker.get("volume_24_h", 0))
                                await builder.on_tick(price, vol_24h, time.time())
                    except (KeyError, ValueError, TypeError) as exc:
                        logger.warning("Malformed tick: %s", exc)

        except asyncio.CancelledError:
            logger.info("WebSocket task cancelled — shutting down")
            return
        except Exception as exc:
            msg_text = f"WebSocket error: {exc}"
            logger.error(msg_text)
            try:
                await insert_system_log(MODULE_NAME, "ERROR", msg_text)
            except Exception:
                pass
            logger.info("Reconnecting in %.1fs …", delay)
            await asyncio.sleep(delay)
            delay = min(delay * 2, RECONNECT_MAX_DELAY)
