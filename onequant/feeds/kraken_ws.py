"""Kraken WebSocket v2 — real-time BTC/USD price feed.

Connects to the Kraken WebSocket v2 API, subscribes to the ticker
channel for BTC/USD, and aggregates incoming ticks into 5m, 15m, and 1h
OHLCV candles. Completed candles are written to the btc_candles table.
"""

import asyncio
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

WS_URL: str = "wss://ws.kraken.com/v2"
PAIR: str = "BTC/USD"
CHANNEL: str = "ticker"
MODULE_NAME: str = "kraken_ws"

TIMEFRAMES: dict[str, int] = {
    "5m": 300,
    "15m": 900,
    "1h": 3600,
}

RECONNECT_BASE_DELAY: float = 5.0
RECONNECT_MAX_DELAY: float = 60.0

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

    fh = logging.FileHandler("logs/kraken_ws.log")
    fh.setLevel(logging.DEBUG)

    ch = logging.StreamHandler()
    ch.setLevel(getattr(logging, config.LOG_LEVEL, logging.INFO))

    fmt = logging.Formatter("%(asctime)s [%(levelname)s] %(name)s — %(message)s")
    fh.setFormatter(fmt)
    ch.setFormatter(fmt)
    logger.addHandler(fh)
    logger.addHandler(ch)


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


async def run_kraken_ws() -> None:
    """Connect to Kraken WebSocket v2 and stream BTC/USD ticks forever.

    Auto-reconnects with exponential backoff on disconnection.
    Never raises — all exceptions are caught and logged.
    """
    _setup_logging()
    builder = CandleBuilder()
    delay = RECONNECT_BASE_DELAY

    while True:
        try:
            logger.info("Connecting to Kraken WS v2 (delay was %.1fs)", delay)
            async with websockets.connect(
                WS_URL, ping_interval=30, open_timeout=30,
            ) as ws:
                # Kraken v2 subscribe message
                subscribe_msg = {
                    "method": "subscribe",
                    "params": {
                        "channel": CHANNEL,
                        "symbol": [PAIR],
                    },
                }
                await ws.send(json.dumps(subscribe_msg))
                logger.info("Connected to Kraken WS — subscribed to %s %s", PAIR, CHANNEL)
                delay = RECONNECT_BASE_DELAY  # reset on successful connect

                async for raw in ws:
                    try:
                        msg = json.loads(raw)

                        # Kraken v2 sends different message types
                        channel = msg.get("channel")
                        msg_type = msg.get("type")

                        # Handle heartbeat
                        if channel == "heartbeat":
                            continue

                        # Handle status/subscription confirmations
                        if msg_type in ("update", "snapshot") and channel == "ticker":
                            data_list = msg.get("data", [])
                            for ticker in data_list:
                                # Kraken v2 ticker fields
                                last_price = float(ticker.get("last", 0))
                                vol_24h = float(ticker.get("volume", 0))
                                if last_price > 0:
                                    await builder.on_tick(last_price, vol_24h, time.time())

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
