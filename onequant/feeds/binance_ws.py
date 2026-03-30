"""Binance.US WebSocket — real-time BTC/USD kline (candle) feed.

Connects to the Binance.US WebSocket API and subscribes to 5m and 15m
kline streams for BTCUSD. Completed candles are written to the btc_candles
table.

Reconnects with exponential backoff: 5s, 10s, 20s, 40s, 60s.
"""

import asyncio
import json
import logging
import time
from pathlib import Path

import websockets

from database.db import insert_candle, insert_system_log

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

WS_BASE_URL: str = "wss://stream.binance.us:9443/ws"
STREAMS: list[str] = ["btcusd@kline_5m", "btcusd@kline_15m"]
MODULE_NAME: str = "binance_ws"

RECONNECT_DELAYS: list[float] = [5.0, 10.0, 20.0, 40.0, 60.0]

# Map Binance kline intervals to our timeframe labels
INTERVAL_MAP: dict[str, str] = {
    "5m": "5m",
    "15m": "15m",
}

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

    fh = logging.FileHandler("logs/binance_ws.log")
    fh.setLevel(logging.DEBUG)

    ch = logging.StreamHandler()
    ch.setLevel(logging.INFO)

    fmt = logging.Formatter("%(asctime)s [%(levelname)s] %(name)s — %(message)s")
    fh.setFormatter(fmt)
    ch.setFormatter(fmt)
    logger.addHandler(fh)
    logger.addHandler(ch)


# ---------------------------------------------------------------------------
# Kline processing
# ---------------------------------------------------------------------------


async def _process_kline(kline: dict) -> None:
    """Process a kline event and write completed candles to the database."""
    k = kline.get("k", {})
    if not k:
        return

    is_closed = k.get("x", False)
    if not is_closed:
        return  # Only persist completed candles

    interval = k.get("i", "")
    timeframe = INTERVAL_MAP.get(interval)
    if timeframe is None:
        return

    try:
        timestamp = int(k["t"]) // 1000  # Binance sends ms, we store seconds
        open_ = float(k["o"])
        high = float(k["h"])
        low = float(k["l"])
        close = float(k["c"])
        volume = float(k["v"])

        await insert_candle(
            timestamp=timestamp,
            timeframe=timeframe,
            open_=open_,
            high=high,
            low=low,
            close=close,
            volume=volume,
            symbol="BTCUSD",
        )
        logger.info(
            "Candle closed  %s  ts=%d  O=%.2f H=%.2f L=%.2f C=%.2f V=%.4f",
            timeframe, timestamp, open_, high, low, close, volume,
        )
    except (KeyError, ValueError, TypeError) as exc:
        logger.warning("Malformed kline data: %s", exc)


# ---------------------------------------------------------------------------
# Main loop
# ---------------------------------------------------------------------------


async def run_binance_ws() -> None:
    """Connect to Binance.US WebSocket and stream BTCUSD klines forever.

    Subscribes to both 5m and 15m kline streams simultaneously.
    Auto-reconnects with exponential backoff on disconnection.
    Never raises — all exceptions are caught and logged.
    """
    _setup_logging()
    reconnect_idx = 0

    # Build combined stream URL
    streams = "/".join(STREAMS)
    ws_url = f"{WS_BASE_URL}/{streams}"

    while True:
        try:
            async with websockets.connect(
                ws_url,
                ping_interval=30,
                open_timeout=30,
            ) as ws:
                logger.info(
                    "Connected to Binance.US WS — subscribed to %s",
                    ", ".join(STREAMS),
                )
                print(f"Binance.US WS connected: {', '.join(STREAMS)}")
                reconnect_idx = 0  # reset on successful connect

                async for raw in ws:
                    try:
                        msg = json.loads(raw)

                        # Combined stream format wraps data in {"stream": ..., "data": ...}
                        if "data" in msg:
                            data = msg["data"]
                        else:
                            data = msg

                        event_type = data.get("e", "")
                        if event_type == "kline":
                            await _process_kline(data)
                    except (KeyError, ValueError, TypeError) as exc:
                        logger.warning("Malformed message: %s", exc)

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

            delay = RECONNECT_DELAYS[min(reconnect_idx, len(RECONNECT_DELAYS) - 1)]
            logger.info("Reconnecting in %.1fs …", delay)
            print(f"Binance.US WS disconnected — reconnecting in {delay:.0f}s")
            await asyncio.sleep(delay)
            reconnect_idx += 1
