import asyncio
import json
import time
import logging
import os
from pathlib import Path
import websockets
from database.db import insert_candle

Path("logs").mkdir(exist_ok=True)
logger = logging.getLogger("coinbase")
logger.setLevel(logging.INFO)
_handler = logging.FileHandler(os.path.join("logs", "coinbase.log"))
_handler.setFormatter(logging.Formatter("%(asctime)s %(levelname)s %(message)s"))
logger.addHandler(_handler)

WS_URL = "wss://advanced-trade-ws.coinbase.com"

TIMEFRAMES = {
    "5m": 300,
    "15m": 900,
    "1h": 3600,
}


class CandleBuilder:
    def __init__(self):
        self.candles = {}
        for tf, seconds in TIMEFRAMES.items():
            self.candles[tf] = {
                "seconds": seconds,
                "open": None,
                "high": None,
                "low": None,
                "close": None,
                "volume": 0.0,
                "start": None,
            }

    def _bucket_start(self, ts: float, seconds: int) -> int:
        return int(ts // seconds) * seconds

    def update(self, price: float, volume: float, ts: float):
        for tf, candle in self.candles.items():
            seconds = candle["seconds"]
            bucket = self._bucket_start(ts, seconds)

            if candle["start"] is None:
                candle["start"] = bucket
                candle["open"] = price
                candle["high"] = price
                candle["low"] = price
                candle["close"] = price
                candle["volume"] = volume
            elif bucket > candle["start"]:
                # candle closed — write it
                insert_candle(
                    candle["start"],
                    candle["open"],
                    candle["high"],
                    candle["low"],
                    candle["close"],
                    candle["volume"],
                    tf,
                )
                logger.info(
                    "Candle closed: %s @ %d  O=%.2f H=%.2f L=%.2f C=%.2f V=%.4f",
                    tf, candle["start"],
                    candle["open"], candle["high"],
                    candle["low"], candle["close"], candle["volume"],
                )
                # start new candle
                candle["start"] = bucket
                candle["open"] = price
                candle["high"] = price
                candle["low"] = price
                candle["close"] = price
                candle["volume"] = volume
            else:
                candle["high"] = max(candle["high"], price)
                candle["low"] = min(candle["low"], price)
                candle["close"] = price
                candle["volume"] += volume


async def run():
    builder = CandleBuilder()
    while True:
        try:
            async with websockets.connect(WS_URL) as ws:
                subscribe_msg = json.dumps({
                    "type": "subscribe",
                    "product_ids": ["BTC-USD"],
                    "channel": "ticker",
                })
                await ws.send(subscribe_msg)
                logger.info("Connected to Coinbase WebSocket")

                async for raw in ws:
                    try:
                        msg = json.loads(raw)
                        if msg.get("channel") != "ticker":
                            continue
                        for event in msg.get("events", []):
                            for ticker in event.get("tickers", []):
                                price = float(ticker["price"])
                                volume = float(ticker.get("volume_24_h", 0))
                                ts = time.time()
                                builder.update(price, volume, ts)
                    except (KeyError, ValueError, TypeError) as e:
                        logger.error("Tick parse error: %s", e)
        except Exception as e:
            logger.error("WebSocket error: %s", e)
            await asyncio.sleep(30)
