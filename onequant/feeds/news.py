import asyncio
import time
import logging
import os
from pathlib import Path
import aiohttp
from config import CRYPTOPANIC_API_KEY
from database.db import insert_news, insert_fear_greed

Path("logs").mkdir(exist_ok=True)
logger = logging.getLogger("news")
logger.setLevel(logging.INFO)
_handler = logging.FileHandler(os.path.join("logs", "news.log"))
_handler.setFormatter(logging.Formatter("%(asctime)s %(levelname)s %(message)s"))
logger.addHandler(_handler)

CRYPTOPANIC_URL = "https://cryptopanic.com/api/free/v1/posts/"
FEAR_GREED_URL = "https://api.alternative.me/fng/"


async def poll_cryptopanic(session: aiohttp.ClientSession):
    params = {
        "auth_token": CRYPTOPANIC_API_KEY,
        "currencies": "BTC",
        "filter": "important",
    }
    now = int(time.time())

    async with session.get(CRYPTOPANIC_URL, params=params) as resp:
        resp.raise_for_status()
        data = await resp.json()

    results = data.get("results", [])
    count = 0
    for post in results:
        headline = post.get("title", "")
        url = post.get("url", "")
        source = post.get("source", {}).get("title", "unknown")
        currencies = ",".join(
            c.get("code", "") for c in post.get("currencies", [])
        )
        # CryptoPanic free tier doesn't provide sentiment scores
        sentiment = None
        votes = post.get("votes", {})
        if votes:
            pos = votes.get("positive", 0)
            neg = votes.get("negative", 0)
            total = pos + neg
            if total > 0:
                sentiment = round((pos - neg) / total, 2)

        insert_news(now, source, headline, url, sentiment, currencies)
        count += 1

    logger.info("Fetched %d headlines from CryptoPanic", count)


async def poll_fear_greed(session: aiohttp.ClientSession):
    async with session.get(FEAR_GREED_URL) as resp:
        resp.raise_for_status()
        data = await resp.json(content_type=None)

    for entry in data.get("data", []):
        timestamp = int(entry.get("timestamp", 0))
        score = int(entry.get("value", 0))
        label = entry.get("value_classification", "Unknown")
        insert_fear_greed(timestamp, score, label)

    logger.info("Updated Fear & Greed index")


async def run():
    async with aiohttp.ClientSession() as session:
        while True:
            try:
                await poll_cryptopanic(session)
            except Exception as e:
                logger.error("CryptoPanic error: %s", e)

            try:
                await poll_fear_greed(session)
            except Exception as e:
                logger.error("Fear & Greed error: %s", e)

            await asyncio.sleep(900)  # 15 minutes
