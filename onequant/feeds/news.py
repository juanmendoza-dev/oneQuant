"""Crypto news headline poller and Fear & Greed Index poller.

Two independent async loops, each running on a 15-minute interval:
  - free-crypto-news (cryptocurrency.cv): fetches BTC-related headlines
  - Fear & Greed Index: fetches the current score and label
"""

import asyncio
import logging
import time
from datetime import datetime, timezone
from pathlib import Path

import aiohttp

from config import config
from database.db import insert_fear_greed, insert_news, insert_system_log, news_url_exists

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

MODULE_NAME: str = "news"
POLL_INTERVAL: int = 900  # 15 minutes in seconds
ERROR_RETRY_DELAY: int = 30

CRYPTO_NEWS_URL: str = "https://cryptocurrency.cv/api/news"
FEAR_GREED_URL: str = "https://api.alternative.me/fng/"

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------

logger: logging.Logger = logging.getLogger(MODULE_NAME)


def _setup_logging() -> None:
    """Configure file and console logging for the news module."""
    if logger.handlers:
        return
    logger.setLevel(logging.DEBUG)
    Path("logs").mkdir(exist_ok=True)

    fh = logging.FileHandler("logs/news.log")
    fh.setLevel(logging.DEBUG)

    ch = logging.StreamHandler()
    ch.setLevel(getattr(logging, config.LOG_LEVEL, logging.INFO))

    fmt = logging.Formatter("%(asctime)s [%(levelname)s] %(name)s — %(message)s")
    fh.setFormatter(fmt)
    ch.setFormatter(fmt)
    logger.addHandler(fh)
    logger.addHandler(ch)


# ---------------------------------------------------------------------------
# Crypto news poller (cryptocurrency.cv)
# ---------------------------------------------------------------------------

NEWS_FETCH_LIMIT: int = 20


async def _poll_crypto_news() -> None:
    """Fetch latest BTC headlines from cryptocurrency.cv and store new ones."""
    params = {
        "ticker": "BTC",
        "limit": str(NEWS_FETCH_LIMIT),
    }
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(CRYPTO_NEWS_URL, params=params) as resp:
                if resp.status != 200:
                    text = await resp.text()
                    logger.error("crypto-news API %s: %s", resp.status, text)
                    return
                data = await resp.json()
    except Exception as exc:
        msg = f"crypto-news request failed: {exc}"
        logger.error(msg)
        await insert_system_log(MODULE_NAME, "ERROR", msg)
        return

    articles = data.get("articles", [])
    inserted = 0
    for article in articles:
        link = article.get("link", "")
        if not link:
            continue
        if await news_url_exists(link):
            continue

        headline = article.get("title", "")
        pub_ts = int(time.time())
        if article.get("pubDate"):
            try:
                dt = datetime.fromisoformat(
                    article["pubDate"].replace("Z", "+00:00")
                )
                pub_ts = int(dt.timestamp())
            except (ValueError, TypeError):
                pass

        sentiment = "neutral"
        currencies = "BTC"

        await insert_news(
            pub_ts, "cryptocurrency.cv", headline, link, sentiment, currencies
        )
        inserted += 1

    if inserted:
        logger.info("crypto-news: inserted %d new headlines", inserted)


# ---------------------------------------------------------------------------
# Fear & Greed poller
# ---------------------------------------------------------------------------


async def _poll_fear_greed() -> None:
    """Fetch the latest Fear & Greed Index value and store it."""
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(FEAR_GREED_URL) as resp:
                if resp.status != 200:
                    text = await resp.text()
                    logger.error("Fear & Greed API %s: %s", resp.status, text)
                    return
                data = await resp.json(content_type=None)
    except Exception as exc:
        msg = f"Fear & Greed request failed: {exc}"
        logger.error(msg)
        await insert_system_log(MODULE_NAME, "ERROR", msg)
        return

    try:
        entry = data["data"][0]
        score = int(entry["value"])
        label = entry["value_classification"]
        ts = int(entry["timestamp"])
        await insert_fear_greed(ts, score, label)
        logger.info("Fear & Greed: score=%d label=%s", score, label)
    except (KeyError, IndexError, ValueError, TypeError) as exc:
        msg = f"Failed to parse Fear & Greed response: {exc}"
        logger.error(msg)
        await insert_system_log(MODULE_NAME, "ERROR", msg)


# ---------------------------------------------------------------------------
# Public entry points
# ---------------------------------------------------------------------------


async def run_news_poller() -> None:
    """Poll cryptocurrency.cv every 15 minutes. Never raises."""
    _setup_logging()
    logger.info("crypto-news poller started (interval=%ds)", POLL_INTERVAL)
    while True:
        try:
            await _poll_crypto_news()
        except asyncio.CancelledError:
            logger.info("crypto-news poller cancelled")
            return
        except Exception as exc:
            msg = f"crypto-news poller error: {exc}"
            logger.error(msg)
            try:
                await insert_system_log(MODULE_NAME, "ERROR", msg)
            except Exception:
                pass
            await asyncio.sleep(ERROR_RETRY_DELAY)
            continue
        await asyncio.sleep(POLL_INTERVAL)


async def run_fear_greed_poller() -> None:
    """Poll Fear & Greed Index every 15 minutes. Never raises."""
    _setup_logging()
    logger.info("Fear & Greed poller started (interval=%ds)", POLL_INTERVAL)
    while True:
        try:
            await _poll_fear_greed()
        except asyncio.CancelledError:
            logger.info("Fear & Greed poller cancelled")
            return
        except Exception as exc:
            msg = f"Fear & Greed poller error: {exc}"
            logger.error(msg)
            try:
                await insert_system_log(MODULE_NAME, "ERROR", msg)
            except Exception:
                pass
            await asyncio.sleep(ERROR_RETRY_DELAY)
            continue
        await asyncio.sleep(POLL_INTERVAL)
