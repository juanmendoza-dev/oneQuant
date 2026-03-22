"""CryptoPanic headline poller and Fear & Greed Index poller.

Two independent async loops, each running on a 15-minute interval:
  - CryptoPanic: fetches BTC-related news headlines with sentiment
  - Fear & Greed Index: fetches the current score and label
"""

import asyncio
import logging
import time
from pathlib import Path
from typing import Any

import aiohttp

from config import config
from database.db import insert_fear_greed, insert_news, insert_system_log, news_url_exists

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

MODULE_NAME: str = "news"
POLL_INTERVAL: int = 900  # 15 minutes in seconds
ERROR_RETRY_DELAY: int = 30

CRYPTOPANIC_URL: str = "https://cryptopanic.com/api/v1/posts/"
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
# Sentiment helper
# ---------------------------------------------------------------------------

VOTE_SENTIMENT_MAP: dict[str, str] = {
    "positive": "positive",
    "negative": "negative",
    "important": "positive",
    "liked": "positive",
    "disliked": "negative",
    "lol": "neutral",
    "toxic": "negative",
    "saved": "neutral",
}


def _derive_sentiment(votes: dict[str, Any]) -> str:
    """Derive overall sentiment from CryptoPanic vote data."""
    if not votes:
        return "neutral"
    pos = sum(int(votes.get(k, 0)) for k in ("positive", "important", "liked"))
    neg = sum(int(votes.get(k, 0)) for k in ("negative", "disliked", "toxic"))
    if pos > neg:
        return "positive"
    if neg > pos:
        return "negative"
    return "neutral"


# ---------------------------------------------------------------------------
# CryptoPanic poller
# ---------------------------------------------------------------------------


async def _poll_cryptopanic() -> None:
    """Fetch latest BTC headlines from CryptoPanic and store new ones."""
    params = {
        "auth_token": config.CRYPTOPANIC_API_KEY,
        "currencies": "BTC",
        "kind": "news",
    }
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(CRYPTOPANIC_URL, params=params) as resp:
                if resp.status != 200:
                    text = await resp.text()
                    logger.error("CryptoPanic API %s: %s", resp.status, text)
                    return
                data = await resp.json()
    except Exception as exc:
        msg = f"CryptoPanic request failed: {exc}"
        logger.error(msg)
        await insert_system_log(MODULE_NAME, "ERROR", msg)
        return

    results = data.get("results", [])
    inserted = 0
    for post in results:
        url = post.get("url", "")
        if not url:
            continue
        if await news_url_exists(url):
            continue

        headline = post.get("title", "")
        pub_ts = int(time.time())  # use current time as fallback
        if post.get("published_at"):
            try:
                from datetime import datetime, timezone

                dt = datetime.fromisoformat(post["published_at"].replace("Z", "+00:00"))
                pub_ts = int(dt.timestamp())
            except (ValueError, TypeError):
                pass

        votes = post.get("votes", {})
        sentiment = _derive_sentiment(votes)

        currencies_list = [c.get("code", "") for c in post.get("currencies", [])]
        currencies = ",".join(filter(None, currencies_list)) or "BTC"

        await insert_news(pub_ts, "cryptopanic", headline, url, sentiment, currencies)
        inserted += 1

    if inserted:
        logger.info("CryptoPanic: inserted %d new headlines", inserted)


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


async def run_cryptopanic_poller() -> None:
    """Poll CryptoPanic every 15 minutes. Never raises."""
    _setup_logging()
    logger.info("CryptoPanic poller started (interval=%ds)", POLL_INTERVAL)
    while True:
        try:
            await _poll_cryptopanic()
        except asyncio.CancelledError:
            logger.info("CryptoPanic poller cancelled")
            return
        except Exception as exc:
            msg = f"CryptoPanic poller error: {exc}"
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
