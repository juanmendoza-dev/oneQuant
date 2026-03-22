"""Strategy C — News Driven.

Combines recent news headline sentiment with the Fear & Greed Index to
generate directional signals. This strategy queries the database directly
for news and sentiment data.
"""

import sqlite3

from config import config
from strategies.base import BaseStrategy, Signal

REQUIRED_CANDLES: int = 3
NEWS_WINDOW_SECONDS: int = 1800  # 30 minutes
POSITIVE_HEADLINE_THRESHOLD: int = 2
NEGATIVE_HEADLINE_THRESHOLD: int = 2
FG_BULLISH_THRESHOLD: int = 60
FG_BEARISH_THRESHOLD: int = 40
FG_EXTREME_FEAR_THRESHOLD: int = 20
SIGNAL_CONFIDENCE: float = 0.70


class NewsDrivenStrategy(BaseStrategy):
    """Trade based on recent news sentiment combined with Fear & Greed Index."""

    name: str = "News Driven"
    timeframe: str = "15m"
    required_candles: int = REQUIRED_CANDLES

    def _query_news(self, since_ts: int) -> dict[str, int]:
        """Count positive and negative headlines since the given timestamp."""
        conn = sqlite3.connect(config.DATABASE_PATH)
        try:
            cursor = conn.execute(
                "SELECT sentiment, COUNT(*) FROM news_feed "
                "WHERE timestamp >= ? GROUP BY sentiment",
                (since_ts,),
            )
            counts: dict[str, int] = {"positive": 0, "negative": 0, "neutral": 0}
            for sentiment, count in cursor.fetchall():
                if sentiment in counts:
                    counts[sentiment] = count
            return counts
        finally:
            conn.close()

    def _query_fear_greed(self) -> int | None:
        """Return the most recent Fear & Greed score, or None if unavailable."""
        conn = sqlite3.connect(config.DATABASE_PATH)
        try:
            cursor = conn.execute(
                "SELECT score FROM fear_greed ORDER BY timestamp DESC LIMIT 1"
            )
            row = cursor.fetchone()
            return row[0] if row else None
        finally:
            conn.close()

    def generate_signal(self, candles: list[dict]) -> Signal:
        """Generate a news-driven signal from recent headlines and Fear & Greed.

        Logic:
            - 2+ positive headlines in last 30 min AND fear_greed > 60 → BUY
            - 2+ negative headlines in last 30 min AND fear_greed < 40 → SELL
            - fear_greed < 20 (Extreme Fear) → SKIP regardless
            - Otherwise → SKIP
        """
        if len(candles) < REQUIRED_CANDLES:
            return Signal("SKIP", 0.0, f"Need {REQUIRED_CANDLES} candles, got {len(candles)}")

        current_ts = candles[-1]["timestamp"]
        since_ts = current_ts - NEWS_WINDOW_SECONDS

        fg_score = self._query_fear_greed()
        if fg_score is None:
            return Signal("SKIP", 0.0, "No Fear & Greed data available")

        if fg_score < FG_EXTREME_FEAR_THRESHOLD:
            return Signal("SKIP", 0.0, f"Extreme Fear (F&G={fg_score}) — standing aside")

        news_counts = self._query_news(since_ts)

        if (
            news_counts["positive"] >= POSITIVE_HEADLINE_THRESHOLD
            and fg_score > FG_BULLISH_THRESHOLD
        ):
            return Signal(
                "BUY",
                SIGNAL_CONFIDENCE,
                f"{news_counts['positive']} positive headlines, F&G={fg_score}",
            )

        if (
            news_counts["negative"] >= NEGATIVE_HEADLINE_THRESHOLD
            and fg_score < FG_BEARISH_THRESHOLD
        ):
            return Signal(
                "SELL",
                SIGNAL_CONFIDENCE,
                f"{news_counts['negative']} negative headlines, F&G={fg_score}",
            )

        return Signal(
            "SKIP",
            0.0,
            f"No setup — pos={news_counts['positive']} neg={news_counts['negative']} F&G={fg_score}",
        )
