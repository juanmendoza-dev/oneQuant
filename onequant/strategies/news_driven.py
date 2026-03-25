"""Strategy C — News Driven.

Combines recent news headline sentiment with the Fear & Greed Index to
generate directional signals. News and fear/greed data are bulk-loaded
once via _preload_data() before the backtest loop starts.
"""

import bisect
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

    def __init__(self) -> None:
        self._fg_cache: dict[int, int] | None = None
        self._fg_timestamps: list[int] = []
        self._news_cache: list[tuple[int, str]] | None = None

    def _preload_data(self, start_ts: int, end_ts: int) -> None:
        """Bulk-load all fear_greed and news_feed rows for the backtest range.

        Called once by the engine before the main candle loop.
        """
        conn = sqlite3.connect(config.DATABASE_PATH)
        try:
            # Load fear & greed — keyed by timestamp for fast lookup
            rows = conn.execute(
                "SELECT timestamp, score FROM fear_greed "
                "WHERE timestamp >= ? AND timestamp <= ? "
                "ORDER BY timestamp ASC",
                (start_ts, end_ts),
            ).fetchall()
            if not rows:
                print(
                    "WARNING: fear_greed table has 0 rows in backtest range "
                    "— News Driven strategy will SKIP all candles"
                )
                self._fg_cache = {}
                self._fg_timestamps = []
            else:
                self._fg_cache = {ts: score for ts, score in rows}
                self._fg_timestamps = sorted(self._fg_cache.keys())

            # Load news feed — sorted by timestamp for windowed lookups
            rows = conn.execute(
                "SELECT timestamp, sentiment FROM news_feed "
                "WHERE timestamp >= ? AND timestamp <= ? "
                "ORDER BY timestamp ASC",
                (start_ts, end_ts),
            ).fetchall()
            if not rows:
                print(
                    "WARNING: news_feed table has 0 rows in backtest range "
                    "— News Driven strategy will SKIP all candles"
                )
                self._news_cache = []
            else:
                self._news_cache = [(ts, sentiment) for ts, sentiment in rows]
        finally:
            conn.close()

    def _query_fear_greed(self, current_ts: int) -> int | None:
        """Return the most recent Fear & Greed score at or before current_ts."""
        if not self._fg_timestamps:
            return None
        idx = bisect.bisect_right(self._fg_timestamps, current_ts) - 1
        if idx < 0:
            return None
        return self._fg_cache[self._fg_timestamps[idx]]

    def _query_news(self, since_ts: int, until_ts: int) -> dict[str, int]:
        """Count positive/negative/neutral headlines in [since_ts, until_ts]."""
        counts: dict[str, int] = {"positive": 0, "negative": 0, "neutral": 0}
        if not self._news_cache:
            return counts
        lo = bisect.bisect_left(self._news_cache, (since_ts,))
        for i in range(lo, len(self._news_cache)):
            ts, sentiment = self._news_cache[i]
            if ts > until_ts:
                break
            if sentiment in counts:
                counts[sentiment] += 1
        return counts

    def generate_signal(self, candles: list[dict]) -> Signal:
        """Generate a news-driven signal from recent headlines and Fear & Greed.

        Logic:
            - 2+ positive headlines in last 30 min AND fear_greed > 60 → BUY
            - 2+ negative headlines in last 30 min AND fear_greed < 40 → SELL
            - fear_greed < 20 (Extreme Fear) → SKIP regardless
            - Otherwise → SKIP
        """
        if self._fg_cache is None:
            return Signal("SKIP", 0.0, "News data not preloaded")

        if not self._fg_timestamps and not self._news_cache:
            return Signal("SKIP", 0.0, "Insufficient news history")

        if len(candles) < REQUIRED_CANDLES:
            return Signal("SKIP", 0.0, f"Need {REQUIRED_CANDLES} candles, got {len(candles)}")

        current_ts = candles[-1]["timestamp"]
        since_ts = current_ts - NEWS_WINDOW_SECONDS

        fg_score = self._query_fear_greed(current_ts)
        if fg_score is None:
            return Signal("SKIP", 0.0, "No Fear & Greed data available")

        if fg_score < FG_EXTREME_FEAR_THRESHOLD:
            return Signal("SKIP", 0.0, f"Extreme Fear (F&G={fg_score}) — standing aside")

        news_counts = self._query_news(since_ts, current_ts)

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
