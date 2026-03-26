"""SQLite database setup, table creation, and insert helpers."""

import time
from pathlib import Path
from typing import Optional

import aiosqlite

from config import config

# ---------------------------------------------------------------------------
# Table definitions
# ---------------------------------------------------------------------------

CREATE_BTC_CANDLES: str = """
CREATE TABLE IF NOT EXISTS btc_candles (
    id INTEGER PRIMARY KEY,
    symbol TEXT NOT NULL DEFAULT 'BTC-USD',
    timestamp INTEGER NOT NULL,
    timeframe TEXT NOT NULL,
    open REAL NOT NULL,
    high REAL NOT NULL,
    low REAL NOT NULL,
    close REAL NOT NULL,
    volume REAL NOT NULL,
    created_at INTEGER NOT NULL,
    UNIQUE(symbol, timestamp, timeframe)
)"""

CREATE_NEWS_FEED: str = """
CREATE TABLE IF NOT EXISTS news_feed (
    id INTEGER PRIMARY KEY,
    timestamp INTEGER NOT NULL,
    source TEXT NOT NULL,
    headline TEXT NOT NULL,
    url TEXT NOT NULL,
    sentiment TEXT NOT NULL,
    currencies TEXT NOT NULL,
    created_at INTEGER NOT NULL
)"""

CREATE_FEAR_GREED: str = """
CREATE TABLE IF NOT EXISTS fear_greed (
    id INTEGER PRIMARY KEY,
    timestamp INTEGER NOT NULL,
    score INTEGER NOT NULL,
    label TEXT NOT NULL,
    created_at INTEGER NOT NULL
)"""

CREATE_SYSTEM_LOG: str = """
CREATE TABLE IF NOT EXISTS system_log (
    id INTEGER PRIMARY KEY,
    timestamp INTEGER NOT NULL,
    module TEXT NOT NULL,
    level TEXT NOT NULL,
    message TEXT NOT NULL
)"""

CREATE_INDEXES: list[str] = [
    "CREATE INDEX IF NOT EXISTS idx_candles_symbol ON btc_candles(symbol)",
    "CREATE INDEX IF NOT EXISTS idx_candles_ts ON btc_candles(timestamp)",
    "CREATE INDEX IF NOT EXISTS idx_candles_tf ON btc_candles(timeframe)",
    "CREATE INDEX IF NOT EXISTS idx_news_ts ON news_feed(timestamp)",
    "CREATE INDEX IF NOT EXISTS idx_news_url ON news_feed(url)",
    "CREATE INDEX IF NOT EXISTS idx_fg_ts ON fear_greed(timestamp)",
    "CREATE INDEX IF NOT EXISTS idx_syslog_ts ON system_log(timestamp)",
]

# ---------------------------------------------------------------------------
# Connection management
# ---------------------------------------------------------------------------

_conn: Optional[aiosqlite.Connection] = None


async def init_db() -> None:
    """Open the database connection and create all tables and indexes."""
    global _conn
    db_path = Path(config.DATABASE_PATH)
    db_path.parent.mkdir(parents=True, exist_ok=True)
    _conn = await aiosqlite.connect(str(db_path))
    await _conn.execute("PRAGMA journal_mode=WAL")
    await _conn.execute(CREATE_BTC_CANDLES)
    await _conn.execute(CREATE_NEWS_FEED)
    await _conn.execute(CREATE_FEAR_GREED)
    await _conn.execute(CREATE_SYSTEM_LOG)
    for idx_sql in CREATE_INDEXES:
        await _conn.execute(idx_sql)
    await _conn.commit()


async def close_db() -> None:
    """Close the database connection."""
    global _conn
    if _conn:
        await _conn.close()
        _conn = None


def _get_conn() -> aiosqlite.Connection:
    """Return the active database connection or raise if not initialized."""
    if _conn is None:
        raise RuntimeError("Database not initialized. Call init_db() first.")
    return _conn


# ---------------------------------------------------------------------------
# Insert helpers
# ---------------------------------------------------------------------------


async def insert_candle(
    timestamp: int,
    timeframe: str,
    open_: float,
    high: float,
    low: float,
    close: float,
    volume: float,
    symbol: str = "BTC-USD",
) -> None:
    """Insert a completed candle into btc_candles. Skips duplicates."""
    conn = _get_conn()
    await conn.execute(
        """INSERT OR IGNORE INTO btc_candles
           (symbol, timestamp, timeframe, open, high, low, close, volume, created_at)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)""",
        (symbol, timestamp, timeframe, open_, high, low, close, volume, int(time.time())),
    )
    await conn.commit()


async def insert_candles_bulk(
    rows: list[tuple[int, str, float, float, float, float, float]],
    symbol: str = "BTC-USD",
) -> int:
    """Bulk-insert candles. Returns number of rows actually inserted."""
    conn = _get_conn()
    cursor = await conn.executemany(
        """INSERT OR IGNORE INTO btc_candles
           (symbol, timestamp, timeframe, open, high, low, close, volume, created_at)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)""",
        [(symbol, ts, tf, o, h, l, c, v, int(time.time())) for ts, tf, o, h, l, c, v in rows],
    )
    await conn.commit()
    return cursor.rowcount


async def insert_news(
    timestamp: int,
    source: str,
    headline: str,
    url: str,
    sentiment: str,
    currencies: str,
) -> None:
    """Insert a news headline into news_feed."""
    conn = _get_conn()
    await conn.execute(
        """INSERT INTO news_feed
           (timestamp, source, headline, url, sentiment, currencies, created_at)
           VALUES (?, ?, ?, ?, ?, ?, ?)""",
        (timestamp, source, headline, url, sentiment, currencies, int(time.time())),
    )
    await conn.commit()


async def news_url_exists(url: str) -> bool:
    """Check if a news URL already exists in the database."""
    conn = _get_conn()
    cursor = await conn.execute(
        "SELECT 1 FROM news_feed WHERE url = ? LIMIT 1", (url,)
    )
    row = await cursor.fetchone()
    return row is not None


async def insert_fear_greed(timestamp: int, score: int, label: str) -> None:
    """Insert a Fear & Greed Index reading."""
    conn = _get_conn()
    await conn.execute(
        """INSERT INTO fear_greed (timestamp, score, label, created_at)
           VALUES (?, ?, ?, ?)""",
        (timestamp, score, label, int(time.time())),
    )
    await conn.commit()


async def insert_system_log(module: str, level: str, message: str) -> None:
    """Insert a log entry into system_log table."""
    conn = _get_conn()
    await conn.execute(
        """INSERT INTO system_log (timestamp, module, level, message)
           VALUES (?, ?, ?, ?)""",
        (int(time.time()), module, level, message),
    )
    await conn.commit()


async def get_table_count(table: str) -> int:
    """Get the row count for a given table."""
    allowed = {"btc_candles", "news_feed", "fear_greed", "system_log"}
    if table not in allowed:
        raise ValueError(f"Unknown table: {table}")
    conn = _get_conn()
    cursor = await conn.execute(f"SELECT COUNT(*) FROM {table}")  # noqa: S608
    row = await cursor.fetchone()
    return row[0] if row else 0


async def candle_exists(timestamp: int, timeframe: str, symbol: str = "BTC-USD") -> bool:
    """Check if a candle already exists for the given symbol, timestamp, and timeframe."""
    conn = _get_conn()
    cursor = await conn.execute(
        "SELECT 1 FROM btc_candles WHERE symbol = ? AND timestamp = ? AND timeframe = ? LIMIT 1",
        (symbol, timestamp, timeframe),
    )
    row = await cursor.fetchone()
    return row is not None
