import sqlite3
import threading
from config import DATABASE_PATH

_local = threading.local()


def get_connection() -> sqlite3.Connection:
    if not hasattr(_local, "conn") or _local.conn is None:
        _local.conn = sqlite3.connect(DATABASE_PATH)
        _local.conn.execute("PRAGMA journal_mode=WAL")
        _local.conn.execute("PRAGMA busy_timeout=5000")
    return _local.conn


def init_db():
    conn = get_connection()
    conn.executescript("""
        CREATE TABLE IF NOT EXISTS btc_candles (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            timestamp INTEGER NOT NULL,
            open REAL NOT NULL,
            high REAL NOT NULL,
            low REAL NOT NULL,
            close REAL NOT NULL,
            volume REAL NOT NULL,
            timeframe TEXT NOT NULL,
            UNIQUE(timestamp, timeframe)
        );

        CREATE TABLE IF NOT EXISTS kalshi_markets (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            timestamp INTEGER NOT NULL,
            market_id TEXT NOT NULL,
            market_title TEXT NOT NULL,
            yes_price REAL NOT NULL,
            no_price REAL NOT NULL,
            spread REAL NOT NULL,
            volume INTEGER NOT NULL,
            expiry TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS news_feed (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            timestamp INTEGER NOT NULL,
            source TEXT NOT NULL,
            headline TEXT NOT NULL,
            url TEXT NOT NULL,
            sentiment_score REAL,
            currencies TEXT
        );

        CREATE TABLE IF NOT EXISTS fear_greed (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            timestamp INTEGER NOT NULL,
            score INTEGER NOT NULL,
            label TEXT NOT NULL,
            UNIQUE(timestamp)
        );

        CREATE INDEX IF NOT EXISTS idx_candles_ts ON btc_candles(timestamp);
        CREATE INDEX IF NOT EXISTS idx_candles_tf ON btc_candles(timeframe);
        CREATE INDEX IF NOT EXISTS idx_kalshi_ts ON kalshi_markets(timestamp);
        CREATE INDEX IF NOT EXISTS idx_news_ts ON news_feed(timestamp);
        CREATE INDEX IF NOT EXISTS idx_fg_ts ON fear_greed(timestamp);
    """)
    conn.commit()


def insert_candle(timestamp: int, o: float, h: float, l: float, c: float,
                  volume: float, timeframe: str):
    conn = get_connection()
    conn.execute(
        "INSERT OR IGNORE INTO btc_candles "
        "(timestamp, open, high, low, close, volume, timeframe) "
        "VALUES (?, ?, ?, ?, ?, ?, ?)",
        (timestamp, o, h, l, c, volume, timeframe),
    )
    conn.commit()


def insert_kalshi_market(timestamp: int, market_id: str, market_title: str,
                         yes_price: float, no_price: float, spread: float,
                         volume: int, expiry: str):
    conn = get_connection()
    conn.execute(
        "INSERT INTO kalshi_markets "
        "(timestamp, market_id, market_title, yes_price, no_price, spread, volume, expiry) "
        "VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
        (timestamp, market_id, market_title, yes_price, no_price, spread,
         volume, expiry),
    )
    conn.commit()


def insert_news(timestamp: int, source: str, headline: str, url: str,
                sentiment_score: float | None, currencies: str | None):
    conn = get_connection()
    conn.execute(
        "INSERT INTO news_feed "
        "(timestamp, source, headline, url, sentiment_score, currencies) "
        "VALUES (?, ?, ?, ?, ?, ?)",
        (timestamp, source, headline, url, sentiment_score, currencies),
    )
    conn.commit()


def insert_fear_greed(timestamp: int, score: int, label: str):
    conn = get_connection()
    conn.execute(
        "INSERT OR IGNORE INTO fear_greed (timestamp, score, label) "
        "VALUES (?, ?, ?)",
        (timestamp, score, label),
    )
    conn.commit()
