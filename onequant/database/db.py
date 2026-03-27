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

CREATE_PAPER_TRADES: str = """
CREATE TABLE IF NOT EXISTS paper_trades (
    id INTEGER PRIMARY KEY,
    strategy TEXT NOT NULL,
    symbol TEXT NOT NULL DEFAULT 'BTC-USD',
    timeframe TEXT NOT NULL DEFAULT '15m',
    signal_time INTEGER NOT NULL,
    direction TEXT NOT NULL,
    entry_price REAL NOT NULL,
    stop_loss REAL NOT NULL,
    take_profit REAL NOT NULL,
    position_size_usd REAL NOT NULL DEFAULT 250.00,
    status TEXT NOT NULL,
    exit_price REAL,
    exit_time INTEGER,
    pnl REAL,
    pnl_pct REAL,
    fees_paid REAL,
    regime TEXT,
    signal_reason TEXT,
    backtest_predicted_wr REAL DEFAULT 76.8,
    created_at INTEGER NOT NULL
)"""

CREATE_INDEXES: list[str] = [
    "CREATE INDEX IF NOT EXISTS idx_candles_symbol ON btc_candles(symbol)",
    "CREATE INDEX IF NOT EXISTS idx_candles_ts ON btc_candles(timestamp)",
    "CREATE INDEX IF NOT EXISTS idx_candles_tf ON btc_candles(timeframe)",
    "CREATE INDEX IF NOT EXISTS idx_news_ts ON news_feed(timestamp)",
    "CREATE INDEX IF NOT EXISTS idx_news_url ON news_feed(url)",
    "CREATE INDEX IF NOT EXISTS idx_fg_ts ON fear_greed(timestamp)",
    "CREATE INDEX IF NOT EXISTS idx_syslog_ts ON system_log(timestamp)",
    "CREATE INDEX IF NOT EXISTS idx_paper_status ON paper_trades(status)",
    "CREATE INDEX IF NOT EXISTS idx_paper_ts ON paper_trades(signal_time)",
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
    await _conn.execute(CREATE_PAPER_TRADES)
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


async def get_last_15m_candles(
    limit: int = 230,
    symbol: str = "BTC-USD",
) -> list[dict]:
    """Return the most recent *limit* 15m candles for symbol, ascending by timestamp."""
    conn = _get_conn()
    cursor = await conn.execute(
        """SELECT timestamp, open, high, low, close, volume
           FROM btc_candles
           WHERE symbol = ? AND timeframe = '15m'
           ORDER BY timestamp DESC LIMIT ?""",
        (symbol, limit),
    )
    rows = await cursor.fetchall()
    cols = ("timestamp", "open", "high", "low", "close", "volume")
    return [dict(zip(cols, row)) for row in reversed(rows)]


# ---------------------------------------------------------------------------
# Paper trade helpers
# ---------------------------------------------------------------------------


async def insert_paper_trade(
    strategy: str,
    signal_time: int,
    direction: str,
    entry_price: float,
    stop_loss: float,
    take_profit: float,
    position_size_usd: float,
    regime: str,
    signal_reason: str,
    fees_paid: float,
    symbol: str = "BTC-USD",
    timeframe: str = "15m",
) -> int:
    """Insert a new OPEN paper trade. Returns the row id."""
    conn = _get_conn()
    cursor = await conn.execute(
        """INSERT INTO paper_trades
           (strategy, symbol, timeframe, signal_time, direction, entry_price,
            stop_loss, take_profit, position_size_usd, status, fees_paid,
            regime, signal_reason, created_at)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 'OPEN', ?, ?, ?, ?)""",
        (
            strategy, symbol, timeframe, signal_time, direction, entry_price,
            stop_loss, take_profit, position_size_usd, fees_paid,
            regime, signal_reason, int(time.time()),
        ),
    )
    await conn.commit()
    return cursor.lastrowid


async def insert_skipped_paper_trade(
    strategy: str,
    signal_time: int,
    direction: str,
    entry_price: float,
    stop_loss: float,
    take_profit: float,
    regime: str,
    signal_reason: str,
    symbol: str = "BTC-USD",
    timeframe: str = "15m",
) -> None:
    """Log a SKIPPED signal (fired while a position was already open)."""
    conn = _get_conn()
    await conn.execute(
        """INSERT INTO paper_trades
           (strategy, symbol, timeframe, signal_time, direction, entry_price,
            stop_loss, take_profit, position_size_usd, status,
            regime, signal_reason, created_at)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?, 250.0, 'SKIPPED', ?, ?, ?)""",
        (
            strategy, symbol, timeframe, signal_time, direction, entry_price,
            stop_loss, take_profit, regime, signal_reason, int(time.time()),
        ),
    )
    await conn.commit()


async def update_paper_trade_closed(
    trade_id: int,
    status: str,
    exit_price: float,
    exit_time: int,
    pnl: float,
    pnl_pct: float,
    fees_paid: float,
) -> None:
    """Mark an open paper trade as WIN or LOSS and record exit details."""
    conn = _get_conn()
    await conn.execute(
        """UPDATE paper_trades
           SET status = ?, exit_price = ?, exit_time = ?,
               pnl = ?, pnl_pct = ?, fees_paid = fees_paid + ?
           WHERE id = ?""",
        (status, exit_price, exit_time, pnl, pnl_pct, fees_paid, trade_id),
    )
    await conn.commit()


async def get_open_paper_trade(symbol: str = "BTC-USD") -> Optional[dict]:
    """Return the currently open paper trade for symbol, or None."""
    conn = _get_conn()
    cursor = await conn.execute(
        """SELECT id, strategy, symbol, timeframe, signal_time, direction,
                  entry_price, stop_loss, take_profit, position_size_usd,
                  status, regime, signal_reason
           FROM paper_trades
           WHERE symbol = ? AND status = 'OPEN'
           ORDER BY signal_time DESC LIMIT 1""",
        (symbol,),
    )
    row = await cursor.fetchone()
    if row is None:
        return None
    cols = (
        "id", "strategy", "symbol", "timeframe", "signal_time", "direction",
        "entry_price", "stop_loss", "take_profit", "position_size_usd",
        "status", "regime", "signal_reason",
    )
    return dict(zip(cols, row))


async def get_paper_trades(limit: int = 50) -> list[dict]:
    """Return the most recent *limit* paper trades, newest first."""
    conn = _get_conn()
    cursor = await conn.execute(
        """SELECT id, strategy, symbol, timeframe, signal_time, direction,
                  entry_price, stop_loss, take_profit, position_size_usd,
                  status, exit_price, exit_time, pnl, pnl_pct, fees_paid,
                  regime, signal_reason, backtest_predicted_wr, created_at
           FROM paper_trades
           ORDER BY signal_time DESC LIMIT ?""",
        (limit,),
    )
    rows = await cursor.fetchall()
    cols = (
        "id", "strategy", "symbol", "timeframe", "signal_time", "direction",
        "entry_price", "stop_loss", "take_profit", "position_size_usd",
        "status", "exit_price", "exit_time", "pnl", "pnl_pct", "fees_paid",
        "regime", "signal_reason", "backtest_predicted_wr", "created_at",
    )
    return [dict(zip(cols, row)) for row in rows]


async def get_paper_stats() -> dict:
    """Aggregate stats over all closed paper trades (WIN + LOSS only)."""
    conn = _get_conn()
    cursor = await conn.execute(
        """SELECT
               COUNT(*) AS total_trades,
               SUM(CASE WHEN status = 'WIN' THEN 1 ELSE 0 END) AS wins,
               SUM(CASE WHEN status = 'LOSS' THEN 1 ELSE 0 END) AS losses,
               SUM(pnl) AS total_pnl,
               AVG(pnl) AS avg_pnl,
               SUM(CASE WHEN pnl > 0 THEN pnl ELSE 0 END) AS gross_profit,
               SUM(CASE WHEN pnl < 0 THEN ABS(pnl) ELSE 0 END) AS gross_loss
           FROM paper_trades
           WHERE status IN ('WIN', 'LOSS')"""
    )
    row = await cursor.fetchone()
    if row is None or row[0] == 0:
        return {
            "total_trades": 0,
            "wins": 0,
            "losses": 0,
            "win_rate": 0.0,
            "total_pnl": 0.0,
            "avg_pnl": 0.0,
            "profit_factor": 0.0,
            "backtest_predicted_wr": 76.8,
            "divergence_pct": 0.0,
        }

    total, wins, losses, total_pnl, avg_pnl, gross_profit, gross_loss = row
    win_rate = (wins / total) if total > 0 else 0.0
    profit_factor = (gross_profit / gross_loss) if gross_loss and gross_loss > 0 else float("inf")
    divergence_pct = ((win_rate * 100) - 76.8) if total > 0 else 0.0

    return {
        "total_trades": total,
        "wins": wins,
        "losses": losses,
        "win_rate": round(win_rate, 4),
        "total_pnl": round(total_pnl or 0.0, 2),
        "avg_pnl": round(avg_pnl or 0.0, 2),
        "profit_factor": round(profit_factor, 3),
        "backtest_predicted_wr": 76.8,
        "divergence_pct": round(divergence_pct, 2),
    }
