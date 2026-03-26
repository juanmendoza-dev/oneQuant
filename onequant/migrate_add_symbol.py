"""One-time migration: add symbol column to btc_candles.

Recreates the table with:
  - symbol TEXT NOT NULL DEFAULT 'BTC-USD'
  - UNIQUE(timestamp, timeframe, symbol)  — was (timestamp, timeframe)

Existing BTC-USD rows are preserved. Safe to run multiple times.

Usage:
    cd onequant/
    python migrate_add_symbol.py
"""

import sqlite3
import sys
from config import config


def main() -> None:
    conn = sqlite3.connect(config.DATABASE_PATH)
    conn.execute("PRAGMA journal_mode=WAL")

    # Check if already migrated
    cols = [r[1] for r in conn.execute("PRAGMA table_info(btc_candles)").fetchall()]
    if "symbol" in cols:
        print("Migration already applied — symbol column exists.")
        conn.close()
        return

    row_count = conn.execute("SELECT COUNT(*) FROM btc_candles").fetchone()[0]
    print(f"Migrating btc_candles ({row_count:,} rows)...")

    conn.executescript("""
        BEGIN;

        -- Create new table with symbol column and updated unique constraint
        CREATE TABLE btc_candles_new (
            id         INTEGER PRIMARY KEY,
            symbol     TEXT    NOT NULL DEFAULT 'BTC-USD',
            timestamp  INTEGER NOT NULL,
            timeframe  TEXT    NOT NULL,
            open       REAL    NOT NULL,
            high       REAL    NOT NULL,
            low        REAL    NOT NULL,
            close      REAL    NOT NULL,
            volume     REAL    NOT NULL,
            created_at INTEGER NOT NULL,
            UNIQUE(symbol, timestamp, timeframe)
        );

        -- Copy all existing rows, tagging them as BTC-USD
        INSERT INTO btc_candles_new
            (symbol, timestamp, timeframe, open, high, low, close, volume, created_at)
        SELECT 'BTC-USD', timestamp, timeframe, open, high, low, close, volume, created_at
        FROM btc_candles;

        -- Swap tables
        DROP TABLE btc_candles;
        ALTER TABLE btc_candles_new RENAME TO btc_candles;

        -- Recreate indexes
        CREATE INDEX IF NOT EXISTS idx_candles_symbol ON btc_candles(symbol);
        CREATE INDEX IF NOT EXISTS idx_candles_ts     ON btc_candles(timestamp);
        CREATE INDEX IF NOT EXISTS idx_candles_tf     ON btc_candles(timeframe);

        COMMIT;
    """)

    new_count = conn.execute("SELECT COUNT(*) FROM btc_candles").fetchone()[0]
    conn.close()

    if new_count == row_count:
        print(f"Migration complete — {new_count:,} rows preserved, symbol column added.")
    else:
        print(f"ERROR: row count changed {row_count} -> {new_count}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
