"""Zero-edge test -- a random strategy must lose money after fees.

Generates 1000 synthetic BTC candles with realistic price movement
(geometric Brownian motion) and runs a purely random strategy 10 times
with seeds 0-9.  Every single run must show negative P&L.

Usage:
    cd oneQuant
    python tests/test_zero_edge.py
"""

import os
import random
import sqlite3
import sys
import tempfile

# ---------------------------------------------------------------------------
# Path & env setup (must happen before any project imports)
# ---------------------------------------------------------------------------
sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "onequant"))
os.environ.setdefault("COINBASE_API_KEY", "test")
os.environ.setdefault("COINBASE_API_SECRET", "test")

if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8")

from backtest.engine import BacktestConfig, run_backtest  # noqa: E402
from config import config  # noqa: E402
from strategies.base import BaseStrategy, Signal  # noqa: E402


# ---------------------------------------------------------------------------
# Synthetic data
# ---------------------------------------------------------------------------


def generate_realistic_candles(n: int = 1000, seed: int = 42) -> list[dict]:
    """Generate *n* synthetic BTC 15m candles via geometric Brownian motion.

    Each candle is built from four intra-candle price steps so that
    open != close and high/low are realistic.
    """
    rng = random.Random(seed)
    candles: list[dict] = []
    price = 50_000.0
    start_ts = 1_700_000_000
    interval = 900  # 15 min

    for i in range(n):
        open_price = price
        p = open_price
        prices = [p]
        for _ in range(4):
            p *= 1.0 + rng.gauss(0, 0.004)
            prices.append(p)

        close_price = prices[-1]
        high_price = max(prices) * (1.0 + rng.uniform(0, 0.0005))
        low_price = min(prices) * (1.0 - rng.uniform(0, 0.0005))

        candles.append({
            "timestamp": start_ts + i * interval,
            "open": round(open_price, 2),
            "high": round(high_price, 2),
            "low": round(low_price, 2),
            "close": round(close_price, 2),
            "volume": round(rng.uniform(50, 500), 2),
        })
        price = close_price

    return candles


# ---------------------------------------------------------------------------
# Temp database helper
# ---------------------------------------------------------------------------


def create_test_db(candles: list[dict], timeframe: str = "15m") -> str:
    """Write candles into a throwaway SQLite file and return its path."""
    db_path = tempfile.mktemp(suffix=".db")
    conn = sqlite3.connect(db_path)
    conn.execute(
        "CREATE TABLE btc_candles ("
        "  timestamp INTEGER, open REAL, high REAL, low REAL,"
        "  close REAL, volume REAL, timeframe TEXT,"
        "  UNIQUE(timestamp, timeframe))"
    )
    for c in candles:
        conn.execute(
            "INSERT INTO btc_candles VALUES (?,?,?,?,?,?,?)",
            (c["timestamp"], c["open"], c["high"], c["low"],
             c["close"], c["volume"], timeframe),
        )
    conn.commit()
    conn.close()
    return db_path


# ---------------------------------------------------------------------------
# Random strategy
# ---------------------------------------------------------------------------


class RandomStrategy(BaseStrategy):
    """Makes completely random decisions using random.random()."""

    name = "Random"
    timeframe = "15m"
    required_candles = 3

    def __init__(self, seed: int):
        self._rng = random.Random(seed)

    def generate_signal(self, candles: list[dict]) -> Signal:
        val = self._rng.random()
        if val < 0.33:
            return Signal("BUY", 1.0, "random buy")
        elif val < 0.66:
            return Signal("SELL", 1.0, "random sell")
        return Signal("SKIP", 0.0, "random skip")


# ---------------------------------------------------------------------------
# Test runner
# ---------------------------------------------------------------------------


def run_zero_edge_test() -> bool:
    """Run 10 random-strategy backtests (seeds 0-9).  All must lose money."""
    print("=" * 55)
    print("  Zero-Edge Test")
    print("=" * 55)
    print("  Generating 1000 synthetic BTC candles (GBM)...\n")

    candles = generate_realistic_candles(n=1000, seed=42)
    db_path = create_test_db(candles)
    original_db = config.DATABASE_PATH
    object.__setattr__(config, "DATABASE_PATH", db_path)

    pnls: list[float] = []
    try:
        for seed in range(10):
            strategy = RandomStrategy(seed=seed)
            cfg = BacktestConfig(
                strategy=strategy,
                timeframe="15m",
                initial_capital=25.0,
                position_size_pct=0.10,
                stop_loss_pct=0.02,
                take_profit_pct=0.03,
                min_confidence=0.50,
                slippage_pct=0.001,
                spread_pct=0.0005,
                fee_pct=0.004,
            )
            result = run_backtest(cfg)
            total_pnl = sum(t.pnl for t in result.trades)
            n_trades = len(result.trades)
            pnls.append(total_pnl)

            tag = "OK" if total_pnl < 0 else "PROFITABLE"
            print(f"  Seed {seed}: {n_trades:3d} trades,  P&L = ${total_pnl:+.4f}  [{tag}]")

            if total_pnl >= 0:
                print("\n  CRITICAL ENGINE BUG")
                print("  A zero-edge strategy produced non-negative P&L.")
                print("  Random trading MUST lose money after fees/slippage/spread.")
                return False
    finally:
        object.__setattr__(config, "DATABASE_PATH", original_db)
        try:
            os.unlink(db_path)
        except OSError:
            pass

    print(f"\n  P&L range: ${min(pnls):+.4f}  to  ${max(pnls):+.4f}")
    print(f"  Mean P&L:  ${sum(pnls) / len(pnls):+.4f}")
    print("\n  ENGINE PASSES ZERO EDGE TEST")
    return True


if __name__ == "__main__":
    success = run_zero_edge_test()
    sys.exit(0 if success else 1)
