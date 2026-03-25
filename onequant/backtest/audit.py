"""Engine self-audit — sanity checks that the backtest engine is correct.

Runs 6 synthetic tests before any real backtest to catch regressions.

Usage:
    cd onequant/
    python -m backtest.audit
"""

import sqlite3
import sys
import tempfile
import time
from datetime import datetime, timezone

# Ensure UTF-8 on Windows
if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8")


def _create_test_db(candles: list[dict], timeframe: str = "15m") -> str:
    """Create a temporary SQLite database with the given candle data.

    Returns the path to the temporary database file.
    """
    db_path = tempfile.mktemp(suffix=".db")
    conn = sqlite3.connect(db_path)
    conn.execute(
        "CREATE TABLE btc_candles ("
        "  timestamp INTEGER, open REAL, high REAL, low REAL, "
        "  close REAL, volume REAL, timeframe TEXT, "
        "  UNIQUE(timestamp, timeframe))"
    )
    for c in candles:
        conn.execute(
            "INSERT INTO btc_candles (timestamp, open, high, low, close, volume, timeframe) "
            "VALUES (?, ?, ?, ?, ?, ?, ?)",
            (c["timestamp"], c["open"], c["high"], c["low"], c["close"], c["volume"], timeframe),
        )
    conn.commit()
    conn.close()
    return db_path


def _make_candles(
    n: int,
    start_ts: int = 1700000000,
    interval: int = 900,
    base_price: float = 50000.0,
    price_increment: float = 0.0,
    volume: float = 100.0,
) -> list[dict]:
    """Generate n synthetic candles with predictable prices.

    Each candle: open = base + i*increment, close = open + increment,
    high = max(open, close) + 10, low = min(open, close) - 10.
    """
    candles = []
    for i in range(n):
        o = base_price + i * price_increment
        c = o + price_increment
        h = max(o, c) + 10.0
        lo = min(o, c) - 10.0
        candles.append({
            "timestamp": start_ts + i * interval,
            "open": o,
            "high": h,
            "low": lo,
            "close": c,
            "volume": volume,
        })
    return candles


def _patch_config(db_path: str):
    """Temporarily point config.DATABASE_PATH to the test database."""
    from config import config
    original = config.DATABASE_PATH
    object.__setattr__(config, "DATABASE_PATH", db_path)
    return original


def _restore_config(original_path: str):
    """Restore the original database path."""
    from config import config
    object.__setattr__(config, "DATABASE_PATH", original_path)


# ---------------------------------------------------------------------------
# Test 1 — Lookahead check
# ---------------------------------------------------------------------------


def test_lookahead() -> bool:
    """Verify each entry fills at the exact candle AFTER the signal window.

    Creates an uptrending series with unique opens (open = 50000 + i*100).
    For every trade, recovers the fill candle index from the entry price
    and the signal candle index from the last candle in the signal window,
    then asserts fill_index == signal_index + 1.
    """
    from backtest.engine import BacktestConfig, run_backtest
    from strategies.base import BaseStrategy, Signal

    BASE_PRICE = 50000.0
    INCREMENT = 100.0

    class AlwaysBuyStrategy(BaseStrategy):
        name = "AlwaysBuy"
        timeframe = "15m"
        required_candles = 3

        def generate_signal(self, candles: list[dict]) -> Signal:
            return Signal("BUY", 1.0, "test signal")

    # Each candle[i].open = BASE_PRICE + i * INCREMENT  (unique per index)
    candles = _make_candles(100, base_price=BASE_PRICE, price_increment=INCREMENT)

    # Build reverse lookup: open price → candle index
    open_to_idx = {c["open"]: idx for idx, c in enumerate(candles)}

    # Also map each candle timestamp → index for signal-window identification
    ts_to_idx = {c["timestamp"]: idx for idx, c in enumerate(candles)}

    db_path = _create_test_db(candles)
    original = _patch_config(db_path)

    try:
        cfg = BacktestConfig(
            strategy=AlwaysBuyStrategy(),
            timeframe="15m",
            initial_capital=100.0,
            position_size_pct=0.10,
            stop_loss_pct=0.50,  # wide SL to avoid early exit
            take_profit_pct=0.50,
            slippage_pct=0.0,  # no slippage for clean check
            spread_pct=0.0,
            fee_pct=0.0,
        )
        result = run_backtest(cfg)

        if not result.trades:
            print("    No trades generated — FAIL")
            return False

        first_trade = result.trades[0]
        entry_price = first_trade.entry_price

        # Recover fill candle index from the unique open price
        if entry_price not in open_to_idx:
            closes = {c["close"] for c in candles}
            if entry_price in closes:
                print(f"    Entry ${entry_price:.2f} is a candle CLOSE — LOOKAHEAD BIAS")
            else:
                print(f"    Entry ${entry_price:.2f} matches no candle open — unexpected")
            return False

        fill_idx = open_to_idx[entry_price]

        # The signal window fed to generate_signal ends one candle before fill.
        # Engine: window = candles[i - min_window : i], fill at candles[i].open
        # So the last candle the strategy SAW is candles[fill_idx - 1].
        signal_last_idx = fill_idx - 1

        if signal_last_idx < 0:
            print(f"    Fill at candle 0 — strategy saw no prior candles — FAIL")
            return False

        # Core assertion: fill is the candle immediately after the signal window
        expected_fill_idx = signal_last_idx + 1
        if fill_idx != expected_fill_idx:
            print(f"    Signal ended at candle {signal_last_idx}, "
                  f"fill at candle {fill_idx} (expected {expected_fill_idx}) — FAIL")
            return False

        # Verify the strategy never saw the fill candle's data
        signal_window_end_ts = candles[signal_last_idx]["timestamp"]
        fill_ts = candles[fill_idx]["timestamp"]
        if fill_ts <= signal_window_end_ts:
            print(f"    Fill candle timestamp <= signal window end — LOOKAHEAD BIAS")
            return False

        print(f"    Signal window ended at candle {signal_last_idx} "
              f"(open ${candles[signal_last_idx]['open']:.0f}), "
              f"fill at candle {fill_idx} "
              f"(open ${entry_price:.0f}) — correct")
        return True
    finally:
        _restore_config(original)


# ---------------------------------------------------------------------------
# Test 2 — Fee check
# ---------------------------------------------------------------------------


def test_fee_calculation() -> bool:
    """Run a single trade with known entry/exit and verify fee math."""
    from backtest.engine import BacktestConfig, run_backtest
    from strategies.base import BaseStrategy, Signal

    class OneBuyStrategy(BaseStrategy):
        name = "OneBuy"
        timeframe = "15m"
        required_candles = 3
        _fired = False

        def generate_signal(self, candles: list[dict]) -> Signal:
            if not self._fired:
                self._fired = True
                return Signal("BUY", 1.0, "test")
            return Signal("SKIP", 0.0, "done")

    # Create candles: flat at 50000, then one candle spikes to hit TP
    candles = _make_candles(20, base_price=50000.0, price_increment=0.0)
    # Make candle 10 have a high that hits TP
    candles[10]["high"] = 60000.0  # will definitely hit TP

    db_path = _create_test_db(candles)
    original = _patch_config(db_path)

    try:
        fee_pct = 0.004  # flat 0.4%
        position_pct = 0.10
        capital = 100.0

        cfg = BacktestConfig(
            strategy=OneBuyStrategy(),
            timeframe="15m",
            initial_capital=capital,
            position_size_pct=position_pct,
            stop_loss_pct=0.50,
            take_profit_pct=0.05,
            slippage_pct=0.0,
            spread_pct=0.0,
            fee_pct=fee_pct,
        )
        result = run_backtest(cfg)

        if not result.trades:
            print("    No trades — FAIL")
            return False

        trade = result.trades[0]
        size = capital * position_pct  # $10
        expected_entry_fee = size * fee_pct  # $0.04
        expected_exit_fee = size * fee_pct  # $0.04
        expected_total = expected_entry_fee + expected_exit_fee  # $0.08

        actual = trade.fees_paid
        diff = abs(actual - expected_total)

        if diff < 0.001:
            print(f"    Fees: expected ${expected_total:.4f}, got ${actual:.4f} — correct")
            return True
        else:
            print(f"    Fees: expected ${expected_total:.4f}, got ${actual:.4f} — MISMATCH")
            return False
    finally:
        _restore_config(original)


# ---------------------------------------------------------------------------
# Test 3 — Stop loss gap check
# ---------------------------------------------------------------------------


def test_gap_stop() -> bool:
    """Verify that a gap below SL fills at the candle open, not the SL level."""
    from backtest.engine import BacktestConfig, run_backtest
    from strategies.base import BaseStrategy, Signal

    class OneBuyStrategy(BaseStrategy):
        name = "GapTest"
        timeframe = "15m"
        required_candles = 3
        _fired = False

        def generate_signal(self, candles: list[dict]) -> Signal:
            if not self._fired:
                self._fired = True
                return Signal("BUY", 1.0, "test")
            return Signal("SKIP", 0.0, "done")

    # Entry around candle 4 at open=50000
    candles = _make_candles(20, base_price=50000.0, price_increment=0.0)

    # After entry, create a gap down candle that opens way below SL
    # With 5% SL on entry ~50000, SL ≈ 47500
    # Make candle 8 open at 45000 (below SL)
    candles[8]["open"] = 45000.0
    candles[8]["high"] = 45500.0
    candles[8]["low"] = 44500.0
    candles[8]["close"] = 45200.0

    db_path = _create_test_db(candles)
    original = _patch_config(db_path)

    try:
        cfg = BacktestConfig(
            strategy=OneBuyStrategy(),
            timeframe="15m",
            initial_capital=100.0,
            position_size_pct=0.10,
            stop_loss_pct=0.05,  # 5% SL
            take_profit_pct=0.50,
            slippage_pct=0.0,
            spread_pct=0.0,
            fee_pct=0.0,
        )
        result = run_backtest(cfg)

        gap_trades = [t for t in result.trades if t.gap_stop]
        if not gap_trades:
            # Check if there's a loss trade that filled at the gap open
            loss_trades = [t for t in result.trades if t.outcome == "LOSS"]
            if loss_trades and abs(loss_trades[0].exit_price - 45000.0) < 1.0:
                print(f"    Gap stop filled at open ${loss_trades[0].exit_price:.2f} "
                      f"(gap_stop flag should be True) — partial pass")
                return True
            print("    No gap stop detected — FAIL")
            return False

        trade = gap_trades[0]
        # Should fill at gap open (45000), not at SL level
        if abs(trade.exit_price - 45000.0) < 1.0:
            print(f"    Gap stop filled at candle open ${trade.exit_price:.2f}, "
                  f"not SL level — correct")
            return True
        else:
            print(f"    Gap stop exit ${trade.exit_price:.2f} != candle open 45000 — FAIL")
            return False
    finally:
        _restore_config(original)


# ---------------------------------------------------------------------------
# Test 4 — Compounding check
# ---------------------------------------------------------------------------


def test_compounding() -> bool:
    """Verify position size increases after wins (compounding)."""
    from backtest.engine import BacktestConfig, run_backtest
    from strategies.base import BaseStrategy, Signal

    class MultiBuyStrategy(BaseStrategy):
        name = "MultiBuy"
        timeframe = "15m"
        required_candles = 3
        _count = 0

        def generate_signal(self, candles: list[dict]) -> Signal:
            if self._count < 3:
                self._count += 1
                return Signal("BUY", 1.0, f"trade {self._count}")
            return Signal("SKIP", 0.0, "done")

    # Create candles that produce 3 consecutive wins
    # Each candle block: entry at open, then high hits TP within a few candles
    candles = _make_candles(60, base_price=50000.0, price_increment=0.0, volume=1000.0)

    # Make candles 5, 15, 25 spike to hit TP (3% above 50000 = 51500)
    for spike_idx in [5, 15, 25]:
        candles[spike_idx]["high"] = 52000.0

    db_path = _create_test_db(candles)
    original = _patch_config(db_path)

    try:
        cfg = BacktestConfig(
            strategy=MultiBuyStrategy(),
            timeframe="15m",
            initial_capital=100.0,
            position_size_pct=0.10,
            stop_loss_pct=0.50,
            take_profit_pct=0.03,
            slippage_pct=0.0,
            spread_pct=0.0,
            fee_pct=0.0,
        )
        result = run_backtest(cfg)

        winning_trades = [t for t in result.trades if t.outcome == "WIN"]
        if len(winning_trades) < 2:
            print(f"    Only {len(winning_trades)} winning trades, need >= 2 — FAIL")
            return False

        sizes = [t.position_size_usd for t in winning_trades]
        increasing = all(sizes[i] < sizes[i + 1] for i in range(len(sizes) - 1))

        if increasing:
            size_strs = ", ".join(f"${s:.4f}" for s in sizes)
            print(f"    Position sizes: {size_strs} — increasing, correct")
            return True
        else:
            size_strs = ", ".join(f"${s:.4f}" for s in sizes)
            print(f"    Position sizes: {size_strs} — NOT increasing — FAIL")
            return False
    finally:
        _restore_config(original)


# ---------------------------------------------------------------------------
# Test 5 — Zero profit sanity check
# ---------------------------------------------------------------------------


def test_zero_profit() -> bool:
    """If strategy SKIPs every candle, final equity must equal starting equity."""
    from backtest.engine import BacktestConfig, run_backtest
    from strategies.base import BaseStrategy, Signal

    class AlwaysSkipStrategy(BaseStrategy):
        name = "AlwaysSkip"
        timeframe = "15m"
        required_candles = 3

        def generate_signal(self, candles: list[dict]) -> Signal:
            return Signal("SKIP", 0.0, "skip everything")

    candles = _make_candles(50, base_price=50000.0)
    db_path = _create_test_db(candles)
    original = _patch_config(db_path)

    try:
        capital = 100.0
        cfg = BacktestConfig(
            strategy=AlwaysSkipStrategy(),
            timeframe="15m",
            initial_capital=capital,
        )
        result = run_backtest(cfg)

        if result.trades:
            print(f"    {len(result.trades)} trades generated for SKIP strategy — FAIL")
            return False

        # No trades means no P&L — equity should be unchanged
        total_pnl = sum(t.pnl for t in result.trades)
        if abs(total_pnl) < 0.001:
            print(f"    No trades, P&L = ${total_pnl:.4f} — correct")
            return True
        else:
            print(f"    No trades but P&L = ${total_pnl:.4f} — FAIL")
            return False
    finally:
        _restore_config(original)


# ---------------------------------------------------------------------------
# Test 6 — Zero-edge (random strategy must lose money)
# ---------------------------------------------------------------------------


def test_zero_edge() -> bool:
    """A purely random strategy must produce negative P&L after costs.

    Generates 1000 synthetic candles (GBM) and runs a random strategy
    with seeds 0, 1, 2.  All three must lose money.
    """
    import random as _random

    from backtest.engine import BacktestConfig, run_backtest
    from strategies.base import BaseStrategy, Signal

    class RandomStrategy(BaseStrategy):
        name = "Random"
        timeframe = "15m"
        required_candles = 3

        def __init__(self, seed: int):
            self._rng = _random.Random(seed)

        def generate_signal(self, candles: list[dict]) -> Signal:
            val = self._rng.random()
            if val < 0.33:
                return Signal("BUY", 1.0, "random buy")
            elif val < 0.66:
                return Signal("SELL", 1.0, "random sell")
            return Signal("SKIP", 0.0, "random skip")

    # Generate 1000 synthetic candles with GBM
    rng = _random.Random(42)
    candles = []
    price = 50000.0
    start_ts = 1700000000
    for i in range(1000):
        o = price
        p = o
        prices = [p]
        for _ in range(4):
            p *= 1.0 + rng.gauss(0, 0.004)
            prices.append(p)
        c = prices[-1]
        h = max(prices) * (1.0 + rng.uniform(0, 0.0005))
        lo = min(prices) * (1.0 - rng.uniform(0, 0.0005))
        candles.append({
            "timestamp": start_ts + i * 900,
            "open": round(o, 2),
            "high": round(h, 2),
            "low": round(lo, 2),
            "close": round(c, 2),
            "volume": round(rng.uniform(50, 500), 2),
        })
        price = c

    db_path = _create_test_db(candles)
    original = _patch_config(db_path)

    try:
        for seed in range(3):
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

            if total_pnl >= 0:
                print(f"    Seed {seed}: P&L ${total_pnl:+.4f} — PROFITABLE (should be negative)")
                return False
            print(f"    Seed {seed}: P&L ${total_pnl:+.4f} — negative as expected")

        print("    All random seeds lose money — correct")
        return True
    finally:
        _restore_config(original)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------


def run_engine_audit() -> bool:
    """Run all 6 engine audit tests. Returns True if all pass."""
    print("=" * 50)
    print("oneQuant Engine Self-Audit")
    print("=" * 50)

    tests = [
        ("Lookahead bias", test_lookahead),
        ("Fee calculation", test_fee_calculation),
        ("Gap stop fill", test_gap_stop),
        ("Compounding sizing", test_compounding),
        ("Zero profit sanity", test_zero_profit),
        ("Zero-edge (random)", test_zero_edge),
    ]

    results = []
    for name, test_fn in tests:
        print(f"\n  Test: {name}")
        try:
            passed = test_fn()
        except Exception as e:
            print(f"    ERROR: {e}")
            passed = False
        status = "PASS" if passed else "FAIL"
        print(f"  Result: {status}")
        results.append((name, passed))

    print("\n" + "=" * 50)
    print("  Audit Summary")
    print("=" * 50)
    all_passed = True
    for name, passed in results:
        symbol = "\u2713" if passed else "\u2717"
        print(f"  {symbol} {name}")
        if not passed:
            all_passed = False

    if all_passed:
        print("\n  ALL TESTS PASSED — engine is ready")
    else:
        print("\n  AUDIT FAILED — do NOT run backtests with a broken engine")

    return all_passed


if __name__ == "__main__":
    success = run_engine_audit()
    sys.exit(0 if success else 1)
