"""Parameter sweep targeting TRADE FREQUENCY for Mean Reversion.

Tests RSI oversold × EMA deviation combinations, ranks by
profit_factor × log(trade_count) to reward both quality and quantity.

Usage:
    cd onequant/
    python sweep_frequency.py
"""

import math
import sqlite3
import sys
from dataclasses import dataclass

if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8")

from backtest.engine import BacktestConfig, run_backtest
from config import config
from strategies.base import BaseStrategy, Signal

# ---------------------------------------------------------------------------
# Strategy with tuneable thresholds
# ---------------------------------------------------------------------------

RSI_PERIOD = 14
EMA_PERIOD = 20
VOLUME_MA_PERIOD = 10


def _calculate_rsi(closes: list[float], period: int) -> float:
    if len(closes) < period + 1:
        return 50.0
    deltas = [closes[i] - closes[i - 1] for i in range(1, len(closes))]
    gains = [max(d, 0.0) for d in deltas]
    losses = [abs(min(d, 0.0)) for d in deltas]
    avg_gain = sum(gains[:period]) / period
    avg_loss = sum(losses[:period]) / period
    for i in range(period, len(deltas)):
        avg_gain = (avg_gain * (period - 1) + gains[i]) / period
        avg_loss = (avg_loss * (period - 1) + losses[i]) / period
    if avg_loss == 0.0:
        return 100.0
    return 100.0 - 100.0 / (1.0 + avg_gain / avg_loss)


def _calculate_ema(values: list[float], period: int) -> float:
    if len(values) < period:
        return sum(values) / len(values)
    m = 2.0 / (period + 1)
    ema = sum(values[:period]) / period
    for v in values[period:]:
        ema = (v - ema) * m + ema
    return ema


class TuneableMeanReversion(BaseStrategy):
    """Mean Reversion with adjustable RSI and EMA deviation thresholds."""

    name = "MR-Sweep"
    timeframe = "15m"
    required_candles = 21

    def __init__(self, rsi_oversold: float, rsi_overbought: float, ema_dev: float):
        self.rsi_oversold = rsi_oversold
        self.rsi_overbought = rsi_overbought
        self.ema_dev = ema_dev

    def generate_signal(self, candles: list[dict]) -> Signal:
        if len(candles) < self.required_candles:
            return Signal("SKIP", 0.0, "not enough candles")

        closes = [c["close"] for c in candles]
        rsi = _calculate_rsi(closes, RSI_PERIOD)
        ema = _calculate_ema(closes, EMA_PERIOD)
        current_close = closes[-1]

        if ema == 0:
            return Signal("SKIP", 0.0, "ema zero")

        dev_pct = ((current_close - ema) / ema) * 100.0

        # Volume confirmation
        volumes = [c["volume"] for c in candles[-VOLUME_MA_PERIOD:]]
        vol_ma = sum(volumes) / len(volumes)
        if vol_ma > 0 and candles[-1]["volume"] <= vol_ma:
            return Signal("SKIP", 0.0, "volume below avg")

        if rsi < self.rsi_oversold and dev_pct < -self.ema_dev:
            pts = self.rsi_oversold - rsi
            extra = (pts // 5) * 0.10
            conf = min(0.60 + extra, 0.90)
            return Signal("BUY", conf, f"RSI {rsi:.1f}")

        if rsi > self.rsi_overbought and dev_pct > self.ema_dev:
            pts = rsi - self.rsi_overbought
            extra = (pts // 5) * 0.10
            conf = min(0.60 + extra, 0.90)
            return Signal("SELL", conf, f"RSI {rsi:.1f}")

        return Signal("SKIP", 0.0, "no setup")


# ---------------------------------------------------------------------------
# Sweep
# ---------------------------------------------------------------------------

RSI_OVERSOLD_VALUES = [24, 26, 28, 30, 32]
EMA_DEVIATION_VALUES = [1.5, 2.0, 2.5, 3.0]


@dataclass
class SweepResult:
    rsi_os: float
    rsi_ob: float
    ema_dev: float
    trades: int
    win_rate: float
    profit_factor: float
    total_pnl: float
    score: float  # PF * log(trade_count)


def run_sweep() -> list[SweepResult]:
    """Run the full parameter sweep and return sorted results."""
    # Verify data exists
    conn = sqlite3.connect(config.DATABASE_PATH)
    row = conn.execute(
        "SELECT COUNT(*) FROM btc_candles WHERE timeframe = '15m'"
    ).fetchone()
    conn.close()
    candle_count = row[0] if row else 0
    print(f"  15m candles in database: {candle_count:,}")
    if candle_count < 100:
        print("  ERROR: Not enough candle data. Run historical.fetch first.")
        return []

    results: list[SweepResult] = []
    combos = [(os_val, ema_val)
              for os_val in RSI_OVERSOLD_VALUES
              for ema_val in EMA_DEVIATION_VALUES]
    total = len(combos)

    print(f"  Testing {total} combinations...\n")

    for idx, (rsi_os, ema_dev) in enumerate(combos, 1):
        rsi_ob = 100.0 - rsi_os  # symmetric: oversold 24 → overbought 76

        strategy = TuneableMeanReversion(
            rsi_oversold=rsi_os,
            rsi_overbought=rsi_ob,
            ema_dev=ema_dev,
        )
        cfg = BacktestConfig(
            strategy=strategy,
            timeframe="15m",
            initial_capital=25.0,
            position_size_pct=0.10,
            stop_loss_pct=0.02,
            take_profit_pct=0.03,
            min_confidence=0.55,
            slippage_pct=0.001,
            spread_pct=0.0005,
        )

        try:
            result = run_backtest(cfg)
        except RuntimeError as e:
            print(f"  [{idx:2d}/{total}] RSI {rsi_os}/{rsi_ob} EMA {ema_dev}% — ERROR: {e}")
            continue

        trades = result.trades
        n = len(trades)
        wins = sum(1 for t in trades if t.outcome == "WIN")
        losses_list = [t for t in trades if t.outcome == "LOSS"]
        wr = wins / n if n else 0.0

        gross_win = sum(t.pnl for t in trades if t.pnl > 0)
        gross_loss = abs(sum(t.pnl for t in trades if t.pnl < 0))
        pf = gross_win / gross_loss if gross_loss > 0 else 0.0
        total_pnl = sum(t.pnl for t in trades)

        score = pf * math.log(n) if n > 1 and pf > 0 else 0.0

        results.append(SweepResult(
            rsi_os=rsi_os,
            rsi_ob=rsi_ob,
            ema_dev=ema_dev,
            trades=n,
            win_rate=wr,
            profit_factor=pf,
            total_pnl=total_pnl,
            score=score,
        ))

        tag = ""
        if n >= 100 and wr > 0.60 and pf > 1.0:
            tag = " <-- CANDIDATE"
        print(f"  [{idx:2d}/{total}] RSI {rsi_os}/{rsi_ob:.0f}  EMA {ema_dev}%  "
              f"n={n:4d}  WR={wr*100:5.1f}%  PF={pf:.3f}  "
              f"P&L=${total_pnl:+.2f}{tag}")

    return results


def print_top_results(results: list[SweepResult]) -> None:
    """Print the top 10 combinations sorted by score."""
    # Sort by score descending
    ranked = sorted(results, key=lambda r: r.score, reverse=True)
    top = ranked[:10]

    print("\n" + "=" * 75)
    print("  Top 10 by PF * log(trade_count)")
    print("=" * 75)
    print(f"  {'#':>2}  {'RSI_OS':>6} {'RSI_OB':>6} {'EMA%':>5}  "
          f"{'Trades':>6} {'WR%':>6} {'PF':>6} {'P&L':>9} {'Score':>7}")
    print("  " + "-" * 71)

    for i, r in enumerate(top, 1):
        flag = ""
        if r.trades >= 100 and r.win_rate > 0.60 and r.profit_factor > 1.0:
            flag = " *"
        print(f"  {i:2d}  {r.rsi_os:6.0f} {r.rsi_ob:6.0f} {r.ema_dev:5.1f}  "
              f"{r.trades:6d} {r.win_rate*100:5.1f}% {r.profit_factor:6.3f} "
              f"${r.total_pnl:+8.2f} {r.score:7.3f}{flag}")

    # Summarize candidates
    candidates = [r for r in results
                  if r.trades >= 100 and r.win_rate > 0.60 and r.profit_factor > 1.0]
    print(f"\n  Candidates (100+ trades, WR>60%, PF>1.0): {len(candidates)}")
    if candidates:
        best = max(candidates, key=lambda r: r.score)
        print(f"  Best candidate: RSI {best.rsi_os}/{best.rsi_ob:.0f}, "
              f"EMA {best.ema_dev}%, "
              f"n={best.trades}, WR={best.win_rate*100:.1f}%, "
              f"PF={best.profit_factor:.3f}, "
              f"P&L=${best.total_pnl:+.2f}")
    else:
        print("  No combination met all three criteria.")


def main() -> None:
    print("=" * 75)
    print("  Mean Reversion Parameter Sweep — Trade Frequency Focus")
    print("=" * 75)

    results = run_sweep()
    if results:
        print_top_results(results)


if __name__ == "__main__":
    main()
