"""Parameter sweep targeting TRADE FREQUENCY for Mean Reversion.

Tests RSI oversold x EMA deviation x stop loss x take profit with
regime filter (BULL_TREND + BEAR_TREND only). Filters for combinations
with 80+ trades, PF >= 1.2, WR >= 60%, max drawdown > -15%.

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
# Sweep parameters
# ---------------------------------------------------------------------------

RSI_OVERSOLD_VALUES = [25, 28, 30, 32, 35]
EMA_DEVIATION_VALUES = [1.0, 1.5, 2.0, 2.5]
STOP_LOSS_VALUES = [0.04, 0.05, 0.06]
TAKE_PROFIT_VALUES = [0.02, 0.03, 0.04]

REGIME_FILTER = ["BULL_TREND", "BEAR_TREND"]


@dataclass
class SweepResult:
    rsi_os: float
    rsi_ob: float
    ema_dev: float
    stop_loss: float
    take_profit: float
    trades: int
    win_rate: float
    profit_factor: float
    max_drawdown: float
    total_pnl: float


def _max_drawdown(trades: list) -> float:
    """Compute max drawdown percentage from a list of TradeResult objects."""
    if not trades:
        return 0.0
    equity = trades[0].equity_before
    peak = equity
    max_dd = 0.0
    for t in trades:
        equity = t.equity_after
        if equity > peak:
            peak = equity
        dd = (peak - equity) / peak if peak > 0 else 0.0
        if dd > max_dd:
            max_dd = dd
    return max_dd * 100.0


def run_sweep() -> list[SweepResult]:
    """Run the full parameter sweep and return results."""
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

    combos = [
        (rsi_os, ema_dev, sl, tp)
        for rsi_os in RSI_OVERSOLD_VALUES
        for ema_dev in EMA_DEVIATION_VALUES
        for sl in STOP_LOSS_VALUES
        for tp in TAKE_PROFIT_VALUES
    ]
    total = len(combos)
    print(f"  Testing {total} combinations (regime: BULL_TREND + BEAR_TREND)...\n")

    results: list[SweepResult] = []

    for idx, (rsi_os, ema_dev, sl, tp) in enumerate(combos, 1):
        rsi_ob = 100.0 - rsi_os

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
            stop_loss_pct=sl,
            take_profit_pct=tp,
            min_confidence=0.55,
            slippage_pct=0.001,
            spread_pct=0.0005,
            order_type="limit",
            allowed_regimes=REGIME_FILTER,
        )

        try:
            result = run_backtest(cfg)
        except RuntimeError as e:
            print(f"  [{idx:3d}/{total}] ERROR: {e}")
            continue

        trades = result.trades
        n = len(trades)
        wins = sum(1 for t in trades if t.outcome == "WIN")
        wr = wins / n if n else 0.0

        gross_win = sum(t.pnl for t in trades if t.pnl > 0)
        gross_loss = abs(sum(t.pnl for t in trades if t.pnl < 0))
        pf = gross_win / gross_loss if gross_loss > 0 else 0.0
        total_pnl = sum(t.pnl for t in trades)
        max_dd = _max_drawdown(trades)

        results.append(SweepResult(
            rsi_os=rsi_os,
            rsi_ob=rsi_ob,
            ema_dev=ema_dev,
            stop_loss=sl,
            take_profit=tp,
            trades=n,
            win_rate=wr,
            profit_factor=pf,
            max_drawdown=max_dd,
            total_pnl=total_pnl,
        ))

        if idx % 20 == 0 or idx == total:
            print(f"  [{idx:3d}/{total}] completed...")

    return results


def print_top_results(results: list[SweepResult]) -> None:
    """Filter and print the top 15 results sorted by profit factor."""
    filtered = [
        r for r in results
        if r.trades >= 80
        and r.profit_factor >= 1.2
        and r.win_rate >= 0.60
        and r.max_drawdown < 15.0
    ]

    ranked = sorted(filtered, key=lambda r: r.profit_factor, reverse=True)
    top = ranked[:15]

    print("\n" + "=" * 95)
    print("  Top 15 — filtered: trades>=80, PF>=1.2, WR>=60%, MaxDD<15%")
    print("  Regime: BULL_TREND + BEAR_TREND only")
    print("=" * 95)
    print(f"  {'#':>2}  {'RSI_OS':>6} {'EMA%':>5} {'SL%':>5} {'TP%':>5}  "
          f"{'Trades':>6} {'WR%':>6} {'PF':>6} {'MaxDD%':>7} {'P&L':>9}")
    print("  " + "-" * 89)

    if not top:
        print("  No combinations met all filter criteria.")
        print(f"\n  Total tested: {len(results)}")
        # Show how many met partial criteria
        t80 = sum(1 for r in results if r.trades >= 80)
        pf12 = sum(1 for r in results if r.profit_factor >= 1.2)
        wr60 = sum(1 for r in results if r.win_rate >= 0.60)
        dd15 = sum(1 for r in results if r.max_drawdown < 15.0)
        print(f"  trades>=80: {t80}  |  PF>=1.2: {pf12}  |  WR>=60%: {wr60}  |  MaxDD<15%: {dd15}")
        return

    for i, r in enumerate(top, 1):
        print(f"  {i:2d}  {r.rsi_os:6.0f} {r.ema_dev:5.1f} {r.stop_loss*100:5.1f} {r.take_profit*100:5.1f}  "
              f"{r.trades:6d} {r.win_rate*100:5.1f}% {r.profit_factor:6.3f} "
              f"{r.max_drawdown:6.1f}% ${r.total_pnl:+8.2f}")

    print(f"\n  {len(filtered)} combinations passed all filters (of {len(results)} tested)")
    best = top[0]
    print(f"  Best: RSI {best.rsi_os}/{best.rsi_ob:.0f}, EMA {best.ema_dev}%, "
          f"SL {best.stop_loss*100:.0f}%, TP {best.take_profit*100:.0f}% "
          f"-> n={best.trades}, WR={best.win_rate*100:.1f}%, "
          f"PF={best.profit_factor:.3f}, MaxDD={best.max_drawdown:.1f}%")


def main() -> None:
    print("=" * 95)
    print("  Mean Reversion Parameter Sweep — Trend-Only Regime Filter")
    print("=" * 95)

    results = run_sweep()
    if results:
        print_top_results(results)


if __name__ == "__main__":
    main()
