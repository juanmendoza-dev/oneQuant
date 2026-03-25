"""Experiment runner — $250 capital test + inline validation.

Runs 3 strategies at $250 capital to test whether fees are the root
cause of failure. Strategies with PF > 1.2 get walk-forward +
Monte Carlo validation inline.

Usage:
    cd onequant/
    python run_experiments.py
"""

import math
import random
import sys

if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8")

from backtest.engine import BacktestConfig, _ts_range, run_backtest
from backtest.metrics import calculate_metrics
from backtest.report import print_report
from strategies.breakout import BreakoutStrategy
from strategies.mean_reversion import MeanReversionStrategy

CAPITAL = 250.0
TIMEFRAME = "15m"
MC_ITERATIONS = 10_000
NUM_BLOCKS = 4
PF_VALIDATION_THRESHOLD = 1.2


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _pf(trades):
    gw = sum(t.pnl for t in trades if t.pnl > 0)
    gl = abs(sum(t.pnl for t in trades if t.pnl < 0))
    return gw / gl if gl > 0 else 0.0


def _max_dd(trades):
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


# ---------------------------------------------------------------------------
# Walk-forward validation
# ---------------------------------------------------------------------------

def _run_wf(cfg_factory, boundaries, anchored):
    label = "Anchored" if anchored else "Sliding"
    periods = []
    for i in range(NUM_BLOCKS - 1):
        train_start = boundaries[0] if anchored else boundaries[i]
        train_end = boundaries[i + 1]
        test_start = boundaries[i + 1]
        test_end = boundaries[i + 2]

        print(f"    {label} [{i+1}]: train...", end="", flush=True)
        train_cfg = cfg_factory(train_start, train_end)
        train_r = run_backtest(train_cfg)
        train_m = calculate_metrics(train_r)

        print(" test...", end="", flush=True)
        test_cfg = cfg_factory(test_start, test_end)
        test_r = run_backtest(test_cfg)
        test_m = calculate_metrics(test_r)
        print(" done")

        periods.append((
            i + 1,
            train_m.win_rate, train_m.profit_factor, train_m.total_trades,
            test_m.win_rate, test_m.profit_factor, test_m.total_trades,
        ))
    return periods


def _print_wf(periods, label):
    print(f"\n  {label} Walk-Forward:")
    print("  " + "─" * 72)
    for p in periods:
        i, twr, tpf, tn, swr, spf, sn = p
        tag = "✓" if spf >= 1.0 else "✗"
        print(f"    Period {i}: Train WR={twr*100:.1f}% PF={tpf:.2f} n={tn}"
              f"  │  OOS  WR={swr*100:.1f}% PF={spf:.2f} n={sn}  {tag}")
    oos_pfs = [min(p[5], 10.0) for p in periods]
    avg = sum(oos_pfs) / len(oos_pfs)
    verdict = "PASS" if avg > 1.0 else "FAIL"
    print(f"  Avg OOS PF: {avg:.2f}  →  {verdict}")
    return avg


def run_walk_forward(name, cfg_factory):
    print(f"\n  Walk-Forward — {name}")
    min_ts, max_ts = _ts_range(TIMEFRAME)
    block = (max_ts - min_ts) // NUM_BLOCKS
    boundaries = [min_ts + i * block for i in range(NUM_BLOCKS + 1)]
    boundaries[-1] = max_ts

    sliding = _run_wf(cfg_factory, boundaries, anchored=False)
    avg_sliding = _print_wf(sliding, "Sliding")

    anchored = _run_wf(cfg_factory, boundaries, anchored=True)
    avg_anchored = _print_wf(anchored, "Anchored")

    passed = avg_sliding > 1.0 and avg_anchored > 1.0
    print(f"  Walk-Forward VERDICT: {'PASS ✓' if passed else 'FAIL ✗'}")
    return passed


# ---------------------------------------------------------------------------
# Monte Carlo
# ---------------------------------------------------------------------------

def run_monte_carlo(name, trades):
    print(f"\n  Monte Carlo — {name}  ({MC_ITERATIONS:,} simulations)")
    if not trades:
        print("  No trades.")
        return False

    pnl_pcts = [t.pnl_pct for t in trades]
    profitable_count = 0
    final_pnls = []
    rng = random.Random(42)

    for _ in range(MC_ITERATIONS):
        shuffled = pnl_pcts[:]
        rng.shuffle(shuffled)
        equity = CAPITAL
        for pct in shuffled:
            equity += equity * 0.10 * pct
        final_pnls.append(equity - CAPITAL)
        if equity > CAPITAL:
            profitable_count += 1

    pct_prof = profitable_count / MC_ITERATIONS
    avg_pnl = sum(final_pnls) / len(final_pnls)
    worst = min(final_pnls)
    best = max(final_pnls)

    passed = pct_prof > 0.60
    print(f"  Profitable runs:  {profitable_count:,}/{MC_ITERATIONS:,} ({pct_prof*100:.1f}%)")
    print(f"  Avg P&L: ${avg_pnl:+.2f}  |  Worst: ${worst:.2f}  |  Best: ${best:+.2f}")
    print(f"  Monte Carlo VERDICT: {'PASS ✓' if passed else 'FAIL ✗'}")
    return passed


# ---------------------------------------------------------------------------
# Validation runner
# ---------------------------------------------------------------------------

def validate_strategy(name, cfg_factory, trades):
    print("\n" + "═" * 60)
    print(f"  VALIDATION — {name}")
    print("═" * 60)
    wf_pass = run_walk_forward(name, cfg_factory)
    mc_pass = run_monte_carlo(name, trades)
    overall = wf_pass and mc_pass
    print(f"\n  ┌─────────────────────────────────┐")
    print(f"  │  Walk-Forward : {'PASS ✓' if wf_pass else 'FAIL ✗':<13}     │")
    print(f"  │  Monte Carlo  : {'PASS ✓' if mc_pass else 'FAIL ✗':<13}     │")
    print(f"  │  OVERALL      : {'VALIDATED ✓' if overall else 'NOT VALIDATED ✗':<13} │")
    print(f"  └─────────────────────────────────┘")
    return overall


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    print("═" * 60)
    print(f"  oneQuant — $250 Capital Test")
    print(f"  Testing whether fees are the root cause of failure")
    print("═" * 60)

    strategies_to_validate = []

    # ── 1. Mean Reversion (trend-only) ───────────────────────────────────────
    print(f"\nRunning: Mean Reversion (trend-only) at ${CAPITAL:.0f}...")
    mr_cfg = BacktestConfig(
        strategy=MeanReversionStrategy(),
        timeframe=TIMEFRAME,
        initial_capital=CAPITAL,
        position_size_pct=0.10,
        stop_loss_pct=0.06,
        take_profit_pct=0.04,
        min_confidence=0.55,
        slippage_pct=0.001,
        spread_pct=0.0005,
        order_type="limit",
        allowed_regimes=["BULL_TREND", "BEAR_TREND"],
    )
    mr_result = run_backtest(mr_cfg)
    mr_result.strategy_name = "Mean Reversion (trend only)"
    mr_m = print_report(mr_result)

    if mr_m.profit_factor >= PF_VALIDATION_THRESHOLD:
        def mr_factory(start, end):
            return BacktestConfig(
                strategy=MeanReversionStrategy(),
                timeframe=TIMEFRAME,
                initial_capital=CAPITAL,
                position_size_pct=0.10,
                stop_loss_pct=0.06,
                take_profit_pct=0.04,
                min_confidence=0.55,
                slippage_pct=0.001,
                spread_pct=0.0005,
                order_type="limit",
                allowed_regimes=["BULL_TREND", "BEAR_TREND"],
                start_ts=start,
                end_ts=end,
            )
        strategies_to_validate.append(("Mean Reversion (trend only)", mr_factory, mr_result.trades))

    # ── 2. Breakout — no regime filter ────────────────────────────────────────
    print(f"\nRunning: Breakout (no regime filter) at ${CAPITAL:.0f}...")
    bo_cfg = BacktestConfig(
        strategy=BreakoutStrategy(),
        timeframe=TIMEFRAME,
        initial_capital=CAPITAL,
        position_size_pct=0.10,
        stop_loss_pct=0.03,
        take_profit_pct=0.04,
        min_confidence=0.65,
        slippage_pct=0.001,
        spread_pct=0.0005,
        order_type="limit",
    )
    bo_result = run_backtest(bo_cfg)
    bo_result.strategy_name = "Breakout (all regimes)"
    bo_m = print_report(bo_result)

    if bo_m.profit_factor >= PF_VALIDATION_THRESHOLD:
        def bo_factory(start, end):
            return BacktestConfig(
                strategy=BreakoutStrategy(),
                timeframe=TIMEFRAME,
                initial_capital=CAPITAL,
                position_size_pct=0.10,
                stop_loss_pct=0.03,
                take_profit_pct=0.04,
                min_confidence=0.65,
                slippage_pct=0.001,
                spread_pct=0.0005,
                order_type="limit",
                start_ts=start,
                end_ts=end,
            )
        strategies_to_validate.append(("Breakout (all regimes)", bo_factory, bo_result.trades))

    # ── 3. Breakout — RANGING only ────────────────────────────────────────────
    print(f"\nRunning: Breakout (RANGING only) at ${CAPITAL:.0f}...")
    bor_cfg = BacktestConfig(
        strategy=BreakoutStrategy(),
        timeframe=TIMEFRAME,
        initial_capital=CAPITAL,
        position_size_pct=0.10,
        stop_loss_pct=0.03,
        take_profit_pct=0.04,
        min_confidence=0.65,
        slippage_pct=0.001,
        spread_pct=0.0005,
        order_type="limit",
        allowed_regimes=["RANGING"],
    )
    bor_result = run_backtest(bor_cfg)
    bor_result.strategy_name = "Breakout (RANGING only)"
    bor_m = print_report(bor_result)

    if bor_m.profit_factor >= PF_VALIDATION_THRESHOLD:
        def bor_factory(start, end):
            return BacktestConfig(
                strategy=BreakoutStrategy(),
                timeframe=TIMEFRAME,
                initial_capital=CAPITAL,
                position_size_pct=0.10,
                stop_loss_pct=0.03,
                take_profit_pct=0.04,
                min_confidence=0.65,
                slippage_pct=0.001,
                spread_pct=0.0005,
                order_type="limit",
                allowed_regimes=["RANGING"],
                start_ts=start,
                end_ts=end,
            )
        strategies_to_validate.append(("Breakout (RANGING only)", bor_factory, bor_result.trades))

    # ── Summary ───────────────────────────────────────────────────────────────
    print("\n" + "═" * 60)
    print("  $250 Capital Test — Summary")
    print("═" * 60)
    for name, m in [
        ("Mean Reversion (trend only)", mr_m),
        ("Breakout (all regimes)", bo_m),
        ("Breakout (RANGING only)", bor_m),
    ]:
        tag = " ← WILL VALIDATE" if m.profit_factor >= PF_VALIDATION_THRESHOLD else ""
        print(f"  {name:<28}  n={m.total_trades:>4}  WR={m.win_rate*100:5.1f}%"
              f"  PF={m.profit_factor:.3f}  DD={m.max_drawdown:.1f}%"
              f"  P&L=${m.total_pnl:+.2f}{tag}")

    # ── Validation for qualifying strategies ──────────────────────────────────
    if not strategies_to_validate:
        print("\n  No strategies passed PF >= 1.2 — nothing to validate.")
        print("  Conclusion: strategies are fundamentally broken, not just fee-limited.")
    else:
        print(f"\n  {len(strategies_to_validate)} strategy/strategies qualify for validation (PF >= 1.2)")
        for name, factory, trades in strategies_to_validate:
            validate_strategy(name, factory, trades)


if __name__ == "__main__":
    main()
