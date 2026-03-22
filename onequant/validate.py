"""oneQuant strategy validation suite.

Four stress tests for the Mean Reversion strategy:
  1. Walk-forward validation — sliding AND anchored (4 × 6-month blocks)
  2. Monte Carlo simulation (10,000 shuffled equity curves)
  3. Parameter stability sweep (1,500 combinations, parallelised)
  4. Sharpe significance t-test

Usage:
    cd onequant/
    python validate.py
"""

import math
import multiprocessing
import os
import random
import sys
import time
from collections import Counter
from dataclasses import dataclass

from backtest.engine import BacktestConfig, _ts_range, run_backtest
from backtest.metrics import Metrics, calculate_metrics
from strategies.mean_reversion import MeanReversionStrategy

# ---------------------------------------------------------------------------
# Ensure UTF-8 output on Windows
# ---------------------------------------------------------------------------

if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8")

# ---------------------------------------------------------------------------
# Shared config — matches run_backtest.py production settings
# ---------------------------------------------------------------------------

TIMEFRAME: str = "15m"
STOP_LOSS_PCT: float = 0.06
TAKE_PROFIT_PCT: float = 0.03
INITIAL_CAPITAL: float = 25.0
NUM_BLOCKS: int = 4
MC_ITERATIONS: int = 10_000

# Minimum trade gates
MIN_TOTAL_TRADES: int = 100
MIN_PERIOD_TRADES: int = 30

# ---------------------------------------------------------------------------
# Test 1 — Walk-forward validation (sliding + anchored)
# ---------------------------------------------------------------------------


@dataclass
class WalkForwardPeriod:
    """Results for one train-test pair."""

    period: int
    train_wr: float
    train_pf: float
    train_pnl: float
    train_trades: int
    test_wr: float
    test_pf: float
    test_pnl: float
    test_trades: int


def _run_block(start_ts: int, end_ts: int) -> Metrics:
    """Run mean reversion backtest on a specific time range."""
    cfg = BacktestConfig(
        strategy=MeanReversionStrategy(),
        timeframe=TIMEFRAME,
        start_ts=start_ts,
        end_ts=end_ts,
        stop_loss_pct=STOP_LOSS_PCT,
        take_profit_pct=TAKE_PROFIT_PCT,
        initial_capital=INITIAL_CAPITAL,
        order_type="limit",
    )
    result = run_backtest(cfg)
    return calculate_metrics(result)


def _run_walk_forward(boundaries: list[int], anchored: bool) -> list[WalkForwardPeriod]:
    """Run walk-forward validation. If anchored, training always starts from day 1."""
    periods: list[WalkForwardPeriod] = []
    label = "Anchored" if anchored else "Sliding"

    for i in range(NUM_BLOCKS - 1):
        train_start = boundaries[0] if anchored else boundaries[i]
        train_end = boundaries[i + 1]
        test_start = boundaries[i + 1]
        test_end = boundaries[i + 2]

        print(f"  {label} Period {i + 1}: training ... ", end="", flush=True)
        train_m = _run_block(train_start, train_end)
        print("testing ... ", end="", flush=True)
        test_m = _run_block(test_start, test_end)
        print("done")

        periods.append(WalkForwardPeriod(
            period=i + 1,
            train_wr=train_m.win_rate,
            train_pf=train_m.profit_factor,
            train_pnl=train_m.total_pnl,
            train_trades=train_m.total_trades,
            test_wr=test_m.win_rate,
            test_pf=test_m.profit_factor,
            test_pnl=test_m.total_pnl,
            test_trades=test_m.total_trades,
        ))
    return periods


def _print_wf_table(periods: list[WalkForwardPeriod], label: str) -> float:
    """Print walk-forward results and return average OOS profit factor."""
    print(f"\n  {label} Walk-Forward Results")
    print("  " + "\u2550" * 55)
    for p in periods:
        check = "\u2713" if p.test_pf >= 1.0 else "\u2717"
        train_str = f"Train {p.train_wr * 100:5.1f}% WR (PF={min(p.train_pf, 99.9):.2f}, n={p.train_trades})"
        test_str = f"Test {p.test_wr * 100:5.1f}% WR (PF={min(p.test_pf, 99.9):.2f}, n={p.test_trades})"
        print(f"  Period {p.period}:  {train_str}  \u2502  {test_str}  {check}")
    print("  " + "\u2500" * 55)

    oos_pfs = [min(p.test_pf, 10.0) for p in periods]  # cap inf at 10.0
    avg_oos_pf = sum(oos_pfs) / len(oos_pfs) if oos_pfs else 0.0
    oos_wrs = [p.test_wr for p in periods]
    avg_oos_wr = sum(oos_wrs) / len(oos_wrs) if oos_wrs else 0.0
    print(f"  Avg OOS WR: {avg_oos_wr * 100:.1f}%   Avg OOS PF: {avg_oos_pf:.2f}")
    return avg_oos_pf


def test_walk_forward() -> bool:
    """Run both sliding and anchored walk-forward validation.

    Returns True if BOTH average out-of-sample profit factors > 1.0
    AND total trades meet minimum threshold.
    """
    print("\n" + "\u2550" * 60)
    print("  Test 1 \u2014 Walk-Forward Validation (Sliding + Anchored)")
    print("\u2550" * 60)

    min_ts, max_ts = _ts_range(TIMEFRAME)
    total_span = max_ts - min_ts
    block_size = total_span // NUM_BLOCKS

    boundaries: list[int] = []
    for i in range(NUM_BLOCKS + 1):
        boundaries.append(min_ts + i * block_size)
    boundaries[-1] = max_ts

    # Check total trade count first
    full_m = _run_block(min_ts, max_ts)
    total_trades = full_m.total_trades

    if total_trades < MIN_TOTAL_TRADES:
        print(f"\n  INSUFFICIENT TRADES: {total_trades} trades found, "
              f"minimum {MIN_TOTAL_TRADES} required for validation.")
        print("  Loosen entry conditions or extend history.")
        return False

    # Sliding walk-forward
    sliding_periods = _run_walk_forward(boundaries, anchored=False)
    avg_sliding_pf = _print_wf_table(sliding_periods, "Sliding")

    # Anchored walk-forward
    anchored_periods = _run_walk_forward(boundaries, anchored=True)
    avg_anchored_pf = _print_wf_table(anchored_periods, "Anchored")

    # Check per-period minimums
    all_periods = sliding_periods + anchored_periods
    low_count_periods = [p for p in all_periods if p.test_trades < MIN_PERIOD_TRADES]
    if low_count_periods:
        print(f"\n  WARNING: {len(low_count_periods)} period(s) have fewer than "
              f"{MIN_PERIOD_TRADES} test trades")

    consistent = avg_sliding_pf > 1.0 and avg_anchored_pf > 1.0
    print(f"\n  Sliding avg PF:  {avg_sliding_pf:.2f}  {'PASS' if avg_sliding_pf > 1.0 else 'FAIL'}")
    print(f"  Anchored avg PF: {avg_anchored_pf:.2f}  {'PASS' if avg_anchored_pf > 1.0 else 'FAIL'}")
    print(f"  Consistent: {'YES' if consistent else 'NO'}")

    return consistent


# ---------------------------------------------------------------------------
# Test 2 — Monte Carlo simulation
# ---------------------------------------------------------------------------


def test_monte_carlo() -> bool:
    """Shuffle trade order 10,000 times to test robustness.

    Returns True if > 60% of simulations are profitable.
    """
    print("\n" + "\u2550" * 60)
    print("  Test 2 \u2014 Monte Carlo Simulation")
    print("\u2550" * 60)

    cfg = BacktestConfig(
        strategy=MeanReversionStrategy(),
        timeframe=TIMEFRAME,
        stop_loss_pct=STOP_LOSS_PCT,
        take_profit_pct=TAKE_PROFIT_PCT,
        initial_capital=INITIAL_CAPITAL,
        order_type="limit",
    )
    result = run_backtest(cfg)
    trades = result.trades

    if not trades:
        print("  No trades to simulate.")
        return False

    print(f"  Source trades: {len(trades)}")
    print(f"  Running {MC_ITERATIONS:,} simulations ... ", end="", flush=True)

    trade_pnl_pcts = [t.pnl_pct for t in trades]
    position_size_pct = 0.10
    profitable_count = 0
    final_pnls: list[float] = []
    max_drawdowns: list[float] = []

    rng = random.Random(42)

    for _ in range(MC_ITERATIONS):
        shuffled = trade_pnl_pcts[:]
        rng.shuffle(shuffled)

        equity = INITIAL_CAPITAL
        peak = equity
        max_dd = 0.0

        for pct in shuffled:
            trade_pnl = equity * position_size_pct * pct
            equity += trade_pnl
            if equity > peak:
                peak = equity
            dd = (peak - equity) / peak if peak > 0 else 0.0
            if dd > max_dd:
                max_dd = dd

        final_pnl = equity - INITIAL_CAPITAL
        final_pnls.append(final_pnl)
        max_drawdowns.append(max_dd * 100.0)
        if final_pnl > 0:
            profitable_count += 1

    print("done\n")

    pct_profitable = profitable_count / MC_ITERATIONS
    avg_pnl = sum(final_pnls) / len(final_pnls)
    worst_pnl = min(final_pnls)
    best_pnl = max(final_pnls)
    avg_dd = sum(max_drawdowns) / len(max_drawdowns)
    worst_dd = max(max_drawdowns)

    robust = pct_profitable > 0.60

    print(f"  Monte Carlo \u2014 {MC_ITERATIONS:,} simulations")
    print("  " + "\u2550" * 47)
    print(f"  Profitable runs:     {profitable_count:,} / {MC_ITERATIONS:,} ({pct_profitable * 100:.1f}%)")
    print(f"  Avg final P&L:       {'+' if avg_pnl >= 0 else ''}${avg_pnl:.2f}")
    print(f"  Worst case P&L:      ${worst_pnl:.2f}")
    print(f"  Best case P&L:       {'+' if best_pnl >= 0 else ''}${best_pnl:.2f}")
    print(f"  Avg max drawdown:    -{avg_dd:.1f}%")
    print(f"  Worst max drawdown:  -{worst_dd:.1f}%")
    print("  " + "\u2500" * 47)
    print(f"  VERDICT: {'ROBUST' if robust else 'NOT ROBUST'}")

    return robust


# ---------------------------------------------------------------------------
# Test 3 — Parameter stability sweep
# ---------------------------------------------------------------------------

RSI_OVERSOLD_RANGE: list[float] = [15, 18, 20, 22, 25]
RSI_OVERBOUGHT_RANGE: list[float] = [75, 78, 80, 82, 85]
EMA_DEV_RANGE: list[float] = [2.0, 2.5, 3.0, 3.5, 4.0]
SL_RANGE: list[float] = [0.04, 0.05, 0.06, 0.07]
TP_RANGE: list[float] = [0.02, 0.03, 0.04]


@dataclass
class SweepResult:
    """Result of one parameter combination."""

    rsi_os: float
    rsi_ob: float
    ema_dev: float
    sl: float
    tp: float
    trades: int
    win_rate: float
    profit_factor: float
    pnl: float


def _run_sweep_combo(args: tuple) -> SweepResult:
    """Worker function for one parameter combination (runs in subprocess)."""
    rsi_os, rsi_ob, ema_dev, sl, tp = args

    strategy = MeanReversionStrategy()

    import strategies.mean_reversion as mr
    original_os = mr.RSI_OVERSOLD
    original_ob = mr.RSI_OVERBOUGHT
    original_dev = mr.EMA_DEVIATION_PCT
    try:
        mr.RSI_OVERSOLD = rsi_os
        mr.RSI_OVERBOUGHT = rsi_ob
        mr.EMA_DEVIATION_PCT = ema_dev

        cfg = BacktestConfig(
            strategy=strategy,
            timeframe=TIMEFRAME,
            stop_loss_pct=sl,
            take_profit_pct=tp,
            initial_capital=INITIAL_CAPITAL,
            order_type="limit",
        )
        result = run_backtest(cfg)
        m = calculate_metrics(result)
    finally:
        mr.RSI_OVERSOLD = original_os
        mr.RSI_OVERBOUGHT = original_ob
        mr.EMA_DEVIATION_PCT = original_dev

    return SweepResult(
        rsi_os=rsi_os,
        rsi_ob=rsi_ob,
        ema_dev=ema_dev,
        sl=sl * 100,
        tp=tp * 100,
        trades=m.total_trades,
        win_rate=m.win_rate,
        profit_factor=m.profit_factor,
        pnl=m.total_pnl,
    )


def test_parameter_stability() -> bool:
    """Sweep 1,500 parameter combinations and check stability.

    Returns True if >15% of combinations are profitable AND current
    settings appear in the top quartile by profit factor.
    """
    print("\n" + "\u2550" * 60)
    print("  Test 3 \u2014 Parameter Stability Sweep")
    print("\u2550" * 60)

    combos: list[tuple] = []
    for rsi_os in RSI_OVERSOLD_RANGE:
        for rsi_ob in RSI_OVERBOUGHT_RANGE:
            for ema_dev in EMA_DEV_RANGE:
                for sl in SL_RANGE:
                    for tp in TP_RANGE:
                        combos.append((rsi_os, rsi_ob, ema_dev, sl, tp))

    total = len(combos)
    print(f"  Combinations: {total}")
    cores = os.cpu_count() or 4
    print(f"  Using {cores} CPU cores")

    start = time.time()

    with multiprocessing.Pool(processes=cores) as pool:
        results: list[SweepResult] = []
        done = 0
        for sr in pool.imap_unordered(_run_sweep_combo, combos, chunksize=20):
            results.append(sr)
            done += 1
            if done % 100 == 0 or done == total:
                elapsed = time.time() - start
                rate = done / elapsed if elapsed > 0 else 0
                eta = (total - done) / rate if rate > 0 else 0
                print(
                    f"\r  Progress: {done}/{total} "
                    f"({done * 100 / total:.0f}%) "
                    f"[{elapsed:.0f}s elapsed, ~{eta:.0f}s remaining]",
                    end="", flush=True,
                )

    elapsed = time.time() - start
    print(f"\n  Completed in {elapsed:.1f}s\n")

    profitable = [r for r in results if r.profit_factor > 1.0 and r.trades > 0]
    pct_profitable = len(profitable) / total * 100 if total > 0 else 0.0

    results_sorted = sorted(results, key=lambda r: r.profit_factor, reverse=True)

    print(f"  Top 20 by Profit Factor")
    print(f"  {'RSI_OS':<7} {'RSI_OB':<7} {'EMA%':<6} {'SL%':<5} {'TP%':<5} "
          f"{'Tr':<5} {'WR%':<7} {'PF':<6} {'P&L':<10}")
    print("  " + "-" * 63)
    for r in results_sorted[:20]:
        pf_str = f"{r.profit_factor:<6.2f}" if r.profit_factor < 1000 else "inf   "
        print(
            f"  {r.rsi_os:<7.0f} {r.rsi_ob:<7.0f} {r.ema_dev:<6.1f} {r.sl:<5.1f} {r.tp:<5.1f} "
            f"{r.trades:<5} {r.win_rate * 100:<7.1f} {pf_str} {r.pnl:<10.2f}"
        )

    print(f"\n  Parameter Stability Analysis")
    print("  " + "\u2550" * 47)
    print(f"  Total combinations tested:   {total}")
    print(f"  Profitable combinations:     {len(profitable)} ({pct_profitable:.1f}%)")

    if profitable:
        os_counts = Counter(r.rsi_os for r in profitable)
        dev_counts = Counter(r.ema_dev for r in profitable)
        sl_counts = Counter(r.sl for r in profitable)
        tp_counts = Counter(r.tp for r in profitable)

        best_os = os_counts.most_common(1)[0]
        best_dev = dev_counts.most_common(1)[0]
        best_sl = sl_counts.most_common(1)[0]
        best_tp = tp_counts.most_common(1)[0]

        n_prof = len(profitable)
        print()
        print(f"  Most robust RSI oversold:    {best_os[0]:.0f} "
              f"(appears in {best_os[1] * 100 / n_prof:.0f}% of profitable runs)")
        print(f"  Most robust EMA deviation:   {best_dev[0]:.1f}%")
        print(f"  Most robust stop loss:       {best_sl[0]:.1f}%")
        print(f"  Most robust take profit:     {best_tp[0]:.1f}%")

    top_quartile_cutoff = len(results_sorted) // 4
    top_quartile = results_sorted[:top_quartile_cutoff]
    current_in_top_q = any(
        r.rsi_os == 20 and r.ema_dev == 3.0 and r.sl == 6.0 and r.tp == 3.0
        for r in top_quartile
    )

    stable = pct_profitable > 15.0 and current_in_top_q
    print(f"\n  Current settings in top quartile: {'YES' if current_in_top_q else 'NO'}")
    print(f"  Stability verdict: {'STABLE' if stable else 'NOT STABLE'}")

    return stable


# ---------------------------------------------------------------------------
# Test 4 — Sharpe ratio significance test
# ---------------------------------------------------------------------------


def _t_cdf(t_val: float, df: int) -> float:
    """CDF of Student's t-distribution via numerical integration (Simpson's rule).

    Avoids scipy dependency while providing reliable results.
    """
    if df <= 0:
        return 0.5

    # PDF: C * (1 + x^2/df)^(-(df+1)/2)
    log_c = (math.lgamma((df + 1) / 2) - math.lgamma(df / 2)
             - 0.5 * math.log(df * math.pi))
    c = math.exp(log_c)

    def pdf(x: float) -> float:
        return c * (1.0 + x * x / df) ** (-(df + 1) / 2.0)

    # Integrate from 0 to |t| using composite Simpson's rule
    abs_t = abs(t_val)
    n_steps = 2000  # even number for Simpson
    h = abs_t / n_steps

    # Simpson's rule: integral ≈ (h/3) * [f(0) + 4f(h) + 2f(2h) + 4f(3h) + ... + f(nh)]
    integral = pdf(0.0) + pdf(abs_t)
    for i in range(1, n_steps):
        x = i * h
        if i % 2 == 0:
            integral += 2.0 * pdf(x)
        else:
            integral += 4.0 * pdf(x)
    integral *= h / 3.0

    # CDF by symmetry: P(T <= t) = 0.5 + integral(0, t) for t >= 0
    if t_val >= 0:
        return 0.5 + integral
    else:
        return 0.5 - integral


def test_sharpe_significance() -> bool:
    """Test if the strategy's Sharpe ratio is statistically significant.

    Uses a t-test: t_stat = sharpe * sqrt(n_trades)
    Returns True if p < 0.05.
    """
    print("\n" + "\u2550" * 60)
    print("  Test 4 \u2014 Sharpe Ratio Significance")
    print("\u2550" * 60)

    cfg = BacktestConfig(
        strategy=MeanReversionStrategy(),
        timeframe=TIMEFRAME,
        stop_loss_pct=STOP_LOSS_PCT,
        take_profit_pct=TAKE_PROFIT_PCT,
        initial_capital=INITIAL_CAPITAL,
        order_type="limit",
    )
    result = run_backtest(cfg)
    m = calculate_metrics(result)

    n = m.total_trades
    sharpe = m.sharpe_ratio

    if n < 2:
        print("  Not enough trades for significance test.")
        return False

    t_stat = sharpe * math.sqrt(n)
    df = n - 1
    p_value = 2.0 * (1.0 - _t_cdf(abs(t_stat), df))

    significant = p_value < 0.05

    print(f"  Trades:      {n}")
    print(f"  Sharpe:      {sharpe:.2f}")
    print(f"  t-statistic: {t_stat:.3f}")
    print(f"  p-value:     {p_value:.4f}")

    if significant:
        print(f"  Result:      Sharpe {sharpe:.2f} (p={p_value:.3f}, statistically significant)")
    else:
        print(f"  Result:      Sharpe {sharpe:.2f} (p={p_value:.3f}, NOT significant \u2014 need more trades)")

    return significant


# ---------------------------------------------------------------------------
# Main — run all tests and print final summary
# ---------------------------------------------------------------------------

BOX_WIDTH: int = 50


def main() -> None:
    """Run all validation tests and print the final verdict."""
    print("=" * 60)
    print("oneQuant v0.3 \u2014 Strategy Validation Suite")
    print("=" * 60)

    wf_pass = test_walk_forward()
    mc_pass = test_monte_carlo()
    ps_pass = test_parameter_stability()
    sh_pass = test_sharpe_significance()

    overall = wf_pass and mc_pass and ps_pass and sh_pass

    top = "\u2554" + "\u2550" * BOX_WIDTH + "\u2557"
    mid = "\u2560" + "\u2550" * BOX_WIDTH + "\u2563"
    bot = "\u255a" + "\u2550" * BOX_WIDTH + "\u255d"

    def _row(label: str, passed: bool) -> str:
        status = "PASS" if passed else "FAIL"
        inner = f" {label:<26}{status:>{BOX_WIDTH - 29}} "
        return f"\u2551{inner}\u2551"

    print("\n" + top)
    print(f"\u2551{'oneQuant Strategy Validation':^{BOX_WIDTH}}\u2551")
    print(mid)
    print(_row("Walk-forward:", wf_pass))
    print(_row("Monte Carlo:", mc_pass))
    print(_row("Parameter stability:", ps_pass))
    print(_row("Sharpe significance:", sh_pass))
    print(mid)
    verdict = "VALIDATED" if overall else "NOT VALIDATED"
    print(f"\u2551{f' OVERALL: {verdict} ':^{BOX_WIDTH}}\u2551")
    print(bot)
    print()


if __name__ == "__main__":
    multiprocessing.freeze_support()
    main()
