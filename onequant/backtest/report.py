"""Formatted console report for backtest results."""

import sys

from backtest.engine import BacktestResult
from backtest.metrics import Metrics, calculate_metrics


def _ensure_utf8() -> None:
    """Reconfigure stdout to UTF-8 so box-drawing characters render on Windows."""
    if hasattr(sys.stdout, "reconfigure"):
        sys.stdout.reconfigure(encoding="utf-8")  # type: ignore[union-attr]


_ensure_utf8()

BOX_WIDTH: int = 52


def _line(label: str, value: str) -> str:
    """Format a single row inside the box."""
    inner = f" {label:<22}{value:>{BOX_WIDTH - 26}} "
    return f"\u2551{inner}\u2551"


def print_report(result: BacktestResult) -> Metrics:
    """Print a formatted backtest report to console and return the metrics."""
    m = calculate_metrics(result)

    period_str = (
        f"{result.start_date.strftime('%Y-%m-%d')} \u2192 "
        f"{result.end_date.strftime('%Y-%m-%d')}"
    )
    pnl_sign = "+" if m.total_pnl >= 0 else ""
    verdict = "PROFITABLE \u2713" if m.is_profitable() else "NOT PROFITABLE \u2717"

    top = "\u2554" + "\u2550" * BOX_WIDTH + "\u2557"
    mid = "\u2560" + "\u2550" * BOX_WIDTH + "\u2563"
    bot = "\u255a" + "\u2550" * BOX_WIDTH + "\u255d"

    print()
    print(top)
    print(f"\u2551{'oneQuant Backtest Report':^{BOX_WIDTH}}\u2551")
    print(mid)
    print(_line("Strategy:", result.strategy_name))
    print(_line("Timeframe:", result.timeframe))
    print(_line("Period:", period_str))
    print(_line("Capital:", f"${result.initial_capital:,.2f}"))
    print(mid)
    print(_line("Total trades:", str(m.total_trades)))
    print(_line("Win rate:", f"{m.win_rate * 100:.1f}%"))
    print(_line("Profit factor:", f"{m.profit_factor:.2f}"))
    print(_line("Total P&L:", f"{pnl_sign}${m.total_pnl:.2f}"))
    print(_line("Total P&L %:", f"{pnl_sign}{m.total_pnl_pct:.1f}%"))
    print(_line("Max drawdown:", f"-{m.max_drawdown:.1f}%"))
    print(_line("Sharpe ratio:", f"{m.sharpe_ratio:.2f}"))
    print(_line("Avg confidence:", f"{m.avg_confidence:.2f}"))
    print(_line("Trades/week:", f"{m.trades_per_week:.1f}"))
    print(_line("Best trade:", f"${m.best_trade:.2f}"))
    print(_line("Worst trade:", f"${m.worst_trade:.2f}"))

    # Cost breakdown
    print(mid)
    print(f"\u2551{'Cost Breakdown':^{BOX_WIDTH}}\u2551")
    print(mid)
    print(_line("Total fees:", f"${m.total_fees:.4f}"))
    print(_line("Avg fee %:", f"{m.avg_fee_pct * 100:.3f}%"))
    print(_line("Total slippage:", f"${m.total_slippage:.4f}"))
    print(_line("Total spread:", f"${m.total_spread_cost:.4f}"))
    print(_line("Gap stops:", str(m.gap_stops)))
    if m.fee_tier_breakdown:
        tier_str = ", ".join(f"{k}:{v}" for k, v in sorted(m.fee_tier_breakdown.items()))
        print(_line("Fee tiers:", tier_str))

    # Regime breakdown
    if m.trades_by_regime:
        print(mid)
        print(f"\u2551{'Regime Breakdown':^{BOX_WIDTH}}\u2551")
        print(mid)
        for regime in sorted(m.trades_by_regime.keys()):
            n = m.trades_by_regime[regime]
            wr = m.win_rate_by_regime.get(regime, 0.0)
            pf = m.pf_by_regime.get(regime, 0.0)
            pf_str = f"{pf:.2f}" if pf != float("inf") else "inf"
            print(_line(f"{regime}:", f"n={n} WR={wr * 100:.0f}% PF={pf_str}"))

    # Buy-and-hold benchmark
    print(mid)
    print(f"\u2551{'Buy & Hold Benchmark':^{BOX_WIDTH}}\u2551")
    print(mid)
    bh_sign = "+" if m.bh_return_pct >= 0 else ""
    print(_line("BH return:", f"{bh_sign}{m.bh_return_pct:.1f}%"))
    print(_line("BH max drawdown:", f"-{m.bh_max_drawdown:.1f}%"))
    print(_line("BH Sharpe:", f"{m.bh_sharpe:.2f}"))
    alpha_sign = "+" if m.alpha_vs_bh >= 0 else ""
    print(_line("Alpha vs BH:", f"{alpha_sign}{m.alpha_vs_bh:.1f}%"))
    sharpe_diff = m.sharpe_ratio - m.bh_sharpe
    sdiff_sign = "+" if sharpe_diff >= 0 else ""
    print(_line("Sharpe edge:", f"{sdiff_sign}{sharpe_diff:.2f}"))

    print(mid)
    print(f"\u2551{f' VERDICT: {verdict} ':^{BOX_WIDTH}}\u2551")
    print(bot)
    print()

    return m


def print_comparison(results: list[tuple[str, Metrics]]) -> None:
    """Print a side-by-side comparison of strategy performance."""
    if not results:
        return

    top = "\u2554" + "\u2550" * BOX_WIDTH + "\u2557"
    mid = "\u2560" + "\u2550" * BOX_WIDTH + "\u2563"
    bot = "\u255a" + "\u2550" * BOX_WIDTH + "\u255d"

    print(top)
    print(f"\u2551{'Strategy Comparison':^{BOX_WIDTH}}\u2551")
    print(mid)

    for name, m in results:
        pnl_sign = "+" if m.total_pnl >= 0 else ""
        tag = " \u2713" if m.is_profitable() else ""
        line = f" {name:<18} PF={m.profit_factor:<6.2f} {pnl_sign}${m.total_pnl:.2f}{tag}"
        print(f"\u2551{line:<{BOX_WIDTH}}\u2551")

    print(mid)
    best = max(results, key=lambda x: x[1].profit_factor)
    winner_line = f" Best: {best[0]} (PF={best[1].profit_factor:.2f})"
    print(f"\u2551{winner_line:<{BOX_WIDTH}}\u2551")
    print(bot)
    print()
