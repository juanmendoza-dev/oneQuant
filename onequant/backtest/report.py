"""Formatted console report for backtest results."""

import io
import sys

from backtest.engine import BacktestResult
from backtest.metrics import Metrics, calculate_metrics


def _ensure_utf8() -> None:
    """Reconfigure stdout to UTF-8 so box-drawing characters render on Windows."""
    if hasattr(sys.stdout, "reconfigure"):
        sys.stdout.reconfigure(encoding="utf-8")  # type: ignore[union-attr]


_ensure_utf8()

BOX_WIDTH: int = 42


def _line(label: str, value: str) -> str:
    """Format a single row inside the box."""
    inner = f" {label:<16}{value:>{BOX_WIDTH - 20}} "
    return f"\u2551{inner}\u2551"


def print_report(result: BacktestResult) -> Metrics:
    """Print a formatted backtest report to console and return the metrics.

    Args:
        result: The BacktestResult from the engine.

    Returns:
        The calculated Metrics object.
    """
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
    print(_line("Total fees:", f"${m.total_fees:.2f}"))
    print(_line("Avg confidence:", f"{m.avg_confidence:.2f}"))
    print(_line("Trades/week:", f"{m.trades_per_week:.1f}"))
    print(_line("Best trade:", f"${m.best_trade:.2f}"))
    print(_line("Worst trade:", f"${m.worst_trade:.2f}"))
    print(mid)
    print(f"\u2551{f' VERDICT: {verdict} ':^{BOX_WIDTH}}\u2551")
    print(bot)
    print()

    return m


def print_comparison(results: list[tuple[str, Metrics]]) -> None:
    """Print a side-by-side comparison of strategy performance.

    Args:
        results: List of (strategy_name, Metrics) tuples.
    """
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
