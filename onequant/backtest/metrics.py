"""Performance metrics calculated from a list of trade results."""

import math
from dataclasses import dataclass
from datetime import timedelta

from backtest.engine import BacktestResult, TradeResult

SECONDS_PER_WEEK: float = 7 * 24 * 3600.0


@dataclass
class Metrics:
    """Aggregate performance statistics for a backtest run."""

    total_trades: int
    winning_trades: int
    losing_trades: int
    win_rate: float
    total_pnl: float
    total_pnl_pct: float
    avg_win: float
    avg_loss: float
    profit_factor: float
    max_drawdown: float
    sharpe_ratio: float
    avg_confidence: float
    total_fees: float
    best_trade: float
    worst_trade: float
    trades_per_week: float

    def is_profitable(self) -> bool:
        """Return True only if win_rate > 55% AND profit_factor > 1.2."""
        return self.win_rate > 0.55 and self.profit_factor > 1.2


def calculate_metrics(result: BacktestResult) -> Metrics:
    """Compute all performance metrics from a backtest result.

    Args:
        result: A BacktestResult containing the trade list and run metadata.

    Returns:
        A Metrics dataclass with all calculated statistics.
    """
    trades = result.trades
    if not trades:
        return Metrics(
            total_trades=0, winning_trades=0, losing_trades=0,
            win_rate=0.0, total_pnl=0.0, total_pnl_pct=0.0,
            avg_win=0.0, avg_loss=0.0, profit_factor=0.0,
            max_drawdown=0.0, sharpe_ratio=0.0, avg_confidence=0.0,
            total_fees=0.0, best_trade=0.0, worst_trade=0.0,
            trades_per_week=0.0,
        )

    wins = [t for t in trades if t.outcome == "WIN"]
    losses = [t for t in trades if t.outcome == "LOSS"]
    total = len(trades)

    win_rate = len(wins) / total if total else 0.0
    total_pnl = sum(t.pnl for t in trades)
    total_pnl_pct = (total_pnl / result.initial_capital) * 100.0

    gross_wins = sum(t.pnl for t in wins) if wins else 0.0
    gross_losses = abs(sum(t.pnl for t in losses)) if losses else 0.0

    avg_win = gross_wins / len(wins) if wins else 0.0
    avg_loss = -gross_losses / len(losses) if losses else 0.0

    profit_factor = gross_wins / gross_losses if gross_losses > 0 else float("inf")

    # Max drawdown — track equity curve
    max_drawdown = _calculate_max_drawdown(trades, result.initial_capital)

    # Sharpe ratio (simplified, risk-free = 0)
    sharpe_ratio = _calculate_sharpe(trades)

    avg_confidence = sum(t.confidence for t in trades) / total
    total_fees = sum(t.fees_paid for t in trades)

    pnls = [t.pnl for t in trades]
    best_trade = max(pnls)
    worst_trade = min(pnls)

    # Trades per week
    duration: timedelta = result.end_date - result.start_date
    duration_weeks = duration.total_seconds() / SECONDS_PER_WEEK
    trades_per_week = total / duration_weeks if duration_weeks > 0 else 0.0

    return Metrics(
        total_trades=total,
        winning_trades=len(wins),
        losing_trades=len(losses),
        win_rate=win_rate,
        total_pnl=total_pnl,
        total_pnl_pct=total_pnl_pct,
        avg_win=avg_win,
        avg_loss=avg_loss,
        profit_factor=profit_factor,
        max_drawdown=max_drawdown,
        sharpe_ratio=sharpe_ratio,
        avg_confidence=avg_confidence,
        total_fees=total_fees,
        best_trade=best_trade,
        worst_trade=worst_trade,
        trades_per_week=trades_per_week,
    )


def _calculate_max_drawdown(trades: list[TradeResult], initial_capital: float) -> float:
    """Calculate max drawdown as a percentage from the equity curve."""
    equity = initial_capital
    peak = equity
    max_dd = 0.0

    for trade in trades:
        equity += trade.pnl
        if equity > peak:
            peak = equity
        drawdown = (peak - equity) / peak if peak > 0 else 0.0
        if drawdown > max_dd:
            max_dd = drawdown

    return max_dd * 100.0  # as percentage


def _calculate_sharpe(trades: list[TradeResult]) -> float:
    """Calculate simplified Sharpe ratio (risk-free rate = 0)."""
    if len(trades) < 2:
        return 0.0

    returns = [t.pnl_pct for t in trades]
    mean_ret = sum(returns) / len(returns)
    variance = sum((r - mean_ret) ** 2 for r in returns) / (len(returns) - 1)
    std_ret = math.sqrt(variance)

    if std_ret == 0.0:
        return 0.0

    return (mean_ret / std_ret) * math.sqrt(len(returns))
