"""Performance metrics calculated from a list of trade results."""

import math
from collections import Counter
from dataclasses import dataclass, field
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

    # Fee breakdown
    avg_fee_pct: float = 0.0
    fee_tier_breakdown: dict = field(default_factory=dict)

    # Cost breakdown
    total_slippage: float = 0.0
    total_spread_cost: float = 0.0
    gap_stops: int = 0

    # Time analysis
    performance_by_hour: dict = field(default_factory=dict)
    performance_by_day: dict = field(default_factory=dict)

    # Regime analysis
    trades_by_regime: dict = field(default_factory=dict)
    win_rate_by_regime: dict = field(default_factory=dict)
    pf_by_regime: dict = field(default_factory=dict)

    # Buy-and-hold benchmark
    bh_return_pct: float = 0.0
    bh_max_drawdown: float = 0.0
    bh_sharpe: float = 0.0
    alpha_vs_bh: float = 0.0

    def is_profitable(self) -> bool:
        """Return True only if win_rate > 55% AND profit_factor > 1.2."""
        return self.win_rate > 0.55 and self.profit_factor > 1.2


def _empty_metrics() -> Metrics:
    """Return a zeroed-out Metrics for runs with no trades."""
    return Metrics(
        total_trades=0, winning_trades=0, losing_trades=0,
        win_rate=0.0, total_pnl=0.0, total_pnl_pct=0.0,
        avg_win=0.0, avg_loss=0.0, profit_factor=0.0,
        max_drawdown=0.0, sharpe_ratio=0.0, avg_confidence=0.0,
        total_fees=0.0, best_trade=0.0, worst_trade=0.0,
        trades_per_week=0.0,
    )


def calculate_metrics(result: BacktestResult) -> Metrics:
    """Compute all performance metrics from a backtest result."""
    trades = result.trades
    if not trades:
        m = _empty_metrics()
        if result.buy_and_hold:
            m.bh_return_pct = result.buy_and_hold.return_pct
            m.bh_max_drawdown = result.buy_and_hold.max_drawdown
            m.bh_sharpe = result.buy_and_hold.sharpe
        return m

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

    max_drawdown = _calculate_max_drawdown(trades, result.initial_capital)
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

    # Fee breakdown
    total_position_vol = sum(t.position_size_usd for t in trades)
    avg_fee_pct = (total_fees / (total_position_vol * 2)) if total_position_vol > 0 else 0.0
    fee_tier_breakdown = dict(Counter(t.fee_tier for t in trades))

    # Cost breakdown
    total_slippage = sum(t.slippage_paid for t in trades)
    total_spread_cost = sum(t.spread_cost for t in trades)
    gap_stops = sum(1 for t in trades if t.gap_stop)

    # Performance by hour
    performance_by_hour = _performance_by_group(trades, lambda t: t.entry_hour)
    performance_by_day = _performance_by_group(trades, lambda t: t.entry_day)

    # Regime analysis
    trades_by_regime = dict(Counter(t.regime for t in trades))
    win_rate_by_regime = {}
    pf_by_regime = {}
    for regime in trades_by_regime:
        r_trades = [t for t in trades if t.regime == regime]
        r_wins = [t for t in r_trades if t.outcome == "WIN"]
        r_losses = [t for t in r_trades if t.outcome == "LOSS"]
        win_rate_by_regime[regime] = len(r_wins) / len(r_trades) if r_trades else 0.0
        g_wins = sum(t.pnl for t in r_wins) if r_wins else 0.0
        g_losses = abs(sum(t.pnl for t in r_losses)) if r_losses else 0.0
        pf_by_regime[regime] = g_wins / g_losses if g_losses > 0 else float("inf")

    # Buy-and-hold benchmark
    bh_return_pct = 0.0
    bh_max_drawdown = 0.0
    bh_sharpe = 0.0
    alpha_vs_bh = 0.0
    if result.buy_and_hold:
        bh_return_pct = result.buy_and_hold.return_pct
        bh_max_drawdown = result.buy_and_hold.max_drawdown
        bh_sharpe = result.buy_and_hold.sharpe
        alpha_vs_bh = total_pnl_pct - bh_return_pct

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
        avg_fee_pct=avg_fee_pct,
        fee_tier_breakdown=fee_tier_breakdown,
        total_slippage=total_slippage,
        total_spread_cost=total_spread_cost,
        gap_stops=gap_stops,
        performance_by_hour=performance_by_hour,
        performance_by_day=performance_by_day,
        trades_by_regime=trades_by_regime,
        win_rate_by_regime=win_rate_by_regime,
        pf_by_regime=pf_by_regime,
        bh_return_pct=bh_return_pct,
        bh_max_drawdown=bh_max_drawdown,
        bh_sharpe=bh_sharpe,
        alpha_vs_bh=alpha_vs_bh,
    )


def _performance_by_group(trades: list[TradeResult], key_fn) -> dict:
    """Compute win rate and trade count grouped by a key function."""
    groups: dict = {}
    for t in trades:
        k = key_fn(t)
        if k not in groups:
            groups[k] = {"trades": 0, "wins": 0}
        groups[k]["trades"] += 1
        if t.outcome == "WIN":
            groups[k]["wins"] += 1
    result = {}
    for k, v in groups.items():
        result[k] = {
            "trades": v["trades"],
            "win_rate": v["wins"] / v["trades"] if v["trades"] else 0.0,
        }
    return result


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

    return max_dd * 100.0


def _calculate_sharpe(trades: list[TradeResult]) -> float:
    """Calculate simplified Sharpe ratio (risk-free rate = 0)."""
    if len(trades) < 2:
        return 0.0

    returns = [t.pnl_pct for t in trades]
    mean_ret = sum(returns) / len(returns)
    variance = sum((r - mean_ret) ** 2 for r in returns) / (len(returns) - 1)
    std_ret = math.sqrt(variance) if variance > 0 else 0.0

    if std_ret == 0.0:
        return 0.0

    return (mean_ret / std_ret) * math.sqrt(len(returns))
