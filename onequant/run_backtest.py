"""oneQuant backtest entry point — runs all strategies and prints reports.

Usage:
    cd onequant/
    python run_backtest.py
"""

from backtest.engine import BacktestConfig, run_backtest
from backtest.metrics import Metrics, calculate_metrics
from backtest.report import print_comparison, print_report
from strategies.mean_reversion import MeanReversionStrategy
from strategies.momentum import MomentumStrategy
from strategies.news_driven import NewsDrivenStrategy

TIMEFRAME: str = "15m"
CAPITAL: float = 25.0


def main() -> None:
    """Run all three strategies against historical 15m candles and compare."""
    print("=" * 50)
    print("oneQuant v0.3 — Backtest Runner")
    print("=" * 50)

    configs = [
        BacktestConfig(
            strategy=MomentumStrategy(),
            timeframe=TIMEFRAME,
            initial_capital=CAPITAL,
            min_confidence=0.70,
            take_profit_pct=0.04,
            stop_loss_pct=0.05,
            order_type="limit",
        ),
        BacktestConfig(
            strategy=MeanReversionStrategy(),
            timeframe=TIMEFRAME,
            initial_capital=CAPITAL,
            take_profit_pct=0.03,
            stop_loss_pct=0.06,
            order_type="limit",
        ),
        BacktestConfig(
            strategy=NewsDrivenStrategy(),
            timeframe=TIMEFRAME,
            initial_capital=CAPITAL,
        ),
    ]

    comparison: list[tuple[str, Metrics]] = []

    for cfg in configs:
        print(f"\nRunning backtest: {cfg.strategy.name} ...")
        result = run_backtest(cfg)
        metrics = print_report(result)
        comparison.append((cfg.strategy.name, metrics))

    print_comparison(comparison)


if __name__ == "__main__":
    main()
