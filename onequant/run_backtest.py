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


def main() -> None:
    """Run all three strategies against historical 15m candles and compare."""
    print("=" * 50)
    print("oneQuant v0.2 — Backtest Runner")
    print("=" * 50)

    strategies = [
        MomentumStrategy(),
        MeanReversionStrategy(),
        NewsDrivenStrategy(),
    ]

    comparison: list[tuple[str, Metrics]] = []

    for strategy in strategies:
        print(f"\nRunning backtest: {strategy.name} ...")
        cfg = BacktestConfig(strategy=strategy, timeframe=TIMEFRAME)
        result = run_backtest(cfg)
        metrics = print_report(result)
        comparison.append((strategy.name, metrics))

    print_comparison(comparison)


if __name__ == "__main__":
    main()
