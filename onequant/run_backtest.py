"""oneQuant backtest entry point — runs all strategies and prints reports.

Usage:
    cd onequant/
    python run_backtest.py
"""

from backtest.engine import BacktestConfig, run_backtest
from backtest.metrics import Metrics, calculate_metrics
from backtest.report import print_comparison, print_report
from strategies.mean_reversion import MeanReversionStrategy

TIMEFRAME: str = "15m"
CAPITAL: float = 250.0


def main() -> None:
    """Run all strategies against historical 15m candles and compare."""
    print("=" * 50)
    print("oneQuant v0.3 — Backtest Runner")
    print("=" * 50)

    configs = [
        # Config A: SELL only, BULL_TREND — validated strategy
        # WR 76.8%, PF 1.47, DD -2.5%, 56 trades over 10yr
        BacktestConfig(
            strategy=MeanReversionStrategy(),
            timeframe=TIMEFRAME,
            initial_capital=CAPITAL,
            take_profit_pct=0.04,
            stop_loss_pct=0.06,
            min_confidence=0.55,
            order_type="limit",
            allowed_regimes=["BULL_TREND"],
        ),
    ]

    comparison: list[tuple[str, Metrics]] = []

    for cfg in configs:
        label = cfg.strategy.name
        if cfg.allowed_regimes:
            label += " (trend only)"
        print(f"\nRunning backtest: {label} ...")
        result = run_backtest(cfg)
        result.strategy_name = label
        metrics = print_report(result)
        comparison.append((label, metrics))

    print_comparison(comparison)


if __name__ == "__main__":
    main()
