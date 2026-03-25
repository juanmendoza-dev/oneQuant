"""BB Reversion — initial backtest at $250 capital.

Usage:
    cd onequant/
    python run_bb.py
"""

import sys

if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8")

from backtest.engine import BacktestConfig, run_backtest
from backtest.report import print_report
from strategies.bb_reversion import BBReversionStrategy


def main() -> None:
    print("=" * 50)
    print("BB Reversion — Backtest ($250, 15m, 10yr)")
    print("=" * 50)

    cfg = BacktestConfig(
        strategy=BBReversionStrategy(),
        timeframe="15m",
        initial_capital=250.0,
        position_size_pct=0.10,
        stop_loss_pct=0.03,
        take_profit_pct=0.025,
        min_confidence=0.65,
        slippage_pct=0.001,
        spread_pct=0.0005,
        order_type="limit",
        allowed_regimes=["RANGING"],
    )

    result = run_backtest(cfg)
    print_report(result)


if __name__ == "__main__":
    main()
