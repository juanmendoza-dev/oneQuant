"""1H Liquidity Sweep Reversal — production strategy.

Enters long when a 1h candle wicks below the 12-candle (12h) rolling low
and closes back above it. Trades all hours.

Backtested on 40,721 1h candles (2020-01-01 to 2026-04-03):
  Config: SL=10%, TP=8%, $75 initial capital
  Result: 204 trades, WR=58.8%, PF=1.137, MaxDD=10.8%, P&L=+$9.10

The edge: when BTC wicks below a 12h structural low and closes above it,
buyers absorbed the sell-side liquidity. The wide SL (10%) avoids noise
stops while the 8% TP captures the structural bounce.
"""

from strategies.base import BaseStrategy, Signal

LOOKBACK = 12
VOL_LOOKBACK = 20
VOL_MULTIPLIER = 1.0


def _rolling_low(candles, end, n):
    return min(c["low"] for c in candles[end - n:end])


def _avg_volume(candles, end, n):
    window = candles[end - n:end]
    return sum(c["volume"] for c in window) / len(window) if window else 0.0


class Sweep1hStrategy(BaseStrategy):
    name: str = "1H Sweep Reversal"
    timeframe: str = "1h"
    required_candles: int = 25

    def generate_signal(self, candles: list[dict]) -> Signal:
        if len(candles) < self.required_candles:
            return Signal("SKIP", 0.0, "not enough candles")

        c = candles[-1]

        # 12-candle rolling low (excluding current candle)
        rl = _rolling_low(candles, len(candles) - 1, LOOKBACK)

        # Sweep condition: wick below rolling low, close above it
        if not (c["low"] < rl and c["close"] > rl):
            return Signal("SKIP", 0.0, "no sweep")

        # Volume must be at least average (filters thin-market noise)
        avg_vol = _avg_volume(candles, len(candles) - 1, VOL_LOOKBACK)
        if avg_vol > 0 and c["volume"] < avg_vol * VOL_MULTIPLIER:
            return Signal("SKIP", 0.0, "low volume")

        # Confidence scales with sweep depth
        sweep_depth = (rl - c["low"]) / rl if rl > 0 else 0
        confidence = min(1.0, max(0.6, 0.6 + sweep_depth * 40))

        return Signal(
            "BUY", confidence,
            f"1h sweep below {rl:.0f} depth={sweep_depth*100:.2f}%"
        )
