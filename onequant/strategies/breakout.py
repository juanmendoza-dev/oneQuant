"""Strategy D — Donchian Channel Breakout.

Watches the highest high and lowest low of the last 20 candles.
A breakout with volume confirmation generates a directional signal.
No regime filter — breakouts can occur in any market environment.
"""

from strategies.base import BaseStrategy, Signal

REQUIRED_CANDLES: int = 21  # 20 for channel + 1 current
CHANNEL_PERIOD: int = 20
VOLUME_MULTIPLIER: float = 1.5
BASE_CONFIDENCE: float = 0.72


class BreakoutStrategy(BaseStrategy):
    """BUY when price breaks above 20-period high with volume surge.
    SELL when price breaks below 20-period low with volume surge.

    Volume must be >= 1.5x the 20-period average to confirm the breakout.
    Confidence is fixed — breakout is binary (either confirmed or not).
    """

    name: str = "Breakout"
    timeframe: str = "15m"
    required_candles: int = REQUIRED_CANDLES

    def generate_signal(self, candles: list[dict]) -> Signal:
        """Generate a breakout signal from the last 21 candles.

        Logic:
            - channel_high = max(high) of candles[0:-1]  (prior 20, no current)
            - channel_low  = min(low)  of candles[0:-1]
            - current close breaks above channel_high AND volume >= 1.5x avg → BUY
            - current close breaks below channel_low  AND volume >= 1.5x avg → SELL
        """
        if len(candles) < REQUIRED_CANDLES:
            return Signal("SKIP", 0.0, f"Need {REQUIRED_CANDLES} candles, got {len(candles)}")

        # Use the prior 20 candles to define the channel — current candle is execution candle
        prior = candles[-REQUIRED_CANDLES:-1]  # exactly 20 candles, no lookahead
        current = candles[-1]

        channel_high = max(c["high"] for c in prior)
        channel_low = min(c["low"] for c in prior)

        # Volume: compare current to average of prior 20
        vol_avg = sum(c["volume"] for c in prior) / CHANNEL_PERIOD
        current_vol = current["volume"]

        if vol_avg <= 0:
            return Signal("SKIP", 0.0, "Zero volume average")

        vol_ratio = current_vol / vol_avg
        vol_confirmed = vol_ratio >= VOLUME_MULTIPLIER

        current_close = current["close"]

        if current_close > channel_high:
            if not vol_confirmed:
                return Signal(
                    "SKIP", 0.0,
                    f"Breakout above {channel_high:.0f} but volume only {vol_ratio:.2f}x avg"
                )
            return Signal(
                "BUY",
                BASE_CONFIDENCE,
                f"Broke above {channel_high:.0f}, vol {vol_ratio:.2f}x avg",
            )

        if current_close < channel_low:
            if not vol_confirmed:
                return Signal(
                    "SKIP", 0.0,
                    f"Breakdown below {channel_low:.0f} but volume only {vol_ratio:.2f}x avg"
                )
            return Signal(
                "SELL",
                BASE_CONFIDENCE,
                f"Broke below {channel_low:.0f}, vol {vol_ratio:.2f}x avg",
            )

        return Signal(
            "SKIP", 0.0,
            f"Price {current_close:.0f} within channel [{channel_low:.0f}, {channel_high:.0f}]"
        )
