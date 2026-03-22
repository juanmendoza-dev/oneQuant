"""Strategy A — Momentum.

Detects short-term directional moves confirmed by above-average volume.
Requires the last 22 candles on the 15m timeframe.
"""

from strategies.base import BaseStrategy, Signal

REQUIRED_CANDLES: int = 22
VOLUME_MA_PERIOD: int = 20
LOOKBACK: int = 2
MIN_CONFIDENCE: float = 0.55
MAX_CONFIDENCE: float = 0.90


class MomentumStrategy(BaseStrategy):
    """Buy or sell when consecutive candles move in one direction on high volume."""

    name: str = "Momentum"
    timeframe: str = "15m"

    def generate_signal(self, candles: list[dict]) -> Signal:
        """Generate a momentum signal from the last 22 candles.

        Logic:
            - Compute 20-period volume moving average.
            - If the last 2 candles both closed UP and current volume exceeds
              the moving average → BUY.
            - If the last 2 candles both closed DOWN and current volume exceeds
              the moving average → SELL.
            - Confidence is scaled by the volume ratio (clamped 0.55–0.90).
        """
        if len(candles) < REQUIRED_CANDLES:
            return Signal("SKIP", 0.0, f"Need {REQUIRED_CANDLES} candles, got {len(candles)}")

        volumes = [c["volume"] for c in candles[-VOLUME_MA_PERIOD:]]
        vol_ma = sum(volumes) / VOLUME_MA_PERIOD
        if vol_ma == 0:
            return Signal("SKIP", 0.0, "Volume moving average is zero")

        current_vol = candles[-1]["volume"]
        vol_ratio = current_vol / vol_ma

        if vol_ratio <= 1.0:
            return Signal("SKIP", 0.0, f"Volume ratio {vol_ratio:.2f} below average")

        last_two = candles[-LOOKBACK:]
        both_up = all(c["close"] > c["open"] for c in last_two)
        both_down = all(c["close"] < c["open"] for c in last_two)

        if not both_up and not both_down:
            return Signal("SKIP", 0.0, "No consecutive directional candles")

        confidence = min(MIN_CONFIDENCE + (vol_ratio - 1.0) * 0.25, MAX_CONFIDENCE)

        if both_up:
            return Signal(
                "BUY",
                confidence,
                f"2 consecutive green candles, vol ratio {vol_ratio:.2f}x avg",
            )
        return Signal(
            "SELL",
            confidence,
            f"2 consecutive red candles, vol ratio {vol_ratio:.2f}x avg",
        )
