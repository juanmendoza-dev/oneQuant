"""Strategy A — Momentum.

Detects short-term directional moves confirmed by high volume and
a trend-aligned EMA filter. Requires the last 51 candles on the
15m timeframe.
"""

from strategies.base import BaseStrategy, Signal

REQUIRED_CANDLES: int = 51
VOLUME_MA_PERIOD: int = 20
VOLUME_RATIO_THRESHOLD: float = 2.0
TREND_EMA_PERIOD: int = 50
LOOKBACK: int = 2
MIN_CONFIDENCE: float = 0.55
MAX_CONFIDENCE: float = 0.90


def _calculate_ema(values: list[float], period: int) -> float:
    """Compute the Exponential Moving Average, returning the final value."""
    if len(values) < period:
        return sum(values) / len(values)
    multiplier = 2.0 / (period + 1)
    ema = sum(values[:period]) / period
    for val in values[period:]:
        ema = (val - ema) * multiplier + ema
    return ema


class MomentumStrategy(BaseStrategy):
    """Buy or sell when consecutive candles move in one direction on high volume.

    Includes a 50-period EMA trend filter: BUY only above EMA, SELL only below.
    """

    name: str = "Momentum"
    timeframe: str = "15m"
    required_candles: int = REQUIRED_CANDLES

    def generate_signal(self, candles: list[dict]) -> Signal:
        """Generate a momentum signal from the last 51 candles.

        Logic:
            - Compute 20-period volume moving average.
            - Require volume >= 1.5x the average to trigger.
            - If the last 2 candles both closed UP and price is above the
              50-period EMA → BUY.
            - If the last 2 candles both closed DOWN and price is below the
              50-period EMA → SELL.
            - Confidence scaled by volume ratio (clamped 0.55–0.90).
        """
        if len(candles) < REQUIRED_CANDLES:
            return Signal("SKIP", 0.0, f"Need {REQUIRED_CANDLES} candles, got {len(candles)}")

        volumes = [c["volume"] for c in candles[-VOLUME_MA_PERIOD:]]
        vol_ma = sum(volumes) / VOLUME_MA_PERIOD
        if vol_ma == 0:
            return Signal("SKIP", 0.0, "Volume moving average is zero")

        current_vol = candles[-1]["volume"]
        vol_ratio = current_vol / vol_ma

        if vol_ratio < VOLUME_RATIO_THRESHOLD:
            return Signal("SKIP", 0.0, f"Volume ratio {vol_ratio:.2f} below {VOLUME_RATIO_THRESHOLD}x threshold")

        last_two = candles[-LOOKBACK:]
        both_up = all(c["close"] > c["open"] for c in last_two)
        both_down = all(c["close"] < c["open"] for c in last_two)

        if not both_up and not both_down:
            return Signal("SKIP", 0.0, "No consecutive directional candles")

        # Trend filter: 50-period EMA
        closes = [c["close"] for c in candles]
        ema_50 = _calculate_ema(closes, TREND_EMA_PERIOD)
        current_close = closes[-1]

        if both_up and current_close <= ema_50:
            return Signal("SKIP", 0.0, f"BUY blocked — price {current_close:.2f} below EMA50 {ema_50:.2f}")

        if both_down and current_close >= ema_50:
            return Signal("SKIP", 0.0, f"SELL blocked — price {current_close:.2f} above EMA50 {ema_50:.2f}")

        confidence = min(MIN_CONFIDENCE + (vol_ratio - 1.0) * 0.25, MAX_CONFIDENCE)

        if both_up:
            return Signal(
                "BUY",
                confidence,
                f"2 green candles, vol {vol_ratio:.2f}x avg, above EMA50",
            )
        return Signal(
            "SELL",
            confidence,
            f"2 red candles, vol {vol_ratio:.2f}x avg, below EMA50",
        )
