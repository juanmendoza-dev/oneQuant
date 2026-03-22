"""Strategy B — Mean Reversion.

Uses RSI and EMA to detect overbought/oversold conditions and trade
the expected snap-back. Requires the last 21 candles on the 15m timeframe.
"""

from strategies.base import BaseStrategy, Signal

REQUIRED_CANDLES: int = 21
RSI_PERIOD: int = 14
EMA_PERIOD: int = 20
EMA_DEVIATION_PCT: float = 1.5
RSI_OVERSOLD: float = 30.0
RSI_OVERBOUGHT: float = 70.0
BASE_CONFIDENCE: float = 0.60
CONFIDENCE_PER_5_RSI: float = 0.10
MAX_CONFIDENCE: float = 0.90


def _calculate_rsi(closes: list[float], period: int) -> float:
    """Compute the Relative Strength Index over the given period.

    Uses the smoothed (Wilder) moving average method.
    """
    if len(closes) < period + 1:
        return 50.0  # neutral fallback

    deltas = [closes[i] - closes[i - 1] for i in range(1, len(closes))]
    gains = [max(d, 0.0) for d in deltas]
    losses = [abs(min(d, 0.0)) for d in deltas]

    avg_gain = sum(gains[:period]) / period
    avg_loss = sum(losses[:period]) / period

    for i in range(period, len(deltas)):
        avg_gain = (avg_gain * (period - 1) + gains[i]) / period
        avg_loss = (avg_loss * (period - 1) + losses[i]) / period

    if avg_loss == 0.0:
        return 100.0
    rs = avg_gain / avg_loss
    return 100.0 - (100.0 / (1.0 + rs))


def _calculate_ema(values: list[float], period: int) -> float:
    """Compute the Exponential Moving Average, returning the final value."""
    if len(values) < period:
        return sum(values) / len(values)

    multiplier = 2.0 / (period + 1)
    ema = sum(values[:period]) / period
    for val in values[period:]:
        ema = (val - ema) * multiplier + ema
    return ema


class MeanReversionStrategy(BaseStrategy):
    """Buy on oversold RSI below EMA; sell on overbought RSI above EMA."""

    name: str = "Mean Reversion"
    timeframe: str = "15m"

    def generate_signal(self, candles: list[dict]) -> Signal:
        """Generate a mean-reversion signal from the last 21 candles.

        Logic:
            - RSI < 30 AND close > 1.5% below 20-EMA → BUY
            - RSI > 70 AND close > 1.5% above 20-EMA → SELL
            - Confidence starts at 0.60, +0.10 per 5 RSI points beyond
              the threshold (capped at 0.90).
        """
        if len(candles) < REQUIRED_CANDLES:
            return Signal("SKIP", 0.0, f"Need {REQUIRED_CANDLES} candles, got {len(candles)}")

        closes = [c["close"] for c in candles]
        rsi = _calculate_rsi(closes, RSI_PERIOD)
        ema = _calculate_ema(closes, EMA_PERIOD)
        current_close = closes[-1]

        if ema == 0.0:
            return Signal("SKIP", 0.0, "EMA is zero")

        deviation_pct = ((current_close - ema) / ema) * 100.0

        if rsi < RSI_OVERSOLD and deviation_pct < -EMA_DEVIATION_PCT:
            points_beyond = RSI_OVERSOLD - rsi
            extra = (points_beyond // 5) * CONFIDENCE_PER_5_RSI
            confidence = min(BASE_CONFIDENCE + extra, MAX_CONFIDENCE)
            return Signal(
                "BUY",
                confidence,
                f"RSI {rsi:.1f} (oversold), price {deviation_pct:.2f}% below EMA",
            )

        if rsi > RSI_OVERBOUGHT and deviation_pct > EMA_DEVIATION_PCT:
            points_beyond = rsi - RSI_OVERBOUGHT
            extra = (points_beyond // 5) * CONFIDENCE_PER_5_RSI
            confidence = min(BASE_CONFIDENCE + extra, MAX_CONFIDENCE)
            return Signal(
                "SELL",
                confidence,
                f"RSI {rsi:.1f} (overbought), price +{deviation_pct:.2f}% above EMA",
            )

        return Signal("SKIP", 0.0, f"RSI {rsi:.1f}, deviation {deviation_pct:.2f}% — no setup")
