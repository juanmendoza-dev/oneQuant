"""Strategy — EMA Pullback.

Trades mean-reversion pullbacks to the 20-period EMA in trending regimes.

BUY signal (BULL_TREND regime):
  - Close within 0.5% of 20 EMA (price touched/near EMA)
  - RSI between 40 and 60
  - Volume below 20-period average (quiet pullback)
  - Previous candle close > current close (pullback in progress)

SELL signal (BEAR_TREND regime):
  - Close within 0.5% of 20 EMA
  - RSI between 40 and 60
  - Volume below 20-period average
  - Previous candle close < current close (pullback in progress)

Uses last 30 candles. Regime filtering handled by the engine via allowed_regimes.
"""

from strategies.base import BaseStrategy, Signal

REQUIRED_CANDLES: int = 30
RSI_PERIOD: int = 14
EMA_PERIOD: int = 20
EMA_PROXIMITY_PCT: float = 0.005   # 0.5%
RSI_LOW: float = 40.0
RSI_HIGH: float = 60.0
CONFIDENCE: float = 0.72


def _calculate_rsi(closes: list[float], period: int) -> float:
    """Compute RSI using Wilder's smoothed moving average."""
    if len(closes) < period + 1:
        return 50.0

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
    """Compute EMA, returning the final value."""
    if len(values) < period:
        return sum(values) / len(values)

    multiplier = 2.0 / (period + 1)
    ema = sum(values[:period]) / period
    for val in values[period:]:
        ema = (val - ema) * multiplier + ema
    return ema


class EMAPullbackStrategy(BaseStrategy):
    """EMA pullback strategy — fires in BULL_TREND or BEAR_TREND regimes."""

    name: str = "EMA Pullback"
    timeframe: str = "15m"
    required_candles: int = REQUIRED_CANDLES

    def generate_signal(self, candles: list[dict]) -> Signal:
        """Generate a signal when price pulls back to the 20 EMA in a trend."""
        if len(candles) < REQUIRED_CANDLES:
            return Signal("SKIP", 0.0, f"Need {REQUIRED_CANDLES} candles, got {len(candles)}")

        closes = [c["close"] for c in candles]
        volumes = [c["volume"] for c in candles]

        current_close = closes[-1]
        prev_close = closes[-2]

        rsi = _calculate_rsi(closes, RSI_PERIOD)
        ema = _calculate_ema(closes, EMA_PERIOD)

        if ema == 0.0:
            return Signal("SKIP", 0.0, "EMA is zero")

        # Proximity to EMA
        proximity = abs((current_close - ema) / ema)
        near_ema = proximity <= EMA_PROXIMITY_PCT

        # RSI in neutral zone
        rsi_neutral = RSI_LOW <= rsi <= RSI_HIGH

        # Volume below 20-period average (quiet pullback)
        vol_ma = sum(volumes[-EMA_PERIOD:]) / EMA_PERIOD
        vol_quiet = vol_ma > 0 and volumes[-1] < vol_ma

        if not near_ema:
            return Signal(
                "SKIP",
                0.0,
                f"Price {current_close:.2f} not near EMA {ema:.2f} "
                f"(proximity {proximity * 100:.3f}% > 0.5%)",
            )

        if not rsi_neutral:
            return Signal("SKIP", 0.0, f"RSI {rsi:.1f} outside 40–60 range")

        if not vol_quiet:
            return Signal(
                "SKIP",
                0.0,
                f"Volume {volumes[-1]:.0f} not below 20-period avg {vol_ma:.0f}",
            )

        # BUY: previous close > current close (pullback down in BULL_TREND)
        if prev_close > current_close:
            return Signal(
                "BUY",
                CONFIDENCE,
                (
                    f"Bullish EMA pullback: close {current_close:.2f} near EMA {ema:.2f} "
                    f"({proximity * 100:.3f}%), RSI {rsi:.1f}, "
                    f"vol {volumes[-1]:.0f} < avg {vol_ma:.0f}, "
                    f"prev close {prev_close:.2f} > curr close {current_close:.2f}"
                ),
            )

        # SELL: previous close < current close (pullback up in BEAR_TREND)
        if prev_close < current_close:
            return Signal(
                "SELL",
                CONFIDENCE,
                (
                    f"Bearish EMA pullback: close {current_close:.2f} near EMA {ema:.2f} "
                    f"({proximity * 100:.3f}%), RSI {rsi:.1f}, "
                    f"vol {volumes[-1]:.0f} < avg {vol_ma:.0f}, "
                    f"prev close {prev_close:.2f} < curr close {current_close:.2f}"
                ),
            )

        return Signal("SKIP", 0.0, f"No pullback direction: prev={prev_close:.2f}, curr={current_close:.2f}")
