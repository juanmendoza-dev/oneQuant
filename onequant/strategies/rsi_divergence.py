"""Strategy — RSI Divergence.

Detects bullish and bearish RSI divergence using swing highs/lows.
Fires in RANGING regime only.

Bullish divergence (BUY):
  - Price making lower low (recent swing low < prior swing low)
  - RSI making higher low (RSI at recent swing low > RSI at prior swing low)
  - Current RSI < 45 (oversold zone)

Bearish divergence (SELL):
  - Price making higher high (recent swing high > prior swing high)
  - RSI making lower high (RSI at recent swing high < RSI at prior swing high)
  - Current RSI > 55 (overbought zone)

Uses last 30 candles; swing detection over candles[-15:-1] (prior 14 candles).
"""

from strategies.base import BaseStrategy, Signal

REQUIRED_CANDLES: int = 30
RSI_PERIOD: int = 14
CONFIDENCE: float = 0.70
RSI_OVERSOLD_THRESHOLD: float = 45.0
RSI_OVERBOUGHT_THRESHOLD: float = 55.0


def _calculate_rsi_at(closes: list[float], index: int, period: int) -> float:
    """Compute RSI using all closes from 0..index (inclusive), Wilder method.

    This computes rolling RSI independently for each candle using only
    data available up to that point — no lookahead.
    """
    window = closes[: index + 1]
    if len(window) < period + 1:
        return 50.0  # neutral fallback

    deltas = [window[i] - window[i - 1] for i in range(1, len(window))]
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


class RSIDivergenceStrategy(BaseStrategy):
    """RSI divergence strategy — fires in RANGING regime only."""

    name: str = "RSI Divergence"
    timeframe: str = "15m"
    required_candles: int = REQUIRED_CANDLES

    def generate_signal(self, candles: list[dict]) -> Signal:
        """Detect RSI divergence using swing highs/lows in prior 14 candles."""
        if len(candles) < REQUIRED_CANDLES:
            return Signal("SKIP", 0.0, f"Need {REQUIRED_CANDLES} candles, got {len(candles)}")

        closes = [c["close"] for c in candles]

        # Current RSI (using all 30 candles up to last)
        current_rsi = _calculate_rsi_at(closes, len(closes) - 1, RSI_PERIOD)

        # Swing detection window: candles[-15:-1] = indices 15..28 in 0-based
        # relative to the 30-candle window (indices 15 to 28 inclusive)
        # That is candles[15] through candles[28], with neighbors at i-1 and i+1
        swing_start = len(candles) - 15  # index 15 in 0-based
        swing_end = len(candles) - 1     # index 29, exclusive → check up to 28

        swing_lows: list[tuple[int, float, float]] = []   # (abs_index, close, rsi)
        swing_highs: list[tuple[int, float, float]] = []  # (abs_index, close, rsi)

        for i in range(swing_start, swing_end):
            # Neighbor indices
            prev_close = closes[i - 1]
            curr_close = closes[i]
            next_close = closes[i + 1]

            rsi_at_i = _calculate_rsi_at(closes, i, RSI_PERIOD)

            # Swing low: close lower than both neighbors
            if curr_close < prev_close and curr_close < next_close:
                swing_lows.append((i, curr_close, rsi_at_i))

            # Swing high: close higher than both neighbors
            if curr_close > prev_close and curr_close > next_close:
                swing_highs.append((i, curr_close, rsi_at_i))

        # --- BULLISH DIVERGENCE (BUY) ---
        if len(swing_lows) >= 2 and current_rsi < RSI_OVERSOLD_THRESHOLD:
            # Two most recent swing lows
            first_low = swing_lows[-2]   # earlier swing low
            second_low = swing_lows[-1]  # more recent swing low

            price_lower_low = second_low[1] < first_low[1]   # price: lower low
            rsi_higher_low = second_low[2] > first_low[2]    # RSI: higher low

            if price_lower_low and rsi_higher_low:
                return Signal(
                    "BUY",
                    CONFIDENCE,
                    (
                        f"Bullish divergence: price {first_low[1]:.2f}→{second_low[1]:.2f} "
                        f"(lower low), RSI {first_low[2]:.1f}→{second_low[2]:.1f} "
                        f"(higher low), current RSI {current_rsi:.1f}"
                    ),
                )

        # --- BEARISH DIVERGENCE (SELL) ---
        if len(swing_highs) >= 2 and current_rsi > RSI_OVERBOUGHT_THRESHOLD:
            # Two most recent swing highs
            first_high = swing_highs[-2]   # earlier swing high
            second_high = swing_highs[-1]  # more recent swing high

            price_higher_high = second_high[1] > first_high[1]   # price: higher high
            rsi_lower_high = second_high[2] < first_high[2]      # RSI: lower high

            if price_higher_high and rsi_lower_high:
                return Signal(
                    "SELL",
                    CONFIDENCE,
                    (
                        f"Bearish divergence: price {first_high[1]:.2f}→{second_high[1]:.2f} "
                        f"(higher high), RSI {first_high[2]:.1f}→{second_high[2]:.1f} "
                        f"(lower high), current RSI {current_rsi:.1f}"
                    ),
                )

        return Signal(
            "SKIP",
            0.0,
            f"No divergence: {len(swing_lows)} swing lows, "
            f"{len(swing_highs)} swing highs, RSI {current_rsi:.1f}",
        )
