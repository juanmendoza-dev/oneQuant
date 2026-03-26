"""Strategy — Capitulation Reversal.

Fires only when all four conditions align simultaneously, indicating
a panic selling climax with high probability of reversal.

BUY signal — ALL must be true:
  - RSI < 20 (extreme oversold)
  - Current candle body > 2× average of prior 20 candle bodies
    (large red candle = panic selling)
  - Volume > 3× the 20-period volume average
    (climactic volume)
  - Price closes below lower Bollinger Band (20-period, 2 std devs)

No regime filter — capitulation can occur in any regime.
Expected fire rate: 10–20 times per year maximum.
"""

import math

from strategies.base import BaseStrategy, Signal

REQUIRED_CANDLES: int = 30
RSI_PERIOD: int = 14
BB_PERIOD: int = 20
BB_STD_MULTIPLIER: float = 2.0
VOL_PERIOD: int = 20
BODY_LOOKBACK: int = 20     # prior candles for average body
RSI_THRESHOLD: float = 20.0
VOLUME_MULTIPLIER: float = 3.0
BODY_MULTIPLIER: float = 2.0
CONFIDENCE: float = 0.85


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
    return 100.0 - (100.0 / (1.0 + avg_gain / avg_loss))


def _bb_lower(closes: list[float], period: int, std_mult: float) -> float:
    """Return the lower Bollinger Band from the last `period` closes."""
    window = closes[-period:]
    mean = sum(window) / period
    variance = sum((c - mean) ** 2 for c in window) / period
    std = math.sqrt(variance) if variance > 0 else 0.0
    return mean - std_mult * std


class CapitulationStrategy(BaseStrategy):
    """Buy on panic selling climax: extreme RSI + large body + surge volume + below BB."""

    name: str = "Capitulation Reversal"
    timeframe: str = "15m"
    required_candles: int = REQUIRED_CANDLES

    def generate_signal(self, candles: list[dict]) -> Signal:
        if len(candles) < REQUIRED_CANDLES:
            return Signal("SKIP", 0.0, f"Need {REQUIRED_CANDLES} candles, got {len(candles)}")

        closes = [c["close"] for c in candles]
        volumes = [c["volume"] for c in candles]

        current_close = closes[-1]
        rsi = _calculate_rsi(closes, RSI_PERIOD)
        bb_low = _bb_lower(closes, BB_PERIOD, BB_STD_MULTIPLIER)

        # Volume: current vs 20-period average
        vol_ma = sum(volumes[-VOL_PERIOD:]) / VOL_PERIOD
        current_vol = volumes[-1]

        # Candle body: current vs average of prior BODY_LOOKBACK bodies (excluding current)
        prior_bodies = [abs(c["close"] - c["open"]) for c in candles[-(BODY_LOOKBACK + 1):-1]]
        avg_body = sum(prior_bodies) / len(prior_bodies) if prior_bodies else 0.0
        current_body = abs(candles[-1]["close"] - candles[-1]["open"])

        # Gate checks (fail-fast, most specific reason first)
        if rsi >= RSI_THRESHOLD:
            return Signal("SKIP", 0.0, f"RSI {rsi:.1f} not extreme (need < {RSI_THRESHOLD})")

        if avg_body == 0 or current_body <= BODY_MULTIPLIER * avg_body:
            ratio = current_body / avg_body if avg_body > 0 else 0.0
            return Signal(
                "SKIP", 0.0,
                f"Body {current_body:.2f} not > {BODY_MULTIPLIER}× avg {avg_body:.2f} (ratio {ratio:.2f}×)",
            )

        if vol_ma == 0 or current_vol <= VOLUME_MULTIPLIER * vol_ma:
            ratio = current_vol / vol_ma if vol_ma > 0 else 0.0
            return Signal(
                "SKIP", 0.0,
                f"Volume {current_vol:.0f} not > {VOLUME_MULTIPLIER}× avg {vol_ma:.0f} (ratio {ratio:.2f}×)",
            )

        if current_close >= bb_low:
            return Signal(
                "SKIP", 0.0,
                f"Close {current_close:.2f} not below BB lower {bb_low:.2f}",
            )

        return Signal(
            "BUY",
            CONFIDENCE,
            (
                f"Capitulation: RSI {rsi:.1f} < {RSI_THRESHOLD}, "
                f"body {current_body:.2f} = {current_body / avg_body:.1f}× avg, "
                f"vol {current_vol:.0f} = {current_vol / vol_ma:.1f}× avg, "
                f"close {current_close:.2f} < BB lower {bb_low:.2f}"
            ),
        )
