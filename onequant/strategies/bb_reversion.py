"""Strategy F — Bollinger Band Reversion.

Trades mean reversion when price touches a band during low-volatility
(tight bandwidth) ranging conditions, confirmed by RSI and absence of
a volume surge.
"""

import math

from strategies.base import BaseStrategy, Signal

REQUIRED_CANDLES: int = 25
BB_PERIOD: int = 20
BB_STD_MULT: float = 2.0
BANDWIDTH_THRESHOLD: float = 0.04   # tight bands = ranging market
RSI_PERIOD: int = 14
RSI_BUY_MAX: float = 40.0           # oversold on lower touch
RSI_SELL_MIN: float = 60.0          # overbought on upper touch
VOLUME_MA_PERIOD: int = 20
VOLUME_SURGE_MULT: float = 1.5      # reject if volume is surging
BASE_CONFIDENCE: float = 0.70


def _calculate_sma_std(values: list[float], period: int) -> tuple[float, float]:
    """Return (sma, std_dev) over the last `period` values."""
    window = values[-period:]
    sma = sum(window) / period
    variance = sum((v - sma) ** 2 for v in window) / period
    return sma, math.sqrt(variance)


def _calculate_rsi(closes: list[float], period: int) -> float:
    """Compute RSI using Wilder smoothing."""
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


class BBReversionStrategy(BaseStrategy):
    """Buy at the lower Bollinger Band, sell at the upper band.

    Only fires when bands are tight (bandwidth < 0.04) — confirming a
    ranging, low-volatility environment. Volume must NOT be surging to
    rule out breakout conditions. RSI confirms the extreme.
    """

    name: str = "BB Reversion"
    timeframe: str = "15m"
    required_candles: int = REQUIRED_CANDLES

    def generate_signal(self, candles: list[dict]) -> Signal:
        if len(candles) < REQUIRED_CANDLES:
            return Signal("SKIP", 0.0, f"Need {REQUIRED_CANDLES} candles, got {len(candles)}")

        closes = [c["close"] for c in candles]
        current_close = closes[-1]

        # ── Bollinger Bands ───────────────────────────────────────────────────
        sma, std = _calculate_sma_std(closes, BB_PERIOD)
        if sma == 0.0:
            return Signal("SKIP", 0.0, "SMA is zero")

        upper = sma + BB_STD_MULT * std
        lower = sma - BB_STD_MULT * std
        bandwidth = (upper - lower) / sma

        # ── Bandwidth filter — only trade tight, ranging bands ────────────────
        if bandwidth >= BANDWIDTH_THRESHOLD:
            return Signal(
                "SKIP", 0.0,
                f"Bandwidth {bandwidth:.4f} >= {BANDWIDTH_THRESHOLD} — bands too wide"
            )

        # ── Volume — reject surges (would signal breakout, not reversion) ─────
        vol_ma = sum(c["volume"] for c in candles[-VOLUME_MA_PERIOD:]) / VOLUME_MA_PERIOD
        current_vol = candles[-1]["volume"]
        vol_ratio = current_vol / vol_ma if vol_ma > 0 else 0.0

        if vol_ratio >= VOLUME_SURGE_MULT:
            return Signal(
                "SKIP", 0.0,
                f"Volume surge {vol_ratio:.2f}x — possible breakout, not reversion"
            )

        # ── RSI ───────────────────────────────────────────────────────────────
        rsi = _calculate_rsi(closes, RSI_PERIOD)

        # ── BUY: price at or below lower band, RSI oversold ───────────────────
        if current_close <= lower and rsi < RSI_BUY_MAX:
            dev = ((current_close - lower) / lower) * 100.0
            return Signal(
                "BUY",
                BASE_CONFIDENCE,
                f"At lower band ({lower:.0f}), dev {dev:.2f}%, "
                f"RSI {rsi:.1f}, BW {bandwidth:.4f}, vol {vol_ratio:.2f}x"
            )

        # ── SELL: price at or above upper band, RSI overbought ────────────────
        if current_close >= upper and rsi > RSI_SELL_MIN:
            dev = ((current_close - upper) / upper) * 100.0
            return Signal(
                "SELL",
                BASE_CONFIDENCE,
                f"At upper band ({upper:.0f}), dev +{dev:.2f}%, "
                f"RSI {rsi:.1f}, BW {bandwidth:.4f}, vol {vol_ratio:.2f}x"
            )

        return Signal(
            "SKIP", 0.0,
            f"Price {current_close:.0f} within bands [{lower:.0f}, {upper:.0f}], "
            f"RSI {rsi:.1f}, BW {bandwidth:.4f}"
        )
