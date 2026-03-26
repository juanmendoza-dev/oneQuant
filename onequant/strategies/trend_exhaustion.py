"""Strategy — Trend Exhaustion.

Fires when a strong trend is losing momentum: extreme RSI extension,
price far from EMA, declining volume, and shrinking candle bodies.

SELL signal in BULL_TREND — ALL must be true:
  - RSI > 80 (extreme overbought)
  - Price > 4% above 20-period EMA (very extended)
  - Volume declining for last 3 consecutive candles
  - Current candle body < all of previous 3 candle bodies (slowing momentum)

BUY signal in BEAR_TREND — mirror of above:
  - RSI < 20 (extreme oversold)
  - Price > 4% below 20-period EMA
  - Volume declining for last 3 candles
  - Current candle body < all of previous 3 candle bodies

Regime is detected internally using 200-period EMA slope over the
provided 225-candle window (mirrors the engine's regime logic).
Engine-level allowed_regimes filters to BULL_TREND and BEAR_TREND.

SL: 3%, TP: 5%
"""

from strategies.base import BaseStrategy, Signal

REQUIRED_CANDLES: int = 225   # 200 (EMA) + 20 (slope window) + 5 buffer
RSI_PERIOD: int = 14
EMA_PERIOD: int = 20
EMA_EXTENSION_PCT: float = 4.0   # price must be > 4% from EMA
RSI_OVERBOUGHT: float = 80.0
RSI_OVERSOLD: float = 20.0
REGIME_EMA_PERIOD: int = 200
REGIME_SLOPE_WINDOW: int = 20
REGIME_BULL_THRESHOLD: float = 0.015
REGIME_BEAR_THRESHOLD: float = -0.015
CONFIDENCE: float = 0.80


def _calculate_rsi(closes: list[float], period: int) -> float:
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


def _calculate_ema(values: list[float], period: int) -> float:
    if len(values) < period:
        return sum(values) / len(values)
    multiplier = 2.0 / (period + 1)
    ema = sum(values[:period]) / period
    for val in values[period:]:
        ema = (val - ema) * multiplier + ema
    return ema


def _ema_series(values: list[float], period: int) -> list[float]:
    """Compute EMA at every index (causal — index i uses only values[0..i])."""
    n = len(values)
    result = [0.0] * n
    if n == 0:
        return result
    running = 0.0
    for i in range(min(period - 1, n)):
        running += values[i]
        result[i] = running / (i + 1)
    if n < period:
        return result
    sma = (running + values[period - 1]) / period
    result[period - 1] = sma
    mult = 2.0 / (period + 1)
    ema = sma
    for i in range(period, n):
        ema = (values[i] - ema) * mult + ema
        result[i] = ema
    return result


def _detect_regime(closes: list[float]) -> str:
    """Detect regime from 200-EMA slope over the full closes window."""
    ema200 = _ema_series(closes, REGIME_EMA_PERIOD)
    last = len(closes) - 1
    if last < REGIME_EMA_PERIOD + REGIME_SLOPE_WINDOW - 1:
        return "UNKNOWN"
    ema_now = ema200[last]
    ema_prev = ema200[last - REGIME_SLOPE_WINDOW]
    if ema_prev == 0:
        return "UNKNOWN"
    slope = (ema_now - ema_prev) / ema_prev
    if slope > REGIME_BULL_THRESHOLD:
        return "BULL_TREND"
    if slope < REGIME_BEAR_THRESHOLD:
        return "BEAR_TREND"
    return "RANGING"


class TrendExhaustionStrategy(BaseStrategy):
    """Short overbought BULL trends and buy oversold BEAR trends at exhaustion."""

    name: str = "Trend Exhaustion"
    timeframe: str = "15m"
    required_candles: int = REQUIRED_CANDLES

    def generate_signal(self, candles: list[dict]) -> Signal:
        if len(candles) < REQUIRED_CANDLES:
            return Signal("SKIP", 0.0, f"Need {REQUIRED_CANDLES} candles, got {len(candles)}")

        closes = [c["close"] for c in candles]
        volumes = [c["volume"] for c in candles]

        # Detect regime from full window
        regime = _detect_regime(closes)
        if regime not in ("BULL_TREND", "BEAR_TREND"):
            return Signal("SKIP", 0.0, f"Regime {regime} — need BULL or BEAR trend")

        current_close = closes[-1]
        rsi = _calculate_rsi(closes[-30:], RSI_PERIOD)   # RSI from recent 30 for accuracy
        ema20 = _calculate_ema(closes[-EMA_PERIOD * 2:], EMA_PERIOD)

        if ema20 == 0:
            return Signal("SKIP", 0.0, "EMA20 is zero")

        deviation_pct = ((current_close - ema20) / ema20) * 100.0

        # Volume declining: each of last 3 candles lower than the one before
        v1, v2, v3 = volumes[-4], volumes[-3], volumes[-2]  # prior 3 closed candles
        vol_declining = v2 < v1 and v3 < v2

        # Candle bodies: current body < all of prior 3 bodies
        def body(c: dict) -> float:
            return abs(c["close"] - c["open"])

        b_curr = body(candles[-1])
        b_prev1 = body(candles[-2])
        b_prev2 = body(candles[-3])
        b_prev3 = body(candles[-4])
        momentum_slowing = b_curr < b_prev1 and b_curr < b_prev2 and b_curr < b_prev3

        # ── BULL_TREND → SELL signal ─────────────────────────────────────────
        if regime == "BULL_TREND":
            if rsi <= RSI_OVERBOUGHT:
                return Signal("SKIP", 0.0, f"BULL: RSI {rsi:.1f} not > {RSI_OVERBOUGHT}")
            if deviation_pct <= EMA_EXTENSION_PCT:
                return Signal(
                    "SKIP", 0.0,
                    f"BULL: price only {deviation_pct:.2f}% above EMA (need > {EMA_EXTENSION_PCT}%)",
                )
            if not vol_declining:
                return Signal("SKIP", 0.0, f"BULL: volume not declining (v={v1:.0f},{v2:.0f},{v3:.0f})")
            if not momentum_slowing:
                return Signal(
                    "SKIP", 0.0,
                    f"BULL: body not shrinking (curr={b_curr:.2f}, prev={b_prev1:.2f},{b_prev2:.2f},{b_prev3:.2f})",
                )
            return Signal(
                "SELL",
                CONFIDENCE,
                (
                    f"Trend exhaustion (BULL→SELL): RSI {rsi:.1f}, "
                    f"price +{deviation_pct:.2f}% above EMA, "
                    f"vol declining, body shrinking"
                ),
            )

        # ── BEAR_TREND → BUY signal ──────────────────────────────────────────
        if rsi >= RSI_OVERSOLD:
            return Signal("SKIP", 0.0, f"BEAR: RSI {rsi:.1f} not < {RSI_OVERSOLD}")
        if deviation_pct >= -EMA_EXTENSION_PCT:
            return Signal(
                "SKIP", 0.0,
                f"BEAR: price only {deviation_pct:.2f}% below EMA (need < -{EMA_EXTENSION_PCT}%)",
            )
        if not vol_declining:
            return Signal("SKIP", 0.0, f"BEAR: volume not declining (v={v1:.0f},{v2:.0f},{v3:.0f})")
        if not momentum_slowing:
            return Signal(
                "SKIP", 0.0,
                f"BEAR: body not shrinking (curr={b_curr:.2f}, prev={b_prev1:.2f},{b_prev2:.2f},{b_prev3:.2f})",
            )
        return Signal(
            "BUY",
            CONFIDENCE,
            (
                f"Trend exhaustion (BEAR→BUY): RSI {rsi:.1f}, "
                f"price {deviation_pct:.2f}% below EMA, "
                f"vol declining, body shrinking"
            ),
        )
