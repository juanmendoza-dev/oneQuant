"""Strategy E — VWAP Momentum.

Uses VWAP (reset every 24 hours) combined with volume surge, three
consecutive directional candles, and RSI confirmation to enter trades
in the direction of the dominant intraday move.
"""

from datetime import datetime, timezone

from strategies.base import BaseStrategy, Signal

REQUIRED_CANDLES: int = 30
RSI_PERIOD: int = 14
VOLUME_MA_PERIOD: int = 20
VWAP_MIN_DEVIATION_PCT: float = 0.3   # price must be 0.3%+ away from VWAP
VOLUME_SURGE_MULTIPLIER: float = 2.0  # volume must be 2x the MA
GREEN_RED_LOOKBACK: int = 3           # consecutive candles required
BUY_RSI_LOW: float = 45.0
BUY_RSI_HIGH: float = 65.0
SELL_RSI_LOW: float = 35.0
SELL_RSI_HIGH: float = 55.0
SIGNAL_CONFIDENCE: float = 0.72


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


def _calculate_vwap(candles: list[dict]) -> float:
    """Compute VWAP for candles belonging to the same UTC day as the last candle.

    Resets at UTC midnight. Uses typical price = (high + low + close) / 3.
    Falls back to the full window if all candles share the same day.
    """
    if not candles:
        return 0.0

    last_ts = candles[-1]["timestamp"]
    last_day = datetime.fromtimestamp(last_ts, tz=timezone.utc).date()

    # Filter to candles on the same UTC day as the last candle
    day_candles = [
        c for c in candles
        if datetime.fromtimestamp(c["timestamp"], tz=timezone.utc).date() == last_day
    ]

    # Need at least 2 candles for a meaningful VWAP; fall back to full window
    if len(day_candles) < 2:
        day_candles = candles

    cum_tp_vol = sum(
        ((c["high"] + c["low"] + c["close"]) / 3.0) * c["volume"]
        for c in day_candles
    )
    cum_vol = sum(c["volume"] for c in day_candles)

    if cum_vol == 0.0:
        return candles[-1]["close"]

    return cum_tp_vol / cum_vol


class VWAPMomentumStrategy(BaseStrategy):
    """Trade in the direction of price momentum confirmed by VWAP position,
    volume surge, three consecutive directional candles, and RSI range.

    BUY when:
        - Close > VWAP by 0.3%+
        - Volume > 2x 20-period average
        - Last 3 candles all closed green (close > open)
        - RSI between 45 and 65

    SELL when:
        - Close < VWAP by 0.3%+
        - Volume > 2x 20-period average
        - Last 3 candles all closed red (close < open)
        - RSI between 35 and 55
    """

    name: str = "VWAP Momentum"
    timeframe: str = "15m"
    required_candles: int = REQUIRED_CANDLES

    def generate_signal(self, candles: list[dict]) -> Signal:
        if len(candles) < REQUIRED_CANDLES:
            return Signal("SKIP", 0.0, f"Need {REQUIRED_CANDLES} candles, got {len(candles)}")

        current = candles[-1]
        current_close = current["close"]

        # ── VWAP ─────────────────────────────────────────────────────────────
        vwap = _calculate_vwap(candles)
        if vwap == 0.0:
            return Signal("SKIP", 0.0, "VWAP is zero")

        vwap_dev_pct = ((current_close - vwap) / vwap) * 100.0

        # ── Volume surge ──────────────────────────────────────────────────────
        vol_ma = sum(c["volume"] for c in candles[-VOLUME_MA_PERIOD:]) / VOLUME_MA_PERIOD
        if vol_ma == 0.0:
            return Signal("SKIP", 0.0, "Volume MA is zero")
        vol_ratio = current["volume"] / vol_ma

        if vol_ratio < VOLUME_SURGE_MULTIPLIER:
            return Signal(
                "SKIP", 0.0,
                f"Volume {vol_ratio:.2f}x avg — need {VOLUME_SURGE_MULTIPLIER}x"
            )

        # ── Consecutive directional candles ───────────────────────────────────
        last_three = candles[-GREEN_RED_LOOKBACK:]
        all_green = all(c["close"] > c["open"] for c in last_three)
        all_red = all(c["close"] < c["open"] for c in last_three)

        if not all_green and not all_red:
            return Signal("SKIP", 0.0, "Last 3 candles not all green or all red")

        # ── RSI ───────────────────────────────────────────────────────────────
        closes = [c["close"] for c in candles]
        rsi = _calculate_rsi(closes, RSI_PERIOD)

        # ── BUY conditions ────────────────────────────────────────────────────
        if (
            all_green
            and vwap_dev_pct >= VWAP_MIN_DEVIATION_PCT
            and BUY_RSI_LOW <= rsi <= BUY_RSI_HIGH
        ):
            return Signal(
                "BUY",
                SIGNAL_CONFIDENCE,
                f"Above VWAP +{vwap_dev_pct:.2f}%, vol {vol_ratio:.1f}x, "
                f"3 green, RSI {rsi:.1f}",
            )

        # ── SELL conditions ───────────────────────────────────────────────────
        if (
            all_red
            and vwap_dev_pct <= -VWAP_MIN_DEVIATION_PCT
            and SELL_RSI_LOW <= rsi <= SELL_RSI_HIGH
        ):
            return Signal(
                "SELL",
                SIGNAL_CONFIDENCE,
                f"Below VWAP {vwap_dev_pct:.2f}%, vol {vol_ratio:.1f}x, "
                f"3 red, RSI {rsi:.1f}",
            )

        # ── Diagnostic SKIP ───────────────────────────────────────────────────
        direction = "bullish" if all_green else "bearish" if all_red else "mixed"
        return Signal(
            "SKIP", 0.0,
            f"No setup — VWAP dev {vwap_dev_pct:.2f}%, RSI {rsi:.1f}, "
            f"vol {vol_ratio:.1f}x, candles {direction}"
        )
