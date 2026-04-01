
from strategies.base import BaseStrategy, Signal

# Tunable parameters
EMA_FAST_PERIOD = 20
EMA_SLOW_PERIOD = 50
EMA_SLOPE_LOOKBACK = 5
RSI_PERIOD = 14
RSI_LOW = 40.0
RSI_HIGH = 58.0
VOLUME_AVG_PERIOD = 20
PULLBACK_LOWER_PCT = -0.003
PULLBACK_UPPER_PCT = 0.005
REQUIRED_CANDLES = 70


def compute_ema(values: list[float], period: int) -> list[float]:
    """Compute EMA for a list of values. Returns list of same length with
    None for indices before the EMA can be computed."""
    if len(values) < period:
        return [None] * len(values)
    
    multiplier = 2.0 / (period + 1.0)
    ema_values: list[float | None] = [None] * len(values)
    
    # Seed with SMA of first `period` values
    sma = sum(values[:period]) / period
    ema_values[period - 1] = sma
    
    for i in range(period, len(values)):
        prev = ema_values[i - 1]
        ema_values[i] = (values[i] - prev) * multiplier + prev
    
    return ema_values


def compute_rsi(closes: list[float], period: int) -> float | None:
    """Compute the current RSI given a list of close prices.
    Needs at least period + 1 values."""
    if len(closes) < period + 1:
        return None
    
    gains = []
    losses = []
    for i in range(1, len(closes)):
        delta = closes[i] - closes[i - 1]
        gains.append(delta if delta > 0 else 0.0)
        losses.append(-delta if delta < 0 else 0.0)
    
    if len(gains) < period:
        return None
    
    # Initial average using SMA
    avg_gain = sum(gains[:period]) / period
    avg_loss = sum(losses[:period]) / period
    
    # Smoothed (Wilder's) for remaining
    for i in range(period, len(gains)):
        avg_gain = (avg_gain * (period - 1) + gains[i]) / period
        avg_loss = (avg_loss * (period - 1) + losses[i]) / period
    
    if avg_loss == 0:
        return 100.0
    
    rs = avg_gain / avg_loss
    return 100.0 - (100.0 / (1.0 + rs))


def compute_volume_average(candles: list[dict], period: int) -> float | None:
    """Compute simple moving average of volume over the last `period` candles
    ending at the second-to-last candle (so we compare current candle vs prior avg)."""
    if len(candles) < period + 1:
        return None
    # Average over the `period` candles ending one before the last
    vol_slice = [c["volume"] for c in candles[-(period + 1):-1]]
    return sum(vol_slice) / len(vol_slice)


class TrendPullbackDynamicSupportStrategy(BaseStrategy):
    name: str = "Trend Pullback to Dynamic EMA Support"
    timeframe: str = "15m"
    required_candles: int = REQUIRED_CANDLES

    def generate_signal(self, candles: list[dict]) -> Signal:
        closes = [c["close"] for c in candles]

        # Compute EMAs
        ema_fast_series = compute_ema(closes, EMA_FAST_PERIOD)
        ema_slow_series = compute_ema(closes, EMA_SLOW_PERIOD)

        current_ema_fast = ema_fast_series[-1]
        current_ema_slow = ema_slow_series[-1]

        if current_ema_fast is None or current_ema_slow is None:
            return Signal("SKIP", 0.0, "Insufficient data for EMA calculation")

        # Check EMA slope lookback is available
        slope_idx = len(ema_slow_series) - 1 - EMA_SLOPE_LOOKBACK
        if slope_idx < 0 or ema_slow_series[slope_idx] is None:
            return Signal("SKIP", 0.0, "Insufficient data for EMA slope calculation")

        ema_slow_prev = ema_slow_series[slope_idx]

        # --- Condition 1: Trend structure intact ---
        ema_fast_above_slow = current_ema_fast > current_ema_slow
        ema_slow_slope_positive = current_ema_slow > ema_slow_prev

        if not ema_fast_above_slow:
            return Signal("SKIP", 0.0,
                          f"20 EMA ({current_ema_fast:.2f}) not above 50 EMA ({current_ema_slow:.2f}); no uptrend")

        if not ema_slow_slope_positive:
            return Signal("SKIP", 0.0,
                          f"50 EMA slope not positive ({current_ema_slow:.2f} vs {ema_slow_prev:.2f} from {EMA_SLOPE_LOOKBACK} candles ago)")

        # --- Condition 2: Price pulled back to 50 EMA zone ---
        current_close = closes[-1]
        lower_band = current_ema_slow * (1.0 + PULLBACK_LOWER_PCT)
        upper_band = current_ema_slow * (1.0 + PULLBACK_UPPER_PCT)
        in_pullback_zone = lower_band <= current_close <= upper_band

        if not in_pullback_zone:
            pct_from_ema = (current_close - current_ema_slow) / current_ema_slow * 100.0
            return Signal("SKIP", 0.0,
                          f"Price ({current_close:.2f}) not in 50 EMA zone [{lower_band:.2f}, {upper_band:.2f}]; "
                          f"{pct_from_ema:+.2f}% from 50 EMA")

        # --- Condition 3: RSI recovery, not exhaustion ---
        rsi = compute_rsi(closes, RSI_PERIOD)
        if rsi is None:
            return Signal("SKIP", 0.0, "Insufficient data for RSI calculation")

        if rsi < RSI_LOW or rsi > RSI_HIGH:
            return Signal("SKIP", 0.0,
                          f"RSI ({rsi:.1f}) outside recovery zone [{RSI_LOW}, {RSI_HIGH}]")

        # --- Condition 4: Volume below 20-period average ---
        vol_avg = compute_volume_average(candles, VOLUME_AVG_PERIOD)
        if vol_avg is None:
            return Signal("SKIP", 0.0, "Insufficient data for volume average calculation")

        current_volume = candles[-1]["volume"]
        if current_volume >= vol_avg:
            vol_ratio = current_volume / vol_avg if vol_avg > 0 else 999.0
            return Signal("SKIP", 0.0,
                          f"Current volume ({current_volume:.2f}) not below average ({vol_avg:.2f}); "
                          f"ratio={vol_ratio:.2f}")

        # --- All conditions met: compute confidence ---
        # Factor 1: How close price is to 50 EMA (closer = better), 0-1
        max_distance_pct = abs(PULLBACK_LOWER_PCT) + abs(PULLBACK_UPPER_PCT)
        actual_distance_pct = abs((current_close - current_ema_slow) / current_ema_slow)
        proximity_score = max(0.0, 1.0 - (actual_distance_pct / max_distance_pct))

        # Factor 2: RSI centered in the sweet spot (49 ideal), 0-1
        rsi_midpoint = (RSI_LOW + RSI_HIGH) / 2.0
        rsi_range = (RSI_HIGH - RSI_LOW) / 2.0
        rsi_score = max(0.0, 1.0 - abs(rsi - rsi_midpoint) / rsi_range)

        # Factor 3: Lower volume ratio is better, 0-1
        vol_ratio = current_volume / vol_avg if vol_avg > 0 else 1.0
        volume_score = max(0.0, 1.0 - vol_ratio)

        # Factor 4: EMA slope strength, 0-1
        slope_pct = (current_ema_slow - ema_slow_prev) / ema_slow_prev * 100.0
        slope_score = min(1.0, slope_pct / 2.0)  # 2% slope over lookback = perfect score

        # Factor 5: EMA spread (fast vs slow), 0-1
        ema_spread_pct = (current_ema_fast - current_ema_slow) / current_ema_slow * 100.0
        spread_score = min(1.0, ema_spread_pct / 3.0)  # 3% spread = perfect score

        # Weighted combination
        raw_confidence = (
            0.25 * proximity_score +
            0.25 * rsi_score +
            0.20 * volume_score +
            0.15 * slope_score +
            0.15 * spread_score
        )

        confidence = max(0.05, min(1.0, raw_confidence))

        reason = (
            f"Trend pullback BUY: Price {current_close:.2f} near 50 EMA {current_ema_slow:.2f} "
            f"(zone [{lower_band:.2f}-{upper_band:.2f}]); "
            f"20 EMA {current_ema_fast:.2f} > 50 EMA; "
            f"50 EMA slope +{slope_pct:.3f}%; "
            f"RSI {rsi:.1f} (recovery zone); "
            f"Vol {current_volume:.0f} < avg {vol_avg:.0f} (ratio {vol_ratio:.2f})"
        )

        return Signal("BUY", confidence, reason)