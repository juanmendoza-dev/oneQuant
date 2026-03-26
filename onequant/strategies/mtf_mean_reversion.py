"""Strategy — Multi-Timeframe Mean Reversion.

Stricter version of Mean Reversion: fires only when both the 15m AND
the 1h timeframe confirm extreme oversold/overbought conditions.

BUY when ALL are true:
  - 15m RSI < 25 AND price > 1.5% below 20-EMA (base Mean Reversion signal)
  - 15m volume above 10-period average (same as Mean Reversion)
  - 1h RSI < 35 (higher timeframe also oversold)
  - 1h regime is BULL_TREND (only trade bounces in the direction of the trend)

SELL when ALL are true:
  - 15m RSI > 75 AND price > 1.5% above 20-EMA
  - 15m volume above 10-period average
  - 1h RSI > 65 (higher timeframe also overbought)
  - 1h regime is BEAR_TREND

1h candles are pre-loaded once via _preload_data() before the main loop.
1h regime uses the same 200-EMA slope logic as the engine (threshold ±1.5%).

Engine-level allowed_regimes=["BULL_TREND", "BEAR_TREND"] handles 15m regime.
"""

import bisect
import sqlite3

from config import config
from strategies.base import BaseStrategy, Signal

# 15m signal parameters (identical to Mean Reversion)
REQUIRED_CANDLES: int = 21
RSI_PERIOD: int = 14
EMA_PERIOD: int = 20
EMA_DEVIATION_PCT: float = 1.5
RSI_OVERSOLD_15M: float = 25.0
RSI_OVERBOUGHT_15M: float = 75.0
VOLUME_MA_PERIOD: int = 10

# 1h confirmation parameters
RSI_OVERSOLD_1H: float = 35.0
RSI_OVERBOUGHT_1H: float = 65.0
REGIME_EMA_PERIOD: int = 200
REGIME_SLOPE_WINDOW: int = 20
REGIME_BULL_THRESHOLD: float = 0.015
REGIME_BEAR_THRESHOLD: float = -0.015

# Pre-load buffer: how many seconds before start_ts to fetch 1h candles
# 200 1h candles = 720,000 seconds; add 20 more for slope window
_1H_PRELOAD_BUFFER: int = (REGIME_EMA_PERIOD + REGIME_SLOPE_WINDOW) * 3600

BASE_CONFIDENCE: float = 0.65
CONFIDENCE_PER_5_RSI: float = 0.10
MAX_CONFIDENCE: float = 0.92


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
    """Causal EMA series — index i uses only values[0..i]."""
    n = len(values)
    result = [0.0] * n
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


class MTFMeanReversionStrategy(BaseStrategy):
    """Mean Reversion confirmed by 1h RSI and 1h regime — fewer but higher-quality trades."""

    name: str = "MTF Mean Reversion"
    timeframe: str = "15m"
    required_candles: int = REQUIRED_CANDLES

    def __init__(self) -> None:
        # Pre-loaded 1h data — all arrays are index-aligned
        self._1h_timestamps: list[int] = []    # sorted ascending
        self._1h_rsi: list[float] = []          # precomputed RSI at each 1h index
        self._1h_regime: list[str] = []         # precomputed regime at each 1h index

    def _preload_data(self, start_ts: int, end_ts: int) -> None:
        """Load 1h candles and precompute RSI + regime series in one pass."""
        fetch_from = start_ts - _1H_PRELOAD_BUFFER
        conn = sqlite3.connect(config.DATABASE_PATH)
        try:
            rows = conn.execute(
                "SELECT timestamp, close FROM btc_candles "
                "WHERE symbol = 'BTC-USD' AND timeframe = '1h' "
                "AND timestamp >= ? AND timestamp <= ? "
                "ORDER BY timestamp ASC",
                (fetch_from, end_ts),
            ).fetchall()
        finally:
            conn.close()

        if not rows:
            return

        timestamps = [r[0] for r in rows]
        closes = [r[1] for r in rows]
        n = len(closes)

        # --- Precompute 1h RSI (Wilder's incremental method) ----------------
        rsi_series = [50.0] * n
        if n >= RSI_PERIOD + 1:
            deltas = [closes[i] - closes[i - 1] for i in range(1, n)]
            gains = [max(d, 0.0) for d in deltas]
            losses = [abs(min(d, 0.0)) for d in deltas]
            avg_gain = sum(gains[:RSI_PERIOD]) / RSI_PERIOD
            avg_loss = sum(losses[:RSI_PERIOD]) / RSI_PERIOD
            # Store RSI at index RSI_PERIOD (first valid RSI)
            if avg_loss > 0:
                rsi_series[RSI_PERIOD] = 100.0 - (100.0 / (1.0 + avg_gain / avg_loss))
            else:
                rsi_series[RSI_PERIOD] = 100.0
            for i in range(RSI_PERIOD, n - 1):
                avg_gain = (avg_gain * (RSI_PERIOD - 1) + gains[i]) / RSI_PERIOD
                avg_loss = (avg_loss * (RSI_PERIOD - 1) + losses[i]) / RSI_PERIOD
                if avg_loss > 0:
                    rsi_series[i + 1] = 100.0 - (100.0 / (1.0 + avg_gain / avg_loss))
                else:
                    rsi_series[i + 1] = 100.0

        # --- Precompute 1h 200-EMA series -----------------------------------
        ema200 = _ema_series(closes, REGIME_EMA_PERIOD)

        # --- Precompute 1h regime at each index -----------------------------
        min_regime_idx = REGIME_EMA_PERIOD + REGIME_SLOPE_WINDOW - 1
        regime_series = ["UNKNOWN"] * n
        for i in range(min_regime_idx, n):
            ema_now = ema200[i]
            ema_prev = ema200[i - REGIME_SLOPE_WINDOW]
            if ema_prev == 0:
                continue
            slope = (ema_now - ema_prev) / ema_prev
            if slope > REGIME_BULL_THRESHOLD:
                regime_series[i] = "BULL_TREND"
            elif slope < REGIME_BEAR_THRESHOLD:
                regime_series[i] = "BEAR_TREND"
            else:
                regime_series[i] = "RANGING"

        self._1h_timestamps = timestamps
        self._1h_rsi = rsi_series
        self._1h_regime = regime_series

    def _1h_context(self, current_ts: int) -> tuple[float, str] | None:
        """Return (1h_rsi, 1h_regime) for the most recent 1h candle at or before current_ts."""
        if not self._1h_timestamps:
            return None
        idx = bisect.bisect_right(self._1h_timestamps, current_ts) - 1
        if idx < 0:
            return None
        return (self._1h_rsi[idx], self._1h_regime[idx])

    def generate_signal(self, candles: list[dict]) -> Signal:
        if len(candles) < REQUIRED_CANDLES:
            return Signal("SKIP", 0.0, f"Need {REQUIRED_CANDLES} candles, got {len(candles)}")

        closes = [c["close"] for c in candles]
        rsi_15m = _calculate_rsi(closes, RSI_PERIOD)
        ema_15m = _calculate_ema(closes, EMA_PERIOD)
        current_close = closes[-1]
        current_ts = candles[-1]["timestamp"]

        if ema_15m == 0.0:
            return Signal("SKIP", 0.0, "EMA15m is zero")

        deviation_pct = ((current_close - ema_15m) / ema_15m) * 100.0

        # 15m volume confirmation
        volumes = [c["volume"] for c in candles[-VOLUME_MA_PERIOD:]]
        vol_ma = sum(volumes) / VOLUME_MA_PERIOD
        current_vol = candles[-1]["volume"]
        if vol_ma > 0 and current_vol <= vol_ma:
            return Signal("SKIP", 0.0, f"15m vol {current_vol:.0f} below avg {vol_ma:.0f}")

        # 1h confirmation
        ctx = self._1h_context(current_ts)
        if ctx is None:
            return Signal("SKIP", 0.0, "No 1h data available")
        rsi_1h, regime_1h = ctx

        # ── BUY ─────────────────────────────────────────────────────────────
        if rsi_15m < RSI_OVERSOLD_15M and deviation_pct < -EMA_DEVIATION_PCT:
            if rsi_1h >= RSI_OVERSOLD_1H:
                return Signal(
                    "SKIP", 0.0,
                    f"15m setup OK but 1h RSI {rsi_1h:.1f} not < {RSI_OVERSOLD_1H}",
                )
            if regime_1h != "BULL_TREND":
                return Signal(
                    "SKIP", 0.0,
                    f"15m setup OK but 1h regime {regime_1h} (need BULL_TREND)",
                )
            points_beyond = RSI_OVERSOLD_15M - rsi_15m
            extra = (points_beyond // 5) * CONFIDENCE_PER_5_RSI
            confidence = min(BASE_CONFIDENCE + extra, MAX_CONFIDENCE)
            return Signal(
                "BUY",
                confidence,
                (
                    f"MTF buy: 15m RSI {rsi_15m:.1f}, dev {deviation_pct:.2f}%, "
                    f"1h RSI {rsi_1h:.1f}, 1h {regime_1h}"
                ),
            )

        # ── SELL ────────────────────────────────────────────────────────────
        if rsi_15m > RSI_OVERBOUGHT_15M and deviation_pct > EMA_DEVIATION_PCT:
            if rsi_1h <= RSI_OVERBOUGHT_1H:
                return Signal(
                    "SKIP", 0.0,
                    f"15m setup OK but 1h RSI {rsi_1h:.1f} not > {RSI_OVERBOUGHT_1H}",
                )
            if regime_1h != "BEAR_TREND":
                return Signal(
                    "SKIP", 0.0,
                    f"15m setup OK but 1h regime {regime_1h} (need BEAR_TREND)",
                )
            points_beyond = rsi_15m - RSI_OVERBOUGHT_15M
            extra = (points_beyond // 5) * CONFIDENCE_PER_5_RSI
            confidence = min(BASE_CONFIDENCE + extra, MAX_CONFIDENCE)
            return Signal(
                "SELL",
                confidence,
                (
                    f"MTF sell: 15m RSI {rsi_15m:.1f}, dev +{deviation_pct:.2f}%, "
                    f"1h RSI {rsi_1h:.1f}, 1h {regime_1h}"
                ),
            )

        return Signal("SKIP", 0.0, f"15m RSI {rsi_15m:.1f}, dev {deviation_pct:.2f}% — no setup")
