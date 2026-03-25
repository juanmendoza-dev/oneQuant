"""Cross-validation -- compare our engine against backtesting.py.

Implements the Mean Reversion strategy in both our backtest engine and
in the backtesting.py framework, runs them on identical synthetic data,
and asserts that trade count, win rate, and profit factor all agree
within 15%.

Usage:
    cd oneQuant
    python tests/test_cross_validation.py
"""

import os
import random
import sqlite3
import sys
import tempfile

# ---------------------------------------------------------------------------
# Path & env setup
# ---------------------------------------------------------------------------
sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "onequant"))
os.environ.setdefault("COINBASE_API_KEY", "test")
os.environ.setdefault("COINBASE_API_SECRET", "test")

if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8")

import pandas as pd  # noqa: E402
from backtesting import Strategy  # noqa: E402
from backtesting.lib import FractionalBacktest as Backtest  # noqa: E402

from backtest.engine import BacktestConfig, run_backtest  # noqa: E402
from config import config  # noqa: E402
from strategies.base import BaseStrategy, Signal  # noqa: E402

# ---------------------------------------------------------------------------
# Shared parameters (used by BOTH engines)
# ---------------------------------------------------------------------------
RSI_PERIOD = 14
RSI_OVERSOLD = 30.0
RSI_OVERBOUGHT = 70.0
EMA_PERIOD = 20
EMA_DEVIATION_PCT = 1.5
VOLUME_MA_PERIOD = 10
SL_PCT = 0.05
TP_PCT = 0.05
FEE_PCT = 0.002       # 0.2% flat per side — low to isolate trade mechanics
POSITION_SIZE = 0.10   # 10% of equity
MIN_CONFIDENCE = 0.55
INITIAL_CAPITAL = 1000.0
BASE_CONFIDENCE = 0.60
CONFIDENCE_PER_5_RSI = 0.10
MAX_CONFIDENCE = 0.90


# ---------------------------------------------------------------------------
# Indicator functions (shared between both engines)
# ---------------------------------------------------------------------------


def calc_rsi(closes: list[float], period: int = RSI_PERIOD) -> float:
    """Wilder-smoothed RSI — returns final value only."""
    if len(closes) < period + 1:
        return 50.0
    deltas = [closes[i] - closes[i - 1] for i in range(1, len(closes))]
    gains = [max(d, 0.0) for d in deltas]
    losses = [abs(min(d, 0.0)) for d in deltas]

    avg_g = sum(gains[:period]) / period
    avg_l = sum(losses[:period]) / period
    for i in range(period, len(deltas)):
        avg_g = (avg_g * (period - 1) + gains[i]) / period
        avg_l = (avg_l * (period - 1) + losses[i]) / period

    if avg_l == 0:
        return 100.0
    return 100.0 - 100.0 / (1.0 + avg_g / avg_l)


def calc_rsi_series(closes, period: int = RSI_PERIOD) -> list[float]:
    """Wilder-smoothed RSI — full series (for backtesting.py self.I)."""
    closes = list(closes)
    n = len(closes)
    out = [50.0] * n
    if n < period + 1:
        return out

    deltas = [closes[i] - closes[i - 1] for i in range(1, n)]
    gains = [max(d, 0.0) for d in deltas]
    losses = [abs(min(d, 0.0)) for d in deltas]

    avg_g = sum(gains[:period]) / period
    avg_l = sum(losses[:period]) / period

    if avg_l == 0:
        out[period] = 100.0
    else:
        out[period] = 100.0 - 100.0 / (1.0 + avg_g / avg_l)

    for i in range(period, len(deltas)):
        avg_g = (avg_g * (period - 1) + gains[i]) / period
        avg_l = (avg_l * (period - 1) + losses[i]) / period
        if avg_l == 0:
            out[i + 1] = 100.0
        else:
            out[i + 1] = 100.0 - 100.0 / (1.0 + avg_g / avg_l)
    return out


def calc_ema(values: list[float], period: int = EMA_PERIOD) -> float:
    """EMA — returns final value only."""
    if len(values) < period:
        return sum(values) / len(values)
    m = 2.0 / (period + 1)
    ema = sum(values[:period]) / period
    for v in values[period:]:
        ema = (v - ema) * m + ema
    return ema


def calc_ema_series(values, period: int = EMA_PERIOD) -> list[float]:
    """EMA — full series (for backtesting.py self.I)."""
    values = list(values)
    n = len(values)
    out = [0.0] * n
    if n == 0:
        return out
    m = 2.0 / (period + 1)
    if n < period:
        s = 0.0
        for i in range(n):
            s += values[i]
            out[i] = s / (i + 1)
        return out
    s = 0.0
    for i in range(period - 1):
        s += values[i]
        out[i] = s / (i + 1)
    sma = (s + values[period - 1]) / period
    out[period - 1] = sma
    ema = sma
    for i in range(period, n):
        ema = (values[i] - ema) * m + ema
        out[i] = ema
    return out


def calc_vol_ma_series(volumes, period: int = VOLUME_MA_PERIOD) -> list[float]:
    """Simple moving average of volume — full series."""
    volumes = list(volumes)
    n = len(volumes)
    out = [0.0] * n
    s = 0.0
    for i in range(n):
        s += volumes[i]
        if i >= period:
            s -= volumes[i - period]
            out[i] = s / period
        else:
            out[i] = s / (i + 1)
    return out


# ---------------------------------------------------------------------------
# Synthetic data — mean-reverting process for sufficient signal density
# ---------------------------------------------------------------------------


def generate_mr_candles(n: int = 10000, seed: int = 99) -> list[dict]:
    """Ornstein-Uhlenbeck process around $50k, high volatility."""
    rng = random.Random(seed)
    candles: list[dict] = []
    price = 50_000.0
    mu = 50_000.0
    theta = 0.002  # mean-reversion speed
    sigma = 250.0  # per-step volatility
    start_ts = 1_700_000_000
    interval = 900

    for i in range(n):
        open_price = price
        p = open_price
        prices = [p]
        for _ in range(4):
            dp = theta * (mu - p) + rng.gauss(0, sigma)
            p += dp
            if p < 1000:
                p = 1000.0
            prices.append(p)

        close_price = prices[-1]
        high = max(prices) * (1.0 + rng.uniform(0, 0.0003))
        low = min(prices) * (1.0 - rng.uniform(0, 0.0003))

        # Volume clusters high near price extremes (distance from mean)
        dev = abs(price - mu) / mu
        vol = 100.0 + dev * 3000.0 + rng.uniform(0, 100)

        candles.append({
            "timestamp": start_ts + i * interval,
            "open": round(open_price, 2),
            "high": round(high, 2),
            "low": round(low, 2),
            "close": round(close_price, 2),
            "volume": round(vol, 2),
        })
        price = close_price

    return candles


def create_test_db(candles: list[dict], timeframe: str = "15m") -> str:
    """Write candles into a throwaway SQLite DB."""
    db_path = tempfile.mktemp(suffix=".db")
    conn = sqlite3.connect(db_path)
    conn.execute(
        "CREATE TABLE btc_candles ("
        "  timestamp INTEGER, open REAL, high REAL, low REAL,"
        "  close REAL, volume REAL, timeframe TEXT,"
        "  UNIQUE(timestamp, timeframe))"
    )
    for c in candles:
        conn.execute(
            "INSERT INTO btc_candles VALUES (?,?,?,?,?,?,?)",
            (c["timestamp"], c["open"], c["high"], c["low"],
             c["close"], c["volume"], timeframe),
        )
    conn.commit()
    conn.close()
    return db_path


# ---------------------------------------------------------------------------
# OUR ENGINE — Mean Reversion strategy
# ---------------------------------------------------------------------------


class MeanReversionOurs(BaseStrategy):
    """Mean Reversion for our engine, using shared parameters."""

    name = "MeanReversionCV"
    timeframe = "15m"
    required_candles = max(RSI_PERIOD + 1, EMA_PERIOD + 1, VOLUME_MA_PERIOD + 1)

    def generate_signal(self, candles: list[dict]) -> Signal:
        if len(candles) < self.required_candles:
            return Signal("SKIP", 0.0, "not enough candles")

        closes = [c["close"] for c in candles]
        rsi = calc_rsi(closes, RSI_PERIOD)
        ema = calc_ema(closes, EMA_PERIOD)
        current_close = closes[-1]

        if ema == 0:
            return Signal("SKIP", 0.0, "ema zero")

        dev_pct = ((current_close - ema) / ema) * 100.0

        # Volume confirmation
        volumes = [c["volume"] for c in candles[-VOLUME_MA_PERIOD:]]
        vol_ma = sum(volumes) / len(volumes)
        if vol_ma > 0 and candles[-1]["volume"] <= vol_ma:
            return Signal("SKIP", 0.0, "volume below avg")

        if rsi < RSI_OVERSOLD and dev_pct < -EMA_DEVIATION_PCT:
            pts = RSI_OVERSOLD - rsi
            extra = (pts // 5) * CONFIDENCE_PER_5_RSI
            conf = min(BASE_CONFIDENCE + extra, MAX_CONFIDENCE)
            return Signal("BUY", conf, f"RSI {rsi:.1f} oversold")

        if rsi > RSI_OVERBOUGHT and dev_pct > EMA_DEVIATION_PCT:
            pts = rsi - RSI_OVERBOUGHT
            extra = (pts // 5) * CONFIDENCE_PER_5_RSI
            conf = min(BASE_CONFIDENCE + extra, MAX_CONFIDENCE)
            return Signal("SELL", conf, f"RSI {rsi:.1f} overbought")

        return Signal("SKIP", 0.0, "no setup")


# ---------------------------------------------------------------------------
# BACKTESTING.PY — Mean Reversion strategy
# ---------------------------------------------------------------------------


class MeanReversionBT(Strategy):
    """Mean Reversion in backtesting.py, same logic as our engine.

    Computes RSI/EMA from a rolling window of the same size our engine
    passes to generate_signal (required_candles), so both engines see
    identical indicator values.
    """
    _window = max(RSI_PERIOD + 1, EMA_PERIOD + 1, VOLUME_MA_PERIOD + 1)

    def init(self):
        pass  # indicators computed per-bar from rolling window

    def next(self):
        if self.position:
            return

        n_bars = len(self.data.Close)
        if n_bars < self._window:
            return

        # Use the same rolling window our engine passes to generate_signal
        closes = list(self.data.Close[-self._window:])
        volumes = list(self.data.Volume[-VOLUME_MA_PERIOD:])

        rsi = calc_rsi(closes, RSI_PERIOD)
        ema = calc_ema(closes, EMA_PERIOD)
        close = closes[-1]

        if ema == 0:
            return

        dev_pct = ((close - ema) / ema) * 100.0

        # Volume confirmation
        vol_ma = sum(volumes) / len(volumes)
        vol = volumes[-1]
        if vol_ma > 0 and vol <= vol_ma:
            return

        # Confidence check
        if rsi < RSI_OVERSOLD and dev_pct < -EMA_DEVIATION_PCT:
            pts = RSI_OVERSOLD - rsi
            extra = (pts // 5) * CONFIDENCE_PER_5_RSI
            conf = min(BASE_CONFIDENCE + extra, MAX_CONFIDENCE)
            if conf < MIN_CONFIDENCE:
                return
            sl = close * (1.0 - SL_PCT)
            tp = close * (1.0 + TP_PCT)
            self.buy(sl=sl, tp=tp, size=POSITION_SIZE)

        elif rsi > RSI_OVERBOUGHT and dev_pct > EMA_DEVIATION_PCT:
            pts = rsi - RSI_OVERBOUGHT
            extra = (pts // 5) * CONFIDENCE_PER_5_RSI
            conf = min(BASE_CONFIDENCE + extra, MAX_CONFIDENCE)
            if conf < MIN_CONFIDENCE:
                return
            sl = close * (1.0 + SL_PCT)
            tp = close * (1.0 - TP_PCT)
            self.sell(sl=sl, tp=tp, size=POSITION_SIZE)


# ---------------------------------------------------------------------------
# Comparison helpers
# ---------------------------------------------------------------------------


def _pct_diff(a: float, b: float) -> float:
    """Symmetric percentage difference."""
    avg = (abs(a) + abs(b)) / 2.0
    if avg == 0:
        return 0.0
    return abs(a - b) / avg


def _compute_pf(trades_pnl: list[float]) -> float:
    """Profit factor from a list of P&L values."""
    gross_win = sum(p for p in trades_pnl if p > 0)
    gross_loss = abs(sum(p for p in trades_pnl if p < 0))
    if gross_loss == 0:
        return float("inf") if gross_win > 0 else 0.0
    return gross_win / gross_loss


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------


def run_cross_validation() -> bool:
    """Run both engines on identical data and compare metrics."""
    print("=" * 55)
    print("  Cross-Validation: our engine vs backtesting.py")
    print("=" * 55)
    print("  Generating 10 000 synthetic candles (OU process)...\n")

    candles = generate_mr_candles(n=10000, seed=99)

    # ── Run OUR engine ────────────────────────────────────────────────────
    db_path = create_test_db(candles)
    original_db = config.DATABASE_PATH
    object.__setattr__(config, "DATABASE_PATH", db_path)

    try:
        cfg = BacktestConfig(
            strategy=MeanReversionOurs(),
            timeframe="15m",
            initial_capital=INITIAL_CAPITAL,
            position_size_pct=POSITION_SIZE,
            stop_loss_pct=SL_PCT,
            take_profit_pct=TP_PCT,
            min_confidence=MIN_CONFIDENCE,
            slippage_pct=0.0,
            spread_pct=0.0,
            fee_pct=FEE_PCT,
        )
        our_result = run_backtest(cfg)
    finally:
        object.__setattr__(config, "DATABASE_PATH", original_db)
        try:
            os.unlink(db_path)
        except OSError:
            pass

    our_trades = our_result.trades
    our_count = len(our_trades)
    our_wins = sum(1 for t in our_trades if t.outcome == "WIN")
    our_wr = our_wins / our_count if our_count else 0.0
    our_pnls = [t.pnl for t in our_trades]
    our_pf = _compute_pf(our_pnls)

    print(f"  Our engine:       {our_count} trades,  WR {our_wr * 100:.1f}%,  PF {our_pf:.3f}")

    # ── Run BACKTESTING.PY ────────────────────────────────────────────────
    df = pd.DataFrame(candles)
    df["Date"] = pd.to_datetime(df["timestamp"], unit="s")
    df = df.set_index("Date")
    df = df.rename(columns={
        "open": "Open", "high": "High", "low": "Low",
        "close": "Close", "volume": "Volume",
    })
    df = df[["Open", "High", "Low", "Close", "Volume"]]

    bt = Backtest(
        df,
        MeanReversionBT,
        cash=INITIAL_CAPITAL,
        commission=FEE_PCT,
        trade_on_close=False,
        hedging=False,
        exclusive_orders=False,
    )
    bt_stats = bt.run()
    bt_trades_df = bt_stats._trades

    bt_count = len(bt_trades_df)
    if bt_count > 0:
        bt_wins = int((bt_trades_df["PnL"] > 0).sum())
        bt_wr = bt_wins / bt_count
        bt_pf = _compute_pf(bt_trades_df["PnL"].tolist())
    else:
        bt_wins = 0
        bt_wr = 0.0
        bt_pf = 0.0

    print(f"  backtesting.py:   {bt_count} trades,  WR {bt_wr * 100:.1f}%,  PF {bt_pf:.3f}")
    print()

    # ── Guard: need enough trades ─────────────────────────────────────────
    if our_count < 5 or bt_count < 5:
        print("  WARNING: fewer than 5 trades in one or both engines.")
        print("  Cannot make a meaningful comparison. Increase data size.")
        return False

    # ── Compare ───────────────────────────────────────────────────────────
    THRESHOLD = 0.15
    diverged: list[str] = []

    diff_count = _pct_diff(our_count, bt_count)
    diff_wr = _pct_diff(our_wr, bt_wr)
    diff_pf = _pct_diff(our_pf, bt_pf)

    print(f"  Trade count diff:    {diff_count * 100:.1f}%  "
          f"({'OK' if diff_count <= THRESHOLD else 'DIVERGED'})")
    print(f"  Win rate diff:       {diff_wr * 100:.1f}%  "
          f"({'OK' if diff_wr <= THRESHOLD else 'DIVERGED'})")
    print(f"  Profit factor diff:  {diff_pf * 100:.1f}%  "
          f"({'OK' if diff_pf <= THRESHOLD else 'DIVERGED'})")
    print()

    if diff_count > THRESHOLD:
        diverged.append(f"trade count ({our_count} vs {bt_count}, diff {diff_count * 100:.1f}%)")
    if diff_wr > THRESHOLD:
        diverged.append(f"win rate ({our_wr * 100:.1f}% vs {bt_wr * 100:.1f}%, diff {diff_wr * 100:.1f}%)")
    if diff_pf > THRESHOLD:
        diverged.append(f"profit factor ({our_pf:.3f} vs {bt_pf:.3f}, diff {diff_pf * 100:.1f}%)")

    if diverged:
        print("  ENGINE CROSS-VALIDATION FAILED")
        for d in diverged:
            print(f"    DIVERGED: {d}")
        return False

    print("  ENGINE CROSS-VALIDATION PASSED")
    return True


if __name__ == "__main__":
    success = run_cross_validation()
    sys.exit(0 if success else 1)
