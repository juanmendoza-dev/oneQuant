"""Core backtest engine — walks through candles and simulates trades.

Audit-hardened engine with:
- No lookahead bias (signals use confirmed closed candles only)
- Realistic fills at next-candle open with slippage
- Gap-stop handling for stop losses
- Coinbase tiered fee model (rolling 30-day volume)
- Compounding position sizing with Coinbase $2 minimum
- Bid-ask spread modeling
- Market regime detection (200-EMA slope)
- Time-of-day and day-of-week tracking per trade
"""

import math
import sqlite3
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Optional

from config import config
from strategies.base import BaseStrategy, Signal

# ---------------------------------------------------------------------------
# Defaults
# ---------------------------------------------------------------------------

DEFAULT_CAPITAL: float = 25.0
DEFAULT_POSITION_SIZE_PCT: float = 0.10
DEFAULT_STOP_LOSS_PCT: float = 0.02
DEFAULT_TAKE_PROFIT_PCT: float = 0.03
DEFAULT_MIN_CONFIDENCE: float = 0.65
DEFAULT_SLIPPAGE_PCT: float = 0.001
DEFAULT_SPREAD_PCT: float = 0.0005
MIN_POSITION_SIZE: float = 2.0

THIRTY_DAYS_SECONDS: int = 30 * 24 * 3600
REGIME_EMA_PERIOD: int = 200
REGIME_SLOPE_WINDOW: int = 20
REGIME_BULL_THRESHOLD: float = 0.015
REGIME_BEAR_THRESHOLD: float = -0.015

# ---------------------------------------------------------------------------
# Coinbase Advanced Trade fee tiers (2026)
# ---------------------------------------------------------------------------

FEE_TIERS: list[tuple[float, float, float]] = [
    # (monthly_volume_cap, maker_fee, taker_fee)
    (10_000, 0.0040, 0.0060),
    (50_000, 0.0025, 0.0040),
    (float("inf"), 0.0010, 0.0020),
]


def _get_fee_pct(rolling_30d_volume: float, order_type: str) -> tuple[float, str]:
    """Return (fee_pct, tier_name) for the given monthly volume and order type."""
    for i, (cap, maker, taker) in enumerate(FEE_TIERS):
        if rolling_30d_volume < cap:
            pct = maker if order_type == "limit" else taker
            return pct, f"tier_{i + 1}"
    last = FEE_TIERS[-1]
    pct = last[1] if order_type == "limit" else last[2]
    return pct, f"tier_{len(FEE_TIERS)}"


# ---------------------------------------------------------------------------
# EMA and regime helpers
# ---------------------------------------------------------------------------


def _calculate_ema_series(values: list[float], period: int) -> list[float]:
    """Compute EMA at every index — result[i] uses only values[0..i]."""
    n = len(values)
    if n == 0:
        return []
    result = [0.0] * n
    if n < period:
        running = 0.0
        for i in range(n):
            running += values[i]
            result[i] = running / (i + 1)
        return result

    # Progressive SMA for indices before `period`
    running = 0.0
    for i in range(period - 1):
        running += values[i]
        result[i] = running / (i + 1)

    sma = (running + values[period - 1]) / period
    result[period - 1] = sma

    multiplier = 2.0 / (period + 1)
    ema = sma
    for i in range(period, n):
        ema = (values[i] - ema) * multiplier + ema
        result[i] = ema
    return result


def _detect_regime(ema_series: list[float], index: int) -> str:
    """Detect market regime from pre-computed EMA series at *index*."""
    min_idx = REGIME_EMA_PERIOD + REGIME_SLOPE_WINDOW - 1
    if index < min_idx:
        return "UNKNOWN"
    ema_now = ema_series[index]
    ema_prev = ema_series[index - REGIME_SLOPE_WINDOW]
    if ema_prev == 0:
        return "UNKNOWN"
    slope = (ema_now - ema_prev) / ema_prev
    if slope > REGIME_BULL_THRESHOLD:
        return "BULL_TREND"
    if slope < REGIME_BEAR_THRESHOLD:
        return "BEAR_TREND"
    return "RANGING"


# ---------------------------------------------------------------------------
# Data classes
# ---------------------------------------------------------------------------


@dataclass
class TradeResult:
    """Record of a single completed (or still-open) trade."""

    entry_time: datetime
    exit_time: datetime
    direction: str
    entry_price: float
    exit_price: float
    stop_loss: float
    take_profit: float
    outcome: str  # 'WIN', 'LOSS', 'OPEN'
    pnl: float
    pnl_pct: float
    confidence: float
    reason: str
    fees_paid: float
    slippage_paid: float
    spread_cost: float
    gap_stop: bool
    fill_type: str  # 'limit', 'market', 'gap'
    position_size_usd: float
    equity_before: float
    equity_after: float
    entry_hour: int
    entry_day: str
    regime: str
    fee_tier: str


@dataclass
class _OpenPosition:
    """Internal tracker for a position that hasn't closed yet."""

    entry_time: datetime
    direction: str
    entry_price: float
    stop_loss: float
    take_profit: float
    size_usd: float
    confidence: float
    reason: str
    entry_fee: float
    slippage_paid: float
    spread_entry_cost: float
    equity_before: float
    entry_hour: int
    entry_day: str
    regime: str
    fee_tier: str


@dataclass
class BacktestConfig:
    """All tuneable parameters for a single backtest run."""

    strategy: BaseStrategy
    timeframe: str = "15m"
    start_ts: Optional[int] = None
    end_ts: Optional[int] = None
    initial_capital: float = DEFAULT_CAPITAL
    position_size_pct: float = DEFAULT_POSITION_SIZE_PCT
    stop_loss_pct: float = DEFAULT_STOP_LOSS_PCT
    take_profit_pct: float = DEFAULT_TAKE_PROFIT_PCT
    min_confidence: float = DEFAULT_MIN_CONFIDENCE
    fee_pct: Optional[float] = None  # None → use tiered model
    slippage_pct: float = DEFAULT_SLIPPAGE_PCT
    spread_pct: float = DEFAULT_SPREAD_PCT
    order_type: str = "limit"  # 'limit' or 'market'
    allowed_regimes: Optional[list[str]] = None  # None → trade all regimes


@dataclass
class BuyAndHoldBenchmark:
    """Buy-and-hold benchmark computed over the same period as the backtest."""

    return_pct: float
    max_drawdown: float
    sharpe: float


@dataclass
class BacktestResult:
    """Output of a backtest run."""

    strategy_name: str
    timeframe: str
    start_date: datetime
    end_date: datetime
    initial_capital: float
    trades: list[TradeResult] = field(default_factory=list)
    buy_and_hold: Optional[BuyAndHoldBenchmark] = None


# ---------------------------------------------------------------------------
# Candle loading
# ---------------------------------------------------------------------------


def _load_candles(
    timeframe: str,
    start_ts: Optional[int],
    end_ts: Optional[int],
) -> list[dict]:
    """Load candles from the database, sorted by timestamp ascending."""
    conn = sqlite3.connect(config.DATABASE_PATH)
    conn.row_factory = sqlite3.Row
    try:
        query = (
            "SELECT timestamp, open, high, low, close, volume "
            "FROM btc_candles WHERE timeframe = ?"
        )
        params: list = [timeframe]

        if start_ts is not None:
            query += " AND timestamp >= ?"
            params.append(start_ts)
        if end_ts is not None:
            query += " AND timestamp <= ?"
            params.append(end_ts)

        query += " ORDER BY timestamp ASC"
        rows = conn.execute(query, params).fetchall()
        return [dict(r) for r in rows]
    finally:
        conn.close()


def _ts_range(timeframe: str) -> tuple[int, int]:
    """Return (min_ts, max_ts) for the given timeframe in the database."""
    conn = sqlite3.connect(config.DATABASE_PATH)
    try:
        row = conn.execute(
            "SELECT MIN(timestamp), MAX(timestamp) FROM btc_candles WHERE timeframe = ?",
            (timeframe,),
        ).fetchone()
        if row and row[0] is not None:
            return (row[0], row[1])
        raise RuntimeError(f"No candles found for timeframe {timeframe}")
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------


def _rolling_volume(trade_log: list[tuple[int, float]], current_ts: int) -> float:
    """Sum completed trade volumes from the last 30 days."""
    cutoff = current_ts - THIRTY_DAYS_SECONDS
    return sum(vol for ts, vol in trade_log if ts >= cutoff)


def _resolve_fee(cfg: BacktestConfig, rolling_vol: float) -> tuple[float, str]:
    """Return (fee_pct, tier_name) for this trade."""
    if cfg.fee_pct is not None:
        return cfg.fee_pct, "override"
    return _get_fee_pct(rolling_vol, cfg.order_type)


def _compute_buy_and_hold(candles: list[dict]) -> BuyAndHoldBenchmark:
    """Compute buy-and-hold return, max drawdown, and Sharpe over the candle set."""
    closes = [c["close"] for c in candles]
    if len(closes) < 2 or closes[0] == 0:
        return BuyAndHoldBenchmark(return_pct=0.0, max_drawdown=0.0, sharpe=0.0)

    return_pct = ((closes[-1] - closes[0]) / closes[0]) * 100.0

    # Max drawdown
    peak = closes[0]
    max_dd = 0.0
    for c in closes:
        if c > peak:
            peak = c
        dd = (peak - c) / peak if peak > 0 else 0.0
        if dd > max_dd:
            max_dd = dd

    # Sharpe from candle-to-candle returns
    returns = [(closes[i] - closes[i - 1]) / closes[i - 1]
               for i in range(1, len(closes)) if closes[i - 1] != 0]
    if len(returns) < 2:
        return BuyAndHoldBenchmark(return_pct=return_pct, max_drawdown=max_dd * 100.0, sharpe=0.0)

    mean_r = sum(returns) / len(returns)
    var_r = sum((r - mean_r) ** 2 for r in returns) / (len(returns) - 1)
    std_r = math.sqrt(var_r) if var_r > 0 else 0.0
    sharpe = (mean_r / std_r) * math.sqrt(len(returns)) if std_r > 0 else 0.0

    return BuyAndHoldBenchmark(
        return_pct=return_pct,
        max_drawdown=max_dd * 100.0,
        sharpe=sharpe,
    )


# ---------------------------------------------------------------------------
# Engine
# ---------------------------------------------------------------------------


def run_backtest(cfg: BacktestConfig) -> BacktestResult:
    """Execute a full backtest for the given configuration.

    Walks through candles one at a time with no lookahead bias:
    - Signals generated from confirmed closed candles only (up to i-1)
    - Entries filled at candle[i].open with slippage and spread
    - Stop losses respect gap-through pricing
    - Fees follow Coinbase's tiered schedule (or flat override)
    - Position size compounds with current equity
    """
    if cfg.start_ts is None or cfg.end_ts is None:
        db_min, db_max = _ts_range(cfg.timeframe)
        if cfg.start_ts is None:
            cfg.start_ts = db_min
        if cfg.end_ts is None:
            cfg.end_ts = db_max

    candles = _load_candles(cfg.timeframe, cfg.start_ts, cfg.end_ts)
    if not candles:
        raise RuntimeError("No candles loaded — check timeframe and date range")

    # Pre-compute EMA200 for regime detection (no lookahead — each index
    # only depends on closes at or before that index)
    all_closes = [c["close"] for c in candles]
    ema200 = _calculate_ema_series(all_closes, REGIME_EMA_PERIOD)

    strategy = cfg.strategy
    trades: list[TradeResult] = []
    position: Optional[_OpenPosition] = None
    capital = cfg.initial_capital

    # Rolling 30-day volume for fee tier calculation (round-trip notional)
    trade_log: list[tuple[int, float]] = []

    min_window = max(getattr(strategy, "required_candles", 22), 3)

    for i in range(min_window, len(candles)):
        candle = candles[i]
        candle_time = datetime.fromtimestamp(candle["timestamp"], tz=timezone.utc)
        ts = candle["timestamp"]

        # Regime uses EMA up to the PREVIOUS closed candle (i-1) to avoid
        # using any information from the current candle
        regime = _detect_regime(ema200, i - 1) if i > 0 else "UNKNOWN"

        # ----- Check open position for stop-loss / take-profit -----
        if position is not None:
            hit_sl = False
            hit_tp = False
            gap_stop = False

            if position.direction == "BUY":
                hit_sl = candle["low"] <= position.stop_loss
                hit_tp = candle["high"] >= position.take_profit
            else:  # SELL (short)
                hit_sl = candle["high"] >= position.stop_loss
                hit_tp = candle["low"] <= position.take_profit

            if hit_sl or hit_tp:
                # Conservative: if both hit in same candle, assume stop loss
                if hit_sl:
                    if position.direction == "BUY":
                        if candle["open"] <= position.stop_loss:
                            exit_price = candle["open"]  # gap through
                            gap_stop = True
                        else:
                            exit_price = position.stop_loss
                    else:  # SELL short
                        if candle["open"] >= position.stop_loss:
                            exit_price = candle["open"]  # gap through
                            gap_stop = True
                        else:
                            exit_price = position.stop_loss
                    outcome = "LOSS"
                    fill_type = "gap" if gap_stop else "market"
                else:
                    exit_price = position.take_profit
                    outcome = "WIN"
                    fill_type = "limit"

                # Half spread on exit (price adjustment)
                price_half_spread_exit = exit_price * (cfg.spread_pct / 2)
                if position.direction == "BUY":
                    exit_price -= price_half_spread_exit  # selling: receive bid
                else:
                    exit_price += price_half_spread_exit  # buying to cover: pay ask
                # Dollar cost of exit half spread
                dollar_spread_exit = position.size_usd * (cfg.spread_pct / 2)

                # Exit fee (tiered)
                rolling_vol = _rolling_volume(trade_log, ts)
                exit_fee_pct, _ = _resolve_fee(cfg, rolling_vol)
                exit_fee = position.size_usd * exit_fee_pct
                total_fees = position.entry_fee + exit_fee

                if position.direction == "BUY":
                    pnl_raw = (exit_price - position.entry_price) / position.entry_price
                else:
                    pnl_raw = (position.entry_price - exit_price) / position.entry_price

                pnl_usd = position.size_usd * pnl_raw - total_fees
                pnl_pct = pnl_usd / position.size_usd if position.size_usd else 0.0
                spread_cost = position.spread_entry_cost + dollar_spread_exit

                capital += pnl_usd

                # Record round-trip volume for fee tier tracking
                trade_log.append((ts, position.size_usd * 2))

                trades.append(TradeResult(
                    entry_time=position.entry_time,
                    exit_time=candle_time,
                    direction=position.direction,
                    entry_price=position.entry_price,
                    exit_price=exit_price,
                    stop_loss=position.stop_loss,
                    take_profit=position.take_profit,
                    outcome=outcome,
                    pnl=pnl_usd,
                    pnl_pct=pnl_pct,
                    confidence=position.confidence,
                    reason=position.reason,
                    fees_paid=total_fees,
                    slippage_paid=position.slippage_paid,
                    spread_cost=spread_cost,
                    gap_stop=gap_stop,
                    fill_type=fill_type,
                    position_size_usd=position.size_usd,
                    equity_before=position.equity_before,
                    equity_after=capital,
                    entry_hour=position.entry_hour,
                    entry_day=position.entry_day,
                    regime=position.regime,
                    fee_tier=position.fee_tier,
                ))
                position = None

        # ----- Generate signal if no open position -----
        if position is None:
            # Uses confirmed closed candles only — no lookahead
            # Window ends at candle[i-1]; candle[i] is the execution candle
            window = candles[max(0, i - min_window) : i]
            signal: Signal = strategy.generate_signal(window)

            if (
                signal.direction in ("BUY", "SELL")
                and signal.confidence >= cfg.min_confidence
                and capital > 0
            ):
                # Regime filter
                if cfg.allowed_regimes is not None and regime not in cfg.allowed_regimes:
                    continue

                # Compounding position size (percentage of CURRENT equity)
                size_usd = capital * cfg.position_size_pct
                if size_usd < MIN_POSITION_SIZE:
                    continue  # Coinbase minimum order not met

                # Entry at this candle's OPEN (signal was based on prior candles)
                raw_open = candle["open"]

                # Apply slippage to price
                price_slippage = raw_open * cfg.slippage_pct
                if signal.direction == "BUY":
                    entry_price = raw_open + price_slippage
                else:
                    entry_price = raw_open - price_slippage
                # Dollar cost of slippage for this position
                dollar_slippage = size_usd * cfg.slippage_pct

                # Apply half spread on entry
                price_half_spread = entry_price * (cfg.spread_pct / 2)
                if signal.direction == "BUY":
                    entry_price += price_half_spread  # buying: pay ask
                else:
                    entry_price -= price_half_spread  # selling: receive bid
                # Dollar cost of half spread for this position
                dollar_spread_entry = size_usd * (cfg.spread_pct / 2)

                # Entry fee (tiered)
                rolling_vol = _rolling_volume(trade_log, ts)
                entry_fee_pct, fee_tier = _resolve_fee(cfg, rolling_vol)
                entry_fee = size_usd * entry_fee_pct

                # SL/TP levels from adjusted entry price
                if signal.direction == "BUY":
                    sl = entry_price * (1.0 - cfg.stop_loss_pct)
                    tp = entry_price * (1.0 + cfg.take_profit_pct)
                else:
                    sl = entry_price * (1.0 + cfg.stop_loss_pct)
                    tp = entry_price * (1.0 - cfg.take_profit_pct)

                position = _OpenPosition(
                    entry_time=candle_time,
                    direction=signal.direction,
                    entry_price=entry_price,
                    stop_loss=sl,
                    take_profit=tp,
                    size_usd=size_usd,
                    confidence=signal.confidence,
                    reason=signal.reason,
                    entry_fee=entry_fee,
                    slippage_paid=dollar_slippage,
                    spread_entry_cost=dollar_spread_entry,
                    equity_before=capital,
                    entry_hour=candle_time.hour,
                    entry_day=candle_time.strftime("%A"),
                    regime=regime,
                    fee_tier=fee_tier,
                )

    # ----- Mark any remaining open position at last candle close -----
    if position is not None:
        last_candle = candles[-1]
        last_time = datetime.fromtimestamp(last_candle["timestamp"], tz=timezone.utc)
        exit_price = last_candle["close"]

        price_half_spread_exit = exit_price * (cfg.spread_pct / 2)
        if position.direction == "BUY":
            exit_price -= price_half_spread_exit
        else:
            exit_price += price_half_spread_exit
        dollar_spread_exit = position.size_usd * (cfg.spread_pct / 2)

        rolling_vol = _rolling_volume(trade_log, last_candle["timestamp"])
        exit_fee_pct, _ = _resolve_fee(cfg, rolling_vol)
        exit_fee = position.size_usd * exit_fee_pct
        total_fees = position.entry_fee + exit_fee

        if position.direction == "BUY":
            pnl_raw = (exit_price - position.entry_price) / position.entry_price
        else:
            pnl_raw = (position.entry_price - exit_price) / position.entry_price

        pnl_usd = position.size_usd * pnl_raw - total_fees
        spread_cost = position.spread_entry_cost + dollar_spread_exit
        capital += pnl_usd

        trades.append(TradeResult(
            entry_time=position.entry_time,
            exit_time=last_time,
            direction=position.direction,
            entry_price=position.entry_price,
            exit_price=exit_price,
            stop_loss=position.stop_loss,
            take_profit=position.take_profit,
            outcome="OPEN",
            pnl=pnl_usd,
            pnl_pct=pnl_usd / position.size_usd if position.size_usd else 0.0,
            confidence=position.confidence,
            reason=position.reason,
            fees_paid=total_fees,
            slippage_paid=position.slippage_paid,
            spread_cost=spread_cost,
            gap_stop=False,
            fill_type="market",
            position_size_usd=position.size_usd,
            equity_before=position.equity_before,
            equity_after=capital,
            entry_hour=position.entry_hour,
            entry_day=position.entry_day,
            regime=position.regime,
            fee_tier=position.fee_tier,
        ))

    start_date = datetime.fromtimestamp(candles[0]["timestamp"], tz=timezone.utc)
    end_date = datetime.fromtimestamp(candles[-1]["timestamp"], tz=timezone.utc)

    return BacktestResult(
        strategy_name=strategy.name,
        timeframe=cfg.timeframe,
        start_date=start_date,
        end_date=end_date,
        initial_capital=cfg.initial_capital,
        trades=trades,
        buy_and_hold=_compute_buy_and_hold(candles),
    )
