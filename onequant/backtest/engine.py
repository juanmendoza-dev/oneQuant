"""Core backtest engine — walks through candles and simulates trades."""

import sqlite3
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Optional

from config import config
from strategies.base import BaseStrategy, Signal

# ---------------------------------------------------------------------------
# Defaults
# ---------------------------------------------------------------------------

DEFAULT_CAPITAL: float = 1000.0
DEFAULT_POSITION_SIZE_PCT: float = 0.10
DEFAULT_STOP_LOSS_PCT: float = 0.02
DEFAULT_TAKE_PROFIT_PCT: float = 0.03
DEFAULT_MIN_CONFIDENCE: float = 0.65
DEFAULT_FEE_PCT: float = 0.006


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
    fee_pct: float = DEFAULT_FEE_PCT


@dataclass
class BacktestResult:
    """Output of a backtest run."""

    strategy_name: str
    timeframe: str
    start_date: datetime
    end_date: datetime
    initial_capital: float
    trades: list[TradeResult] = field(default_factory=list)


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
# Engine
# ---------------------------------------------------------------------------


def run_backtest(cfg: BacktestConfig) -> BacktestResult:
    """Execute a full backtest for the given configuration.

    Walks through candles one at a time (no lookahead), generates signals
    via the strategy, opens/closes positions with stop-loss and take-profit
    logic, and returns all trade results.
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

    strategy = cfg.strategy
    trades: list[TradeResult] = []
    position: Optional[_OpenPosition] = None
    capital = cfg.initial_capital

    # Determine how many candles the strategy needs
    # Walk from the earliest point where we have enough history
    min_window = max(getattr(strategy, "required_candles", 22), 3)

    for i in range(min_window, len(candles)):
        candle = candles[i]
        candle_time = datetime.fromtimestamp(candle["timestamp"], tz=timezone.utc)

        # ----- Check open position for stop-loss / take-profit -----
        if position is not None:
            hit_sl = False
            hit_tp = False

            if position.direction == "BUY":
                hit_sl = candle["low"] <= position.stop_loss
                hit_tp = candle["high"] >= position.take_profit
            else:  # SELL (short)
                hit_sl = candle["high"] >= position.stop_loss
                hit_tp = candle["low"] <= position.take_profit

            if hit_sl or hit_tp:
                # Conservative: if both hit in same candle, assume stop loss
                if hit_sl:
                    exit_price = position.stop_loss
                    outcome = "LOSS"
                else:
                    exit_price = position.take_profit
                    outcome = "WIN"

                exit_fee = position.size_usd * cfg.fee_pct
                total_fees = position.entry_fee + exit_fee

                if position.direction == "BUY":
                    pnl_raw = (exit_price - position.entry_price) / position.entry_price
                else:
                    pnl_raw = (position.entry_price - exit_price) / position.entry_price

                pnl_usd = position.size_usd * pnl_raw - total_fees
                pnl_pct = pnl_usd / position.size_usd

                capital += pnl_usd
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
                ))
                position = None

        # ----- Generate signal if no open position -----
        if position is None:
            window = candles[i - min_window : i + 1]
            signal: Signal = strategy.generate_signal(window)

            if (
                signal.direction in ("BUY", "SELL")
                and signal.confidence >= cfg.min_confidence
                and capital > 0
            ):
                entry_price = candle["close"]
                size_usd = capital * cfg.position_size_pct
                entry_fee = size_usd * cfg.fee_pct

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
                )

    # ----- Mark any remaining open position -----
    if position is not None:
        last_candle = candles[-1]
        last_time = datetime.fromtimestamp(last_candle["timestamp"], tz=timezone.utc)
        exit_price = last_candle["close"]
        exit_fee = position.size_usd * cfg.fee_pct
        total_fees = position.entry_fee + exit_fee

        if position.direction == "BUY":
            pnl_raw = (exit_price - position.entry_price) / position.entry_price
        else:
            pnl_raw = (position.entry_price - exit_price) / position.entry_price

        pnl_usd = position.size_usd * pnl_raw - total_fees
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
    )
