"""Paper trading engine — Strategy 1 (Mean Reversion, Config A).

Watches btc_candles for new 15m BTC-USD candles, runs the validated mean
reversion signal check on each close, and manages simulated positions in the
paper_trades table.

Strategy parameters (locked Config A):
  SELL only · BULL_TREND regime · RSI > 75 · EMA dev > 1.5%
  SL: 6% · TP: 4% · position_size_usd: $250 · min_confidence: 0.55
"""

import asyncio
import logging
import time
from pathlib import Path

from database.db import (
    get_last_15m_candles,
    get_open_paper_trade,
    insert_paper_trade,
    update_paper_trade_closed,
    insert_skipped_paper_trade,
)
from strategies.mean_reversion import MeanReversionStrategy

# ---------------------------------------------------------------------------
# Config A — locked parameters
# ---------------------------------------------------------------------------

POSITION_SIZE_USD: float = 250.0
STOP_LOSS_PCT: float = 0.06
TAKE_PROFIT_PCT: float = 0.04
MIN_CONFIDENCE: float = 0.55
ALLOWED_REGIME: str = "BULL_TREND"
BACKTEST_PREDICTED_WR: float = 76.8
FEE_PCT: float = 0.006          # Kraken tier-1 taker
CANDLES_FOR_SIGNAL: int = 30    # feeds into strategy (requires 21)
CANDLES_FOR_REGIME: int = 230   # 200-EMA + 20-slope window + buffer
POLL_INTERVAL: float = 15.0     # seconds between DB checks

# Regime detection mirrors backtest/engine.py constants
REGIME_EMA_PERIOD: int = 200
REGIME_SLOPE_WINDOW: int = 20
REGIME_BULL_THRESHOLD: float = 0.015
REGIME_BEAR_THRESHOLD: float = -0.015

MODULE_NAME: str = "paper_engine"

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------

logger: logging.Logger = logging.getLogger(MODULE_NAME)


def _setup_logging() -> None:
    if logger.handlers:
        return
    logger.setLevel(logging.DEBUG)
    Path("logs").mkdir(exist_ok=True)

    fh = logging.FileHandler("logs/paper_engine.log")
    fh.setLevel(logging.DEBUG)

    ch = logging.StreamHandler()
    ch.setLevel(logging.INFO)

    fmt = logging.Formatter("%(asctime)s [%(levelname)s] %(name)s — %(message)s")
    fh.setFormatter(fmt)
    ch.setFormatter(fmt)
    logger.addHandler(fh)
    logger.addHandler(ch)


# ---------------------------------------------------------------------------
# Regime detection (identical to backtest engine, operating on raw candle list)
# ---------------------------------------------------------------------------


def _calculate_ema_series(values: list[float], period: int) -> list[float]:
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


def _detect_regime(candles: list[dict]) -> str:
    """Detect BULL_TREND / BEAR_TREND / RANGING / UNKNOWN from candle list."""
    if len(candles) < REGIME_EMA_PERIOD + REGIME_SLOPE_WINDOW:
        return "UNKNOWN"
    closes = [c["close"] for c in candles]
    ema200 = _calculate_ema_series(closes, REGIME_EMA_PERIOD)
    idx = len(ema200) - 1
    ema_now = ema200[idx]
    ema_prev = ema200[idx - REGIME_SLOPE_WINDOW]
    if ema_prev == 0:
        return "UNKNOWN"
    slope = (ema_now - ema_prev) / ema_prev
    if slope > REGIME_BULL_THRESHOLD:
        return "BULL_TREND"
    if slope < REGIME_BEAR_THRESHOLD:
        return "BEAR_TREND"
    return "RANGING"


# ---------------------------------------------------------------------------
# Gap-stop exit logic (mirrors backtest engine)
# ---------------------------------------------------------------------------


def _resolve_exit(position: dict, candle: dict) -> tuple[bool, bool, bool]:
    """Return (hit_sl, hit_tp, gap_stop) for a SELL short position on this candle."""
    hit_sl = candle["high"] >= position["stop_loss"]
    hit_tp = candle["low"] <= position["take_profit"]
    gap_stop = False
    if hit_sl and candle["open"] >= position["stop_loss"]:
        gap_stop = True
    return hit_sl, hit_tp, gap_stop


def _exit_price_for(position: dict, candle: dict, hit_sl: bool, gap_stop: bool) -> float:
    if hit_sl:
        return candle["open"] if gap_stop else position["stop_loss"]
    return position["take_profit"]


# ---------------------------------------------------------------------------
# Main engine loop
# ---------------------------------------------------------------------------

_strategy = MeanReversionStrategy()
_last_processed_ts: int = 0


async def _check_open_position(candle: dict) -> None:
    """Check if the open position's SL/TP was hit on this candle."""
    position = await get_open_paper_trade()
    if position is None:
        return

    hit_sl, hit_tp, gap_stop = _resolve_exit(position, candle)
    if not hit_sl and not hit_tp:
        return

    # Both hit same candle → conservative: assume stop loss
    if hit_sl:
        exit_price = _exit_price_for(position, candle, hit_sl=True, gap_stop=gap_stop)
        outcome = "LOSS"
    else:
        exit_price = position["take_profit"]
        outcome = "WIN"

    entry_price = position["entry_price"]
    size_usd = position["position_size_usd"]

    # SELL short: profit when price falls
    pnl_raw = (entry_price - exit_price) / entry_price
    fees = size_usd * FEE_PCT
    pnl_usd = size_usd * pnl_raw - fees
    pnl_pct = pnl_raw - FEE_PCT

    await update_paper_trade_closed(
        trade_id=position["id"],
        status=outcome,
        exit_price=exit_price,
        exit_time=candle["timestamp"],
        pnl=pnl_usd,
        pnl_pct=pnl_pct,
        fees_paid=fees,
    )

    sign = "+" if pnl_usd >= 0 else ""
    logger.info(
        "PAPER TRADE CLOSED: %s %s$%.2f (%s%.2f%%)",
        outcome,
        sign,
        pnl_usd,
        sign,
        pnl_pct * 100,
    )
    print(
        f"PAPER TRADE CLOSED: {outcome} {sign}${pnl_usd:.2f} ({sign}{pnl_pct * 100:.2f}%)"
    )


async def _check_signal(candle: dict, regime_candles: list[dict]) -> None:
    """Run signal check. Opens a new paper trade if conditions are met."""
    position = await get_open_paper_trade()
    if position is not None:
        logger.info("SIGNAL SKIPPED: position already open")
        print("SIGNAL SKIPPED: position already open")
        return

    regime = _detect_regime(regime_candles)

    # Use last CANDLES_FOR_SIGNAL candles for strategy signal
    signal_candles = regime_candles[-CANDLES_FOR_SIGNAL:]
    signal = _strategy.generate_signal(signal_candles)

    print(f"SIGNAL CHECK: score {signal.confidence:.2f} — {signal.reason}")
    logger.debug("Signal: %s conf=%.2f reason=%s", signal.direction, signal.confidence, signal.reason)

    if signal.direction != "SELL":
        return
    if signal.confidence < MIN_CONFIDENCE:
        return
    if regime != ALLOWED_REGIME:
        logger.info("Signal filtered: regime=%s (require %s)", regime, ALLOWED_REGIME)
        return

    entry_price = candle["close"]  # enter at close of signal candle
    sl = entry_price * (1.0 + STOP_LOSS_PCT)
    tp = entry_price * (1.0 - TAKE_PROFIT_PCT)
    fees = POSITION_SIZE_USD * FEE_PCT

    trade_id = await insert_paper_trade(
        strategy="Mean Reversion Config A",
        signal_time=candle["timestamp"],
        direction="SELL",
        entry_price=entry_price,
        stop_loss=sl,
        take_profit=tp,
        position_size_usd=POSITION_SIZE_USD,
        regime=regime,
        signal_reason=signal.reason,
        fees_paid=fees,
    )

    logger.info(
        "PAPER TRADE OPEN: SELL BTC-USD @ $%s (id=%d)",
        f"{entry_price:,.0f}",
        trade_id,
    )
    print(f"PAPER TRADE OPEN: SELL BTC-USD @ ${entry_price:,.0f}")


async def _process_new_candle(candle: dict, regime_candles: list[dict]) -> None:
    """Handle a newly closed 15m candle: check SL/TP then check signal."""
    await _check_open_position(candle)
    await _check_signal(candle, regime_candles)


async def run_paper_engine() -> None:
    """Asyncio task: poll btc_candles for new 15m BTC-USD candles and run checks."""
    global _last_processed_ts
    _setup_logging()
    logger.info("Paper engine started — watching for 15m BTC-USD candles")

    while True:
        try:
            candles = await get_last_15m_candles(limit=CANDLES_FOR_REGIME)

            if not candles:
                await asyncio.sleep(POLL_INTERVAL)
                continue

            latest = candles[-1]
            latest_ts = latest["timestamp"]

            if latest_ts > _last_processed_ts:
                _last_processed_ts = latest_ts
                regime_candles = candles  # full window for regime detection
                await _process_new_candle(latest, regime_candles)

        except asyncio.CancelledError:
            logger.info("Paper engine task cancelled — shutting down")
            return
        except Exception as exc:
            logger.error("Paper engine error: %s", exc)

        await asyncio.sleep(POLL_INTERVAL)
