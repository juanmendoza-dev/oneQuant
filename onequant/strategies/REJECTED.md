# Rejected Strategies

## Breakout (Donchian Channel)

**File:** `strategies/breakout.py`
**Tested:** 2026-03-25
**Verdict:** REJECTED — fundamental flaw in signal quality

### Results ($250 capital, 10 years 15m data)

| Config | Trades | WR | PF | MaxDD | P&L |
|---|---|---|---|---|---|
| No regime filter | 3,155 | 45.3% | 0.705 | -88.9% | -$221.84 |
| RANGING only | 2,944 | 46.0% | 0.735 | -84.4% | -$210.33 |

### Why it fails

Win rate of ~45–46% combined with a 3% stop loss / 4% take profit
(TP:SL ratio of 1.33) is mathematically impossible to be profitable:

```
Expected value = (WR × TP) - ((1 - WR) × SL)
               = (0.46 × 4%) - (0.54 × 3%)
               = 1.84% - 1.62%
               = +0.22% gross

After fees (0.4% round-trip) + slippage (0.1%) + spread (0.05%):
Net EV = 0.22% - 0.55% = -0.33% per trade
```

At 5–6 trades/week this compounds into near-total capital destruction
(-84% to -89% over 10 years). Even with higher capital, the fee drag
cannot be overcome because the edge is negative before costs.

### Root cause

Donchian channel breakouts on 15m BTC-USD produce too many false
breakouts — price crosses the channel boundary but immediately reverses.
This is particularly bad in trending regimes (WR 33–36%) where price
trends away from the channel boundary and the breakout is a pullback
signal, not a continuation signal.

Regime filtering to RANGING-only does not rescue it (WR 46%, PF 0.74).

### What was tried

- No regime filter: PF 0.705
- RANGING regime only: PF 0.735
- BULL + BEAR trend regimes: WR drops to 33–36%, worse

### Not worth pursuing

This is not a fee problem or a parameter tuning problem. Win rate of
~45% is structural — the channel breakout signal itself has negative
predictive power on 15m candles. A higher TP:SL ratio (e.g. 2:1) would
improve EV math but would produce near-zero trade count at realistic
thresholds, eliminating statistical significance.

Strategy is kept in `breakout.py` for reference but should not be
included in production backtests or live trading.

---

## VWAP Momentum

**File:** `strategies/vwap_momentum.py`
**Tested:** 2026-03-25
**Verdict:** REJECTED — negative EV before costs

### Results ($250 capital, 10 years 15m data)

| Config | Trades | WR | PF | MaxDD | P&L |
|---|---|---|---|---|---|
| No regime filter | 1,303 | 38.1% | 0.48 | -70.1% | -$175.18 |

### Why it fails

Win rate of 38% with a 2% stop loss / 3% take profit is negative EV
before costs are even applied:

```
Expected value = (WR × TP) - ((1 - WR) × SL)
               = (0.38 × 3%) - (0.62 × 2%)
               = 1.14% - 1.24%
               = -0.10% per trade (gross)

After fees (0.4% round-trip) + slippage (0.1%) + spread (0.05%):
Net EV = -0.10% - 0.55% = -0.65% per trade
```

### Root cause

By the time all 4 conditions align simultaneously (VWAP deviation,
2x volume surge, 3 consecutive directional candles, RSI in range),
the move is already overextended. Volume surges on 15m BTC-USD candles
mark local extremes — capitulation lows and blow-off tops — not the
start of sustained moves. The signal fires into reversals.

### What was tried

- No regime filter: WR 38%, PF 0.48

Regime breakdown shows BEAR_TREND at 60% WR / PF 1.20, but only
15 trades over 10 years — insufficient sample size, likely noise.

### Not worth pursuing

No parameter variation can fix negative EV math. The 2x volume
requirement is the structural flaw — it selects for exhaustion candles
not momentum candles. Strategy is kept in `vwap_momentum.py` for
reference but should not be included in production backtests or live
trading.
