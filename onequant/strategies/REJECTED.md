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
