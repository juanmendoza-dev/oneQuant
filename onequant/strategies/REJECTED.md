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

---

## Bollinger Band Reversion

**File:** `strategies/bb_reversion.py`
**Tested:** 2026-03-25
**Verdict:** REJECTED — insufficient win rate

### Results ($250 capital, 10 years 15m data, RANGING regime only)

| Config | Trades | WR | PF | MaxDD | P&L |
|---|---|---|---|---|---|
| RANGING, BW < 0.04 | 2,435 | 50.8% | 0.43 | -92.0% | -$230.06 |

### Why it fails

Win rate of 50.8% falls short of the 54.5% minimum required for
positive gross EV with a 3% SL / 2.5% TP setup:

```
EV = (0.508 × 2.5%) - (0.492 × 3%)
   = 1.27% - 1.48% = -0.21% per trade (gross)

After fees (0.4% round-trip) + slippage (0.1%) + spread (0.05%):
Net EV = -0.21% - 0.55% = -0.76% per trade
```

### Root cause

2,435 trades in RANGING-only proves the bandwidth < 0.04 filter
fires too loosely — it is not selecting high-probability setups.
Tightening bandwidth reduces trade quantity but not signal quality;
the underlying WR stays below the breakeven threshold regardless.
Same structural flaw as Breakout and VWAP Momentum: the filter
conditions do not isolate genuinely predictive moments.

### Not worth pursuing

No parameter variation can fix insufficient win rate. Strategy is
kept in `bb_reversion.py` for reference but should not be included
in production backtests or live trading.

---

## Capitulation

**File:** `strategies/capitulation.py`
**Tested:** 2026-03-28
**Verdict:** REJECTED — PF 0.559 < 1.0; MaxDD 37.1% > 20%

| Trades | WR | PF | MaxDD | P&L |
|---|---|---|---|---|
| 336 | 57.7% | 0.559 | -37.1% | -$92.47 |

Despite adequate win rate (57.7%), profit factor is 0.559 — losses
are nearly twice as large as wins. MaxDD of 37.1% exceeds 20% limit.

---

## EMA Pullback

**File:** `strategies/ema_pullback.py`
**Tested:** 2026-03-28
**Verdict:** REJECTED — PF 0.692 < 1.0; MaxDD 72.6% > 20%

| Trades | WR | PF | MaxDD | P&L |
|---|---|---|---|---|
| 1,516 | 60.6% | 0.692 | -72.6% | -$180.71 |

High trade count with 60.6% WR but PF 0.692 and catastrophic 72.6%
drawdown. Losing trades are too large relative to winners.

---

## Momentum

**File:** `strategies/momentum.py`
**Tested:** 2026-03-28
**Verdict:** REJECTED — PF 0.771 < 1.0; MaxDD 67.8% > 20%

| Trades | WR | PF | MaxDD | P&L |
|---|---|---|---|---|
| 1,778 | 62.1% | 0.771 | -67.8% | -$168.79 |

Highest WR of the batch (62.1%) but still PF < 1.0 with 67.8%
drawdown. The TP:SL ratio is unfavorable — wins are too small.

---

## MTF Mean Reversion

**File:** `strategies/mtf_mean_reversion.py`
**Tested:** 2026-03-28
**Verdict:** REJECTED — PF 0.796 < 1.0

| Trades | WR | PF | MaxDD | P&L |
|---|---|---|---|---|
| 74 | 63.5% | 0.796 | -6.0% | -$9.32 |

Low drawdown (6.0%) and decent WR (63.5%) but only 74 trades over
10 years and PF 0.796. Multi-timeframe alignment filters too
aggressively, leaving insufficient sample size.

---

## News Driven

**File:** `strategies/news_driven.py`
**Tested:** 2026-03-28
**Verdict:** REJECTED — 0 trades; depends on news_feed table data

| Trades | WR | PF | MaxDD | P&L |
|---|---|---|---|---|
| 0 | 0.0% | 0.000 | 0.0% | $0.00 |

Strategy requires news_feed table data which was empty during
backtest. Even with data, the strategy violates the OHLCV-only
constraint for backtest reproducibility.

---

## RSI Divergence

**File:** `strategies/rsi_divergence.py`
**Tested:** 2026-03-28
**Verdict:** REJECTED — WR 54.4% < 55%; PF 0.519 < 1.0; MaxDD 86.3% > 20%

| Trades | WR | PF | MaxDD | P&L |
|---|---|---|---|---|
| 1,404 | 54.4% | 0.519 | -86.3% | -$214.78 |

Fails all three thresholds. 86.3% drawdown is near-total capital
destruction. Divergence signals fire too frequently with no edge.

---

## Trend Exhaustion

**File:** `strategies/trend_exhaustion.py`
**Tested:** 2026-03-28
**Verdict:** REJECTED — 2 trades only; WR 0%, PF 0.000

| Trades | WR | PF | MaxDD | P&L |
|---|---|---|---|---|
| 2 | 0.0% | 0.000 | -1.4% | -$3.58 |

Only 2 trades over 10 years — signal conditions are too restrictive
to produce a meaningful sample. Both trades were losses.


---

## VWAP Bounce

**File:** `strategies/vwap_bounce.py` (deleted)
**Tested:** 2026-03-28
**Verdict:** REJECTED — WR 48.9% < 55%; PF 0.40 < 1.0; MaxDD 92.0% > 20%

| Trades | WR | PF | MaxDD | P&L |
|---|---|---|---|---|
| 1,677 | 48.9% | 0.40 | -92.0% | -$230.04 |

Fading price deviations from daily VWAP with RSI confirmation.
Total fees ($116.75) consumed nearly half the capital. Regime
breakdown shows BEAR_TREND WR 63% / PF 0.88 and BULL_TREND WR 67% /
PF 0.75 — better in trending regimes but RANGING (1,483 trades,
WR 47%, PF 0.36) destroys overall performance. VWAP mean reversion
on 15m candles generates too many signals in ranging markets where
price oscillates around VWAP without meaningful deviation.

---

## BB Squeeze

**File:** `strategies/bb_squeeze.py` (deleted)
**Tested:** 2026-03-28
**Verdict:** REJECTED — WR 42.4% < 55%; PF 0.61 < 1.0; MaxDD 51.6% > 20%

| Trades | WR | PF | MaxDD | P&L |
|---|---|---|---|---|
| 824 | 42.4% | 0.61 | -51.6% | -$128.91 |

Bollinger Band squeeze-then-expansion breakout with volume
confirmation. 813 of 824 trades occurred in RANGING regime (WR 43%,
PF 0.62). Only 5 trades in BULL_TREND (WR 60%, PF 1.42) — too few
to be meaningful. The squeeze detection fires too loosely in ranging
conditions where bandwidth oscillates naturally. False breakouts
after squeezes are the dominant outcome on 15m timeframe.

---

## RSI Both Directions

**File:** `strategies/rsi_both_directions.py` (deleted)
**Tested:** 2026-03-28
**Verdict:** REJECTED — PF 0.72 < 1.0

| Trades | WR | PF | MaxDD | P&L |
|---|---|---|---|---|
| 168 | 60.7% | 0.72 | -12.1% | -$30.18 |

RSI < 28 long / RSI > 72 short with 200 EMA regime filter. Best
drawdown of the batch (-12.1%) and decent WR (60.7%), but PF 0.72
means losses are significantly larger than wins. With TP 4% / SL 6%,
the math requires WR > 71% to be profitable after fees. The 200 EMA
filter helped reduce trade count (168 vs thousands) but could not
push WR high enough. BULL_TREND showed WR 75% / PF 1.38 but only
4 trades — noise, not signal.

---

## EMA Ribbon

**File:** `strategies/ema_ribbon.py` (deleted)
**Tested:** 2026-03-28
**Verdict:** REJECTED — WR 43.5% < 55%; PF 0.66 < 1.0; MaxDD 86.5% > 20%

| Trades | WR | PF | MaxDD | P&L |
|---|---|---|---|---|
| 2,517 | 43.5% | 0.66 | -86.5% | -$215.75 |

5-EMA ribbon alignment with pullback to fastest EMA. Generated 2,517
trades (4.7/week) with $235.82 in fees — fees alone nearly equal the
total loss. The "pullback to 8 EMA" condition fires too frequently
because price constantly touches short-period EMAs. EMA alignment
does not reliably predict continuation on 15m candles — 2,346 of
2,517 trades were RANGING with WR 43%.

---

## Opening Range

**File:** `strategies/opening_range.py` (deleted)
**Tested:** 2026-03-28
**Verdict:** REJECTED — WR 46.6% < 55%; PF 0.72 < 1.0; MaxDD 87.1% > 20%

| Trades | WR | PF | MaxDD | P&L |
|---|---|---|---|---|
| 3,245 | 46.6% | 0.72 | -87.1% | -$217.33 |

First-hour range breakout. Highest trade count (3,245, 6.1/week) and
highest total fees ($302.42). Ranging regime dominates (3,025 trades,
WR 48%, PF 0.77). Trend regimes performed even worse (BULL_TREND
WR 27%, BEAR_TREND WR 39%) — breakouts during trends tend to be in
the wrong direction because the opening range captures a pullback,
not the trend. The concept assumes meaningful range-setting behavior
at UTC midnight, but crypto trades 24/7 with no true "opening."

---

## Candle Momentum

**File:** `strategies/candle_momentum.py` (deleted)
**Tested:** 2026-03-28
**Verdict:** REJECTED — WR 41.8% < 55%; PF 0.57 < 1.0; MaxDD 66.8% > 20%

| Trades | WR | PF | MaxDD | P&L |
|---|---|---|---|---|
| 1,090 | 41.8% | 0.57 | -66.8% | -$165.74 |

3 consecutive same-direction candles with increasing volume. The
pattern fires after the move has already happened — by the time
3 candles confirm direction with rising volume, the short-term
momentum is exhausted. WR of 41.8% means continuation is the
minority outcome. BULL_TREND WR 31% is particularly bad — in bull
trends, 3 bearish candles are counter-trend noise that reverses.

---

## Session Overlap

**File:** `strategies/session_overlap.py` (deleted)
**Tested:** 2026-03-28
**Verdict:** REJECTED — WR 44.1% < 55%; PF 0.61 < 1.0; MaxDD 52.7% > 20%

| Trades | WR | PF | MaxDD | P&L |
|---|---|---|---|---|
| 854 | 44.1% | 0.61 | -52.7% | -$130.17 |

EMA 9/21 crossover restricted to London/NY overlap (13:00-17:00 UTC).
Time restriction reduced trade count vs all-hours crossover but did
not improve signal quality. 837 of 854 trades in RANGING (WR 44%,
PF 0.60). EMA crossovers are lagging indicators — by the time
the 9 crosses the 21, the move that caused the cross is often
already fading. The session window hypothesis (more liquidity =
better signals) did not hold for this indicator type.

---

## Funding Reversal

**File:** `strategies/funding_reversal.py` (deleted)
**Tested:** 2026-03-28
**Verdict:** REJECTED — PF 0.57 < 1.0; MaxDD 51.8% > 20%

| Trades | WR | PF | MaxDD | P&L |
|---|---|---|---|---|
| 548 | 55.3% | 0.57 | -51.8% | -$128.00 |

Fade exhaustion after 6+ consecutive candles with extreme RSI. WR
passes the 55% threshold marginally, but PF 0.57 means losses dwarf
wins. With TP 4% / SL 6%, breakeven WR is ~71%. Regime breakdown
shows BULL_TREND WR 92% / PF 5.05 (13 trades) and BEAR_TREND WR 75%
/ PF 1.52 (16 trades) — exhaustion fades work in trends but sample
sizes are too small to validate. RANGING (519 trades, WR 54%,
PF 0.53) destroys overall performance. The 6-candle run condition
in ranging markets catches normal oscillations, not true exhaustion.

---

## VWAP Reversion (5m)

**File:** `strategies/vwap_reversion.py` (deleted)
**Tested:** 2026-04-01
**Timeframe:** 5m candles, $75 capital, 0% maker fees
**Verdict:** REJECTED — WR 36.0% < 52%; PF 0.78 < 1.0; MaxDD 61.8% > 25%

| Trades | WR | PF | MaxDD | P&L |
|---|---|---|---|---|
| 8,121 | 36.0% | 0.78 | -61.8% | -$46.02 |

Mean reversion to daily VWAP with RSI oversold/overbought and volume
confirmation. 8,105 of 8,121 trades in RANGING regime (WR 36%, PF 0.78).
The 0.8% VWAP deviation threshold fires far too often on 5m candles —
price oscillates around VWAP continuously in ranging conditions. Slippage
($11.60) and spread ($3.87) totaled $15.47 on $75 capital despite 0% fees.
The RSI 35/65 filters are too loose to identify genuine mean-reversion setups.

---

## Opening Range Breakout (5m)

**File:** `strategies/opening_range.py` (deleted)
**Tested:** 2026-04-01
**Timeframe:** 5m candles, $75 capital, 0% maker fees
**Verdict:** REJECTED — WR 39.7% < 52%; PF 0.87 < 1.0

| Trades | WR | PF | MaxDD | P&L |
|---|---|---|---|---|
| 237 | 39.7% | 0.87 | -4.0% | -$2.09 |

UTC midnight opening range (first 30min) breakout on 5m candles. All 237
trades in RANGING regime. Low drawdown (-4.0%) and low trade count
suggest the time window filter (00:30–04:00 UTC) and range width filter
(0.3%–2.0%) are working, but breakout direction is still a coin flip at
39.7% WR. Crypto has no true "opening" — UTC midnight is arbitrary and
does not produce the same range-setting behavior as equity market opens.
Previously rejected on 15m (WR 46.6%); 5m version is even worse.

---

## Volume Profile Scalp (5m)

**File:** `strategies/volume_profile_scalp.py` (deleted)
**Tested:** 2026-04-01
**Timeframe:** 5m candles, $75 capital, 0% maker fees
**Verdict:** REJECTED — WR 35.3% < 52%; PF 0.84 < 1.0; MaxDD 30.9% > 25%

| Trades | WR | PF | MaxDD | P&L |
|---|---|---|---|---|
| 6,915 | 35.3% | 0.84 | -30.9% | -$23.10 |

Volume profile POC/HVN scalping on 5m candles. 6,899 of 6,915 trades in
RANGING. The 0.2% proximity threshold with a 100-candle lookback creates
an almost continuous set of "key levels" — price is nearly always near
some HVN. The reversal candle pattern (previous candle opposite color +
current candle confirms) is too common on 5m timeframe to have predictive
power. Slippage + spread: $16.77 on $75 capital.

---

## Momentum Continuation (5m)

**File:** `strategies/momentum_continuation.py` (deleted)
**Tested:** 2026-04-01
**Timeframe:** 5m candles, $75 capital, 0% maker fees
**Verdict:** REJECTED — WR 30.5% < 52%; PF 0.85 < 1.0

| Trades | WR | PF | MaxDD | P&L |
|---|---|---|---|---|
| 2,249 | 30.5% | 0.85 | -14.7% | -$10.57 |

Momentum continuation after strong candles (body > 0.3%, > 70% range,
2x volume) on 5m. The 2:1 TP:SL ratio (1.2% TP / 0.6% SL) requires only
33% WR to break even on gross, but WR was 30.5% — even the favorable
asymmetry could not overcome. 2,244 of 2,249 trades in RANGING where
momentum candles are noise, not signal. The 0.3% body threshold is too
low for 5m BTC candles, firing ~7 times/week into random volatility.
S/R proximity check (last 20 candle highs/lows) did not meaningfully
filter because price always has nearby levels on 5m timeframe.
