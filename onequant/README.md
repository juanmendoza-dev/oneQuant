# oneQuant — Phase 1: Data Pipeline

Self-improving prediction market trading bot. This phase collects real-time and historical data from Coinbase, Kalshi, CryptoPanic, and the Fear & Greed Index.

## Setup

```bash
cd onequant
python -m venv venv
source venv/bin/activate   # Linux/Mac
# or: venv\Scripts\activate  # Windows

pip install -r requirements.txt
cp .env.example .env
# Edit .env with your actual API keys
```

## Required API Keys

| Service | Where to get it |
|---------|----------------|
| Coinbase Advanced Trade | https://www.coinbase.com/settings/api |
| Kalshi | https://kalshi.com/account/api |
| CryptoPanic | https://cryptopanic.com/developers/api/ |

## Usage

### Fetch historical data (run once)

```bash
python -m historical.fetch
```

This fetches 2 years of BTC-USD OHLCV candles (5m, 15m, 1h) from Coinbase.

### Start the live data pipeline

```bash
python main.py
```

This runs concurrently:
- **Coinbase WebSocket** — real-time BTC-USD ticker, builds 5m/15m/1h candles
- **Kalshi poller** — BTC direction markets every 60 seconds
- **News poller** — CryptoPanic headlines + Fear & Greed Index every 15 minutes

Stop with `CTRL+C`.

## Database

SQLite database at `./onequant.db` (configurable via `DATABASE_PATH` env var).

Tables: `btc_candles`, `kalshi_markets`, `news_feed`, `fear_greed`

## Logs

Feed-specific logs are written to `logs/` (auto-created at runtime).
