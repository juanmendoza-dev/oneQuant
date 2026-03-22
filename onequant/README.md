# oneQuant — Crypto Data Pipeline

oneQuant is a real-time cryptocurrency data pipeline that collects BTC-USD market data from multiple sources and stores it in a local SQLite database. It streams live price data via the Coinbase Advanced Trade WebSocket API, polls news headlines from CryptoPanic, tracks the Fear & Greed Index, and can backfill up to 2 years of historical OHLCV candles. This is Phase 1 of a self-improving algorithmic trading bot — data collection only.

## Prerequisites

- **Python 3.11+**
- **git** with GPG signing configured (Kleopatra recommended on Windows)
- **Coinbase Advanced Trade** API key and secret
- **CryptoPanic** API key (free tier)

## Setup

```bash
# 1. Clone the repository
git clone https://github.com/juanmendoza-dev/oneQuant.git
cd oneQuant/onequant

# 2. Create and activate a virtual environment
python -m venv venv
source venv/bin/activate        # Linux/macOS
venv\Scripts\activate           # Windows

# 3. Install dependencies
pip install -r requirements.txt

# 4. Configure environment variables
cp .env.example .env
# Edit .env and fill in your real API keys

# 5. (Optional) Backfill historical candles
python -m historical.fetch

# 6. Start the data pipeline
python main.py
```

## Verifying Data Flow

After running for a few minutes, check that data is flowing:

```bash
sqlite3 onequant.db "SELECT COUNT(*) FROM btc_candles;"
sqlite3 onequant.db "SELECT COUNT(*) FROM news_feed;"
sqlite3 onequant.db "SELECT COUNT(*) FROM fear_greed;"
sqlite3 onequant.db "SELECT * FROM btc_candles ORDER BY timestamp DESC LIMIT 5;"
sqlite3 onequant.db "SELECT * FROM news_feed ORDER BY timestamp DESC LIMIT 5;"
sqlite3 onequant.db "SELECT * FROM fear_greed ORDER BY timestamp DESC LIMIT 5;"
```

BTC candles should appear every 5 minutes, news headlines and Fear & Greed scores every 15 minutes.

## GPG Commit Signing Setup

All commits to this repository must be GPG-signed.

```bash
# Generate a GPG key (if you don't have one)
gpg --full-generate-key

# List your keys to find the key ID
gpg --list-secret-keys --keyid-format=long

# Configure git to sign commits
git config --local commit.gpgsign true
git config --local user.signingkey YOUR_KEY_ID

# Verify a signed commit
git log --show-signature -1
```

On Windows, install [Kleopatra](https://www.gpg4win.org/) for GPG key management. Ensure `gpg` is in your PATH.

## Project Structure

```
onequant/
├── .env.example          # Environment variable template
├── .gitignore            # Git ignore rules
├── requirements.txt      # Pinned Python dependencies
├── README.md             # This file
├── config.py             # Central configuration loader
├── database/
│   ├── __init__.py
│   └── db.py             # SQLite schema, connection, insert helpers
├── feeds/
│   ├── __init__.py
│   ├── coinbase_ws.py    # Coinbase WebSocket real-time BTC-USD feed
│   ├── coinbase_rest.py  # Coinbase REST API helpers
│   └── news.py           # CryptoPanic + Fear & Greed pollers
├── historical/
│   ├── __init__.py
│   └── fetch.py          # Historical OHLCV candle backfiller
├── logs/                 # Log files (created at runtime, gitignored)
└── main.py               # Entry point — starts all feeds concurrently
```

## Deployment

Developed and tested on a local machine. Intended for 24/7 deployment on a Proxmox LXC container running Ubuntu 24.04.
