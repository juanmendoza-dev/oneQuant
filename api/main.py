"""oneQuant Dashboard API — FastAPI backend serving trade, strategy, and system data."""

import hashlib
import os
import sqlite3
from datetime import datetime, timezone
from pathlib import Path

from fastapi import FastAPI, Query
from fastapi.middleware.cors import CORSMiddleware

app = FastAPI(title="oneQuant API", version="0.1.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

PROJECT_ROOT = Path("/root/oneQuant")
DB_PATH = PROJECT_ROOT / "onequant" / "onequant.db"
STRATEGIES_DIR = PROJECT_ROOT / "onequant" / "strategies"
ENGINE_HASH_FILE = PROJECT_ROOT / "onequant" / "ENGINE_HASH.txt"
ENGINE_FILE = PROJECT_ROOT / "onequant" / "backtest" / "engine.py"
LOGS_DIR = PROJECT_ROOT / "logs"
ONEQUANT_LOGS_DIR = PROJECT_ROOT / "onequant" / "logs"

REJECTED_STRATEGIES = {"bb_reversion", "breakout", "vwap_momentum"}
VALIDATED_STRATEGIES = {"mean_reversion"}

BACKTEST_PREDICTED_WR = 76.8


def get_db():
    conn = sqlite3.connect(str(DB_PATH))
    conn.row_factory = sqlite3.Row
    return conn


# --------------------------------------------------------------------------- #
# GET /api/health
# --------------------------------------------------------------------------- #


@app.get("/api/health")
def health():
    return {"status": "ok", "timestamp": datetime.now(timezone.utc).isoformat()}


# --------------------------------------------------------------------------- #
# GET /api/paper-trades
# --------------------------------------------------------------------------- #


@app.get("/api/paper-trades")
def paper_trades():
    conn = get_db()
    try:
        rows = conn.execute(
            "SELECT * FROM paper_trades ORDER BY created_at DESC LIMIT 50"
        ).fetchall()
        return [dict(r) for r in rows]
    except sqlite3.OperationalError:
        return []
    finally:
        conn.close()


# --------------------------------------------------------------------------- #
# GET /api/paper-stats
# --------------------------------------------------------------------------- #


@app.get("/api/paper-stats")
def paper_stats():
    conn = get_db()
    try:
        rows = conn.execute(
            "SELECT status, pnl, pnl_pct FROM paper_trades WHERE status IN ('WIN', 'LOSS')"
        ).fetchall()
    except sqlite3.OperationalError:
        return {
            "total_trades": 0,
            "wins": 0,
            "losses": 0,
            "win_rate": 0.0,
            "total_pnl": 0.0,
            "backtest_predicted_wr": BACKTEST_PREDICTED_WR,
            "divergence_pct": 0.0,
        }
    finally:
        conn.close()

    total = len(rows)
    wins = sum(1 for r in rows if r["status"] == "WIN")
    losses = total - wins
    win_rate = round((wins / total) * 100, 2) if total > 0 else 0.0
    total_pnl = round(sum(r["pnl"] or 0 for r in rows), 2)
    divergence = round(win_rate - BACKTEST_PREDICTED_WR, 2) if total > 0 else 0.0

    return {
        "total_trades": total,
        "wins": wins,
        "losses": losses,
        "win_rate": win_rate,
        "total_pnl": total_pnl,
        "backtest_predicted_wr": BACKTEST_PREDICTED_WR,
        "divergence_pct": divergence,
    }


# --------------------------------------------------------------------------- #
# GET /api/strategies
# --------------------------------------------------------------------------- #


@app.get("/api/strategies")
def strategies():
    results = []
    for f in sorted(STRATEGIES_DIR.glob("*.py")):
        name = f.stem
        if name.startswith("_") or name == "base":
            continue
        if name in REJECTED_STRATEGIES:
            status = "rejected"
        elif name in VALIDATED_STRATEGIES:
            status = "validated"
        else:
            status = "candidate"
        results.append({"name": name, "file": f.name, "status": status})
    return results


# --------------------------------------------------------------------------- #
# GET /api/engine-status
# --------------------------------------------------------------------------- #


@app.get("/api/engine-status")
def engine_status():
    # Read stored hash
    stored_hash = None
    if ENGINE_HASH_FILE.exists():
        content = ENGINE_HASH_FILE.read_text().strip()
        if content:
            stored_hash = content.split()[0]

    # Compute current hash
    current_hash = None
    if ENGINE_FILE.exists():
        current_hash = hashlib.sha256(ENGINE_FILE.read_bytes()).hexdigest()

    hash_match = stored_hash == current_hash if (stored_hash and current_hash) else None

    # Read last audit log lines
    last_audit = None
    for log_dir in [ONEQUANT_LOGS_DIR, LOGS_DIR]:
        for log_file in log_dir.glob("*.log"):
            try:
                text = log_file.read_text()
                for line in reversed(text.strip().splitlines()):
                    if "audit" in line.lower() or "engine" in line.lower():
                        last_audit = line.strip()
                        break
                if last_audit:
                    break
            except Exception:
                continue
        if last_audit:
            break

    return {
        "stored_hash": stored_hash,
        "current_hash": current_hash,
        "hash_match": hash_match,
        "last_audit": last_audit,
    }


# --------------------------------------------------------------------------- #
# GET /api/candles
# --------------------------------------------------------------------------- #


@app.get("/api/candles")
def candles(limit: int = Query(default=100, le=5000)):
    conn = get_db()
    try:
        rows = conn.execute(
            "SELECT timestamp, open, high, low, close, volume, timeframe, symbol "
            "FROM btc_candles ORDER BY timestamp DESC LIMIT ?",
            (limit,),
        ).fetchall()
        return [dict(r) for r in rows]
    except sqlite3.OperationalError:
        return []
    finally:
        conn.close()


# --------------------------------------------------------------------------- #
# GET /api/system-logs
# --------------------------------------------------------------------------- #


@app.get("/api/system-logs")
def system_logs(limit: int = Query(default=50, le=500)):
    conn = get_db()
    try:
        rows = conn.execute(
            "SELECT * FROM system_log ORDER BY timestamp DESC LIMIT ?",
            (limit,),
        ).fetchall()
        return [dict(r) for r in rows]
    except sqlite3.OperationalError:
        return []
    finally:
        conn.close()
