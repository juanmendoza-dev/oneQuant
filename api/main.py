"""oneQuant Dashboard API — FastAPI backend serving trade, strategy, and system data."""

import hashlib
import os
import sqlite3
import subprocess
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

STRATEGY_COLORS = {
    "mean_reversion": "#9b59b6",
    "capitulation": "#e67e22",
    "ema_pullback": "#06b6d4",
    "momentum": "#eab308",
    "mtf_mean_reversion": "#22c55e",
    "news_driven": "#ec4899",
    "rsi_divergence": "#ef4444",
    "trend_exhaustion": "#3b82f6",
}


def get_db():
    conn = sqlite3.connect(str(DB_PATH))
    conn.row_factory = sqlite3.Row
    return conn


# --------------------------------------------------------------------------- #
# GET /api/health
# --------------------------------------------------------------------------- #


def _service_active(name: str) -> bool:
    try:
        r = subprocess.run(
            ["systemctl", "is-active", name],
            capture_output=True, text=True, timeout=5,
        )
        return r.stdout.strip() == "active"
    except Exception:
        return False


def _db_counts() -> list[dict]:
    tables = ["btc_candles", "news_feed", "fear_greed", "system_log", "paper_trades",
              "market_maker_trades", "market_maker_stats"]
    results = []
    try:
        conn = get_db()
        for t in tables:
            count = conn.execute(f"SELECT COUNT(*) FROM {t}").fetchone()[0]  # noqa: S608
            results.append({"label": t, "value": count})
        conn.close()
    except Exception:
        pass
    return results


@app.get("/api/health")
def health():
    svc_names = [
        ("onequant", "Trading Engine"),
        ("onequant-api", "API Server"),
        ("nginx", "Nginx"),
        ("cloudflared", "Cloudflare Tunnel"),
    ]
    services = []
    for svc_id, label in svc_names:
        active = _service_active(svc_id)
        services.append({
            "name": label,
            "healthy": active,
            "status": "RUNNING" if active else "DOWN",
            "detail": "",
        })

    return {
        "status": "ok",
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "services": services,
        "db_stats": _db_counts(),
    }


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


@app.get("/api/paper-race")
def paper_race(timeframe: str = Query(default="15m")):
    conn = get_db()
    try:
        rows = conn.execute(
            "SELECT strategy, signal_time, exit_time, pnl, status, "
            "entry_price, exit_price, direction "
            "FROM paper_trades WHERE timeframe = ? ORDER BY signal_time ASC",
            (timeframe,),
        ).fetchall()
    except sqlite3.OperationalError:
        rows = []
    finally:
        conn.close()

    # Check for open positions
    open_positions = set()
    try:
        conn2 = get_db()
        open_rows = conn2.execute(
            "SELECT DISTINCT strategy FROM paper_trades WHERE status = 'OPEN'"
        ).fetchall()
        open_positions = {r["strategy"] for r in open_rows}
        conn2.close()
    except Exception:
        pass

    # Group trades by strategy
    by_strategy: dict[str, list] = {}
    for r in rows:
        s = r["strategy"]
        if s not in by_strategy:
            by_strategy[s] = []
        by_strategy[s].append(dict(r))

    # All known strategies (even those with no trades yet)
    all_strats = set(STRATEGY_COLORS.keys()) | set(by_strategy.keys())

    strategies = []
    for name in sorted(all_strats):
        color = STRATEGY_COLORS.get(name, "#888888")
        trades_raw = by_strategy.get(name, [])
        cumulative = 0.0
        trades = []
        wins = 0
        closed = 0
        for t in trades_raw:
            if t["status"] in ("WIN", "LOSS"):
                pnl = t["pnl"] or 0.0
                cumulative += pnl
                closed += 1
                if t["status"] == "WIN":
                    wins += 1
                trades.append({
                    "time": datetime.fromtimestamp(
                        t["exit_time"] or t["signal_time"], tz=timezone.utc
                    ).isoformat(),
                    "pnl": round(pnl, 2),
                    "cumulative_pnl": round(cumulative, 2),
                    "result": t["status"],
                    "entry_price": t["entry_price"],
                    "exit_price": t["exit_price"],
                })

        wr = round((wins / closed) * 100, 2) if closed > 0 else 0.0
        strategies.append({
            "name": name,
            "color": color,
            "trades": trades,
            "current_pnl": round(cumulative, 2),
            "win_rate": wr,
            "predicted_wr": BACKTEST_PREDICTED_WR,
            "is_beating_prediction": wr >= BACKTEST_PREDICTED_WR if closed > 0 else False,
            "open_position": name in open_positions,
        })

    return {"strategies": strategies}


@app.get("/api/system-logs")
def system_logs(limit: int = Query(default=50, le=500)):
    conn = get_db()
    try:
        rows = conn.execute(
            "SELECT * FROM system_log ORDER BY timestamp DESC LIMIT ?",
            (limit,),
        ).fetchall()
        results = []
        for r in rows:
            d = dict(r)
            ts = d.get("timestamp")
            if isinstance(ts, (int, float)):
                d["ts"] = datetime.fromtimestamp(ts, tz=timezone.utc).strftime("%H:%M:%S")
            else:
                d["ts"] = str(ts) if ts else ""
            results.append(d)
        return results
    except sqlite3.OperationalError:
        return []
    finally:
        conn.close()


# --------------------------------------------------------------------------- #
# GET /api/market-maker
# --------------------------------------------------------------------------- #


@app.get("/api/market-maker")
def market_maker():
    conn = get_db()
    try:
        # Total stats
        row = conn.execute(
            """SELECT
                   COUNT(*) AS total_round_trips,
                   COALESCE(SUM(spread_collected_usd), 0.0) AS total_spread
               FROM market_maker_trades
               WHERE status = 'FILLED' AND spread_collected_usd > 0"""
        ).fetchone()
        total_round_trips = row["total_round_trips"] if row else 0
        total_spread = row["total_spread"] if row else 0.0

        # Today's stats
        today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        day_row = conn.execute(
            """SELECT
                   COUNT(*) AS rt,
                   COALESCE(SUM(spread_collected_usd), 0.0) AS spread
               FROM market_maker_trades
               WHERE status = 'FILLED' AND spread_collected_usd > 0
                 AND timestamp LIKE ?""",
            (today + "%",),
        ).fetchone()
        daily_rt = day_row["rt"] if day_row else 0
        daily_spread = day_row["spread"] if day_row else 0.0

        # This week's stats (last 7 days)
        week_row = conn.execute(
            """SELECT
                   COALESCE(SUM(spread_collected_usd), 0.0) AS spread
               FROM market_maker_trades
               WHERE status = 'FILLED' AND spread_collected_usd > 0
                 AND timestamp >= datetime('now', '-7 days')"""
        ).fetchone()
        weekly_spread = week_row["spread"] if week_row else 0.0

        # Latest price from a recent trade
        price_row = conn.execute(
            "SELECT price FROM market_maker_trades ORDER BY id DESC LIMIT 1"
        ).fetchone()
        last_price = price_row["price"] if price_row else 0.0

        return {
            "mode": "PAPER",
            "capital_usd": 75.0,
            "total_round_trips": total_round_trips,
            "total_spread_collected": round(total_spread, 4),
            "daily_round_trips": daily_rt,
            "daily_spread_collected": round(daily_spread, 4),
            "weekly_spread_collected": round(weekly_spread, 4),
            "btc_inventory": 0.0,
            "usd_inventory": 75.0,
            "active_buy_price": None,
            "active_sell_price": None,
            "last_price": last_price,
            "status": "RUNNING",
        }

    except sqlite3.OperationalError:
        return {
            "mode": "PAPER",
            "capital_usd": 75.0,
            "total_round_trips": 0,
            "total_spread_collected": 0.0,
            "daily_round_trips": 0,
            "daily_spread_collected": 0.0,
            "weekly_spread_collected": 0.0,
            "btc_inventory": 0.0,
            "usd_inventory": 75.0,
            "active_buy_price": None,
            "active_sell_price": None,
            "last_price": 0.0,
            "status": "RUNNING",
        }
    finally:
        conn.close()
