"""la_perra.py — Daily audit and monitoring script for oneQuant.

Runs 6 engine integrity tests, verifies SHA256 of engine.py, queries paper
trade win rate, calls Claude Haiku to analyze for suspicious patterns, and
sends a morning summary to Telegram.

Includes hardware monitoring (CPU temp, memory, disk), daily cost tracking
(API + electricity), and weekly/monthly cost summaries.

All API keys loaded from onequant/.env only.
"""

import calendar
import hashlib
import json
import os
import re
import shutil
import sqlite3
import subprocess
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

import anthropic
import requests

# ---------------------------------------------------------------------------
# Path setup
# ---------------------------------------------------------------------------

REPO_ROOT = Path(__file__).resolve().parent.parent
ONEQUANT_DIR = REPO_ROOT / "onequant"

# Load .env before any onequant imports
try:
    from dotenv import load_dotenv
    load_dotenv(ONEQUANT_DIR / ".env")
except ImportError:
    env_path = ONEQUANT_DIR / ".env"
    if env_path.exists():
        for line in env_path.read_text().splitlines():
            line = line.strip()
            if line and not line.startswith("#") and "=" in line:
                k, _, v = line.partition("=")
                os.environ.setdefault(k.strip(), v.strip())

sys.path.insert(0, str(ONEQUANT_DIR))

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

_db_path_raw = os.getenv("DATABASE_PATH", "onequant.db").lstrip("./")
DB_PATH = ONEQUANT_DIR / _db_path_raw
ENGINE_PATH = ONEQUANT_DIR / "backtest" / "engine.py"
ENGINE_HASH_PATH = ONEQUANT_DIR / "ENGINE_HASH.txt"
BACKTEST_PREDICTED_WR = 76.8

ANTHROPIC_API_KEY = os.getenv("ANTHROPIC_API_KEY", "")
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "")

LOGS_DIR = REPO_ROOT / "logs"
STRATEGIES_DIR = ONEQUANT_DIR / "strategies"

# Low-frequency strategies where 0 trades over weeks is normal
LOW_FREQ_STRATEGIES = {"mean_reversion", "bb_reversion"}

HAIKU_MODEL = "claude-haiku-4-5-20251001"
TELEGRAM_API_URL = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"

# Electricity constants (FPL Pembroke Pines, FL — Tier 2)
SERVER_WATTAGE = 70    # estimated continuous watts
FPL_RATE = 0.146       # $/kWh tier 2

# Estimated API costs per run
API_COSTS = {
    "el_chef": 0.38,       # Sonnet + Opus per run
    "el_mecanico": 0.14,   # Sonnet + Opus per run
    "la_perra": 0.003,     # Haiku per run
}


# ---------------------------------------------------------------------------
# Check 1: Engine audit
# ---------------------------------------------------------------------------

def run_audit() -> tuple[bool, str]:
    """Run python -m backtest.audit and return (passed, output_text)."""
    try:
        proc = subprocess.run(
            [sys.executable, "-m", "backtest.audit"],
            cwd=str(ONEQUANT_DIR),
            capture_output=True,
            text=True,
            timeout=120,
        )
        output = proc.stdout + proc.stderr
        return proc.returncode == 0, output
    except subprocess.TimeoutExpired:
        return False, "Audit timed out after 120 seconds."
    except Exception as e:
        return False, f"Audit subprocess error: {e}"


# ---------------------------------------------------------------------------
# Check 2: Engine SHA256
# ---------------------------------------------------------------------------

def check_engine_hash() -> tuple[bool, str, str, str]:
    """Check SHA256 of engine.py against ENGINE_HASH.txt.

    Returns (matches, actual_hash, expected_hash, status).
    Status is one of: MATCH, MISMATCH, BASELINE_SET.
    On first run (no hash file), writes baseline and returns BASELINE_SET.
    """
    with open(ENGINE_PATH, "rb") as f:
        actual_hash = hashlib.sha256(f.read()).hexdigest()

    if not ENGINE_HASH_PATH.exists():
        ENGINE_HASH_PATH.write_text(actual_hash)
        return True, actual_hash, actual_hash, "BASELINE_SET"

    # ENGINE_HASH.txt may contain "hash  filename" (sha256sum format) or just hash
    raw = ENGINE_HASH_PATH.read_text().strip()
    expected_hash = raw.split()[0]  # take first token in case of "hash  path" format

    matches = actual_hash == expected_hash
    status = "MATCH" if matches else "MISMATCH"
    return matches, actual_hash, expected_hash, status


# ---------------------------------------------------------------------------
# Check 3: Paper trades query
# ---------------------------------------------------------------------------

def query_paper_trades() -> dict:
    """Query paper_trades for actual win rate. Returns a stats dict."""
    if not DB_PATH.exists():
        return {
            "db_status": "NO_DATABASE",
            "message": "Paper trading not started — onequant.db not found.",
            "total_trades": 0,
            "wins": 0,
            "losses": 0,
            "actual_win_rate_pct": 0.0,
            "predicted_win_rate_pct": BACKTEST_PREDICTED_WR,
            "divergence_pct": 0.0,
            "gross_profit": 0.0,
            "gross_loss": 0.0,
        }

    try:
        conn = sqlite3.connect(str(DB_PATH))
        conn.row_factory = sqlite3.Row
        try:
            row = conn.execute("""
                SELECT
                    COUNT(*) AS total,
                    SUM(CASE WHEN status = 'WIN' THEN 1 ELSE 0 END) AS wins,
                    SUM(CASE WHEN status = 'LOSS' THEN 1 ELSE 0 END) AS losses,
                    SUM(CASE WHEN pnl > 0 THEN pnl ELSE 0 END) AS gross_profit,
                    SUM(CASE WHEN pnl < 0 THEN ABS(pnl) ELSE 0 END) AS gross_loss
                FROM paper_trades
                WHERE status IN ('WIN', 'LOSS')
            """).fetchone()
        finally:
            conn.close()

        total = row["total"] or 0
        wins = row["wins"] or 0
        losses = row["losses"] or 0
        gross_profit = row["gross_profit"] or 0.0
        gross_loss = row["gross_loss"] or 0.0

        actual_wr = (wins / total * 100.0) if total > 0 else 0.0
        divergence = actual_wr - BACKTEST_PREDICTED_WR

        return {
            "db_status": "OK",
            "message": "",
            "total_trades": total,
            "wins": wins,
            "losses": losses,
            "actual_win_rate_pct": actual_wr,
            "predicted_win_rate_pct": BACKTEST_PREDICTED_WR,
            "divergence_pct": divergence,
            "gross_profit": gross_profit,
            "gross_loss": gross_loss,
        }

    except sqlite3.Error as e:
        return {
            "db_status": "DB_ERROR",
            "message": f"SQLite error: {e}",
            "total_trades": 0,
            "wins": 0,
            "losses": 0,
            "actual_win_rate_pct": 0.0,
            "predicted_win_rate_pct": BACKTEST_PREDICTED_WR,
            "divergence_pct": 0.0,
            "gross_profit": 0.0,
            "gross_loss": 0.0,
        }


# ---------------------------------------------------------------------------
# Check 4: Claude Haiku analysis
# ---------------------------------------------------------------------------

def call_claude(findings: dict) -> str:
    """Call Claude Haiku for suspicious pattern analysis. Returns CLEAN or SUSPICIOUS + reason."""
    if not ANTHROPIC_API_KEY:
        return "[Claude analysis unavailable: ANTHROPIC_API_KEY not set]"

    audit_json = json.dumps(findings, indent=2, default=str)

    prompt = f"""You are a quantitative trading system auditor. Analyze this audit JSON from a Bitcoin paper trading bot.

{audit_json}

Check for these suspicious patterns:
1. Win rate (WR) above 90%
2. Profit factor (PF) above 3.0
3. Max drawdown below 0.5%
4. Zero losing trades
5. Paper vs backtest divergence above 10 percentage points

IMPORTANT: Zero completed trades is NORMAL and NOT suspicious if:
- The system has been running less than 30 days, OR
- The strategies are low-frequency (e.g. mean_reversion, bb_reversion) which may only fire ~5 times per year.
Only flag zero trades as suspicious if the system has been running > 30 days AND all strategies are high-frequency.

Respond with exactly one line:
- "CLEAN" if no suspicious patterns are found
- "SUSPICIOUS: <reason>" if any pattern is detected (list all that apply)"""

    try:
        client = anthropic.Anthropic(api_key=ANTHROPIC_API_KEY)
        message = client.messages.create(
            model=HAIKU_MODEL,
            max_tokens=256,
            temperature=0.3,
            messages=[{"role": "user", "content": prompt}],
        )
        return message.content[0].text.strip()
    except anthropic.APIError as e:
        return f"[Claude analysis unavailable: {e}]"
    except Exception as e:
        return f"[Claude analysis error: {e}]"


# ---------------------------------------------------------------------------
# Hardware monitoring
# ---------------------------------------------------------------------------

def get_hardware_stats() -> dict:
    """Collect hardware stats: CPU temp, memory, DB size, disk usage."""
    # CPU temperature from thermal zones
    cpu_temp = None
    try:
        temps = []
        thermal_base = Path("/sys/class/thermal")
        for zone in sorted(thermal_base.glob("thermal_zone*")):
            temp_file = zone / "temp"
            if temp_file.exists():
                raw = temp_file.read_text().strip()
                temps.append(int(raw) / 1000.0)
        if temps:
            cpu_temp = sum(temps) / len(temps)
    except (OSError, ValueError):
        pass

    # Memory usage
    memory_pct = None
    try:
        meminfo = Path("/proc/meminfo").read_text()
        mem_total = mem_avail = None
        for line in meminfo.splitlines():
            if line.startswith("MemTotal:"):
                mem_total = int(line.split()[1])
            elif line.startswith("MemAvailable:"):
                mem_avail = int(line.split()[1])
        if mem_total and mem_avail:
            memory_pct = (mem_total - mem_avail) / mem_total * 100.0
    except (OSError, ValueError):
        pass

    # DB size
    db_size_mb = None
    try:
        if DB_PATH.exists():
            db_size_mb = DB_PATH.stat().st_size / (1024 * 1024)
    except OSError:
        pass

    # Disk usage
    disk_usage_pct = None
    try:
        usage = shutil.disk_usage("/root")
        disk_usage_pct = usage.used / usage.total * 100.0
    except OSError:
        pass

    return {
        "cpu_temp_c": round(cpu_temp, 1) if cpu_temp is not None else None,
        "memory_pct": round(memory_pct, 1) if memory_pct is not None else None,
        "db_size_mb": round(db_size_mb, 1) if db_size_mb is not None else None,
        "disk_usage_pct": round(disk_usage_pct, 1) if disk_usage_pct is not None else None,
    }


# ---------------------------------------------------------------------------
# Cost calculations
# ---------------------------------------------------------------------------

def calculate_electricity_cost(hours: float = 24.0) -> dict:
    """Calculate electricity cost for the given number of hours."""
    kwh = (SERVER_WATTAGE * hours) / 1000.0
    cost = kwh * FPL_RATE
    return {"kwh": round(kwh, 3), "cost": round(cost, 4)}


def calculate_api_costs() -> dict:
    """Return estimated daily API costs."""
    total = sum(API_COSTS.values())
    return {
        "el_chef": API_COSTS["el_chef"],
        "el_mecanico": API_COSTS["el_mecanico"],
        "la_perra": API_COSTS["la_perra"],
        "total": round(total, 3),
    }


# ---------------------------------------------------------------------------
# Daily costs database operations
# ---------------------------------------------------------------------------

def _ensure_daily_costs_table(conn: sqlite3.Connection) -> None:
    """Create daily_costs table if it doesn't exist (for standalone usage)."""
    conn.execute("""
        CREATE TABLE IF NOT EXISTS daily_costs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            date TEXT NOT NULL UNIQUE,
            el_chef_cost REAL DEFAULT 0.0,
            el_mecanico_cost REAL DEFAULT 0.0,
            la_perra_cost REAL DEFAULT 0.0,
            total_api_cost REAL DEFAULT 0.0,
            electricity_kwh REAL DEFAULT 0.0,
            electricity_cost REAL DEFAULT 0.0,
            total_cost REAL DEFAULT 0.0,
            avg_cpu_temp_c REAL DEFAULT 0.0,
            peak_cpu_temp_c REAL DEFAULT 0.0,
            avg_memory_pct REAL DEFAULT 0.0,
            db_size_mb REAL DEFAULT 0.0,
            candidates_generated INTEGER DEFAULT 0,
            candidates_passed INTEGER DEFAULT 0,
            created_at TEXT DEFAULT (datetime('now'))
        )
    """)
    conn.commit()


def save_daily_costs(hw_stats: dict, api_costs: dict, elec: dict,
                     candidates_generated: int = 0, candidates_passed: int = 0) -> None:
    """Insert or replace today's cost record."""
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    total_api = api_costs["total"]
    total_cost = total_api + elec["cost"]

    conn = sqlite3.connect(str(DB_PATH))
    try:
        _ensure_daily_costs_table(conn)
        conn.execute("""
            INSERT OR REPLACE INTO daily_costs
                (date, el_chef_cost, el_mecanico_cost, la_perra_cost,
                 total_api_cost, electricity_kwh, electricity_cost, total_cost,
                 avg_cpu_temp_c, peak_cpu_temp_c, avg_memory_pct, db_size_mb,
                 candidates_generated, candidates_passed)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            today,
            api_costs["el_chef"],
            api_costs["el_mecanico"],
            api_costs["la_perra"],
            total_api,
            elec["kwh"],
            elec["cost"],
            total_cost,
            hw_stats.get("cpu_temp_c") or 0.0,
            hw_stats.get("cpu_temp_c") or 0.0,  # peak = current for single reading
            hw_stats.get("memory_pct") or 0.0,
            hw_stats.get("db_size_mb") or 0.0,
            candidates_generated,
            candidates_passed,
        ))
        conn.commit()
    finally:
        conn.close()


def get_weekly_summary() -> dict:
    """Query daily_costs for last 7 days."""
    end = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    start = (datetime.now(timezone.utc) - timedelta(days=6)).strftime("%Y-%m-%d")

    conn = sqlite3.connect(str(DB_PATH))
    try:
        _ensure_daily_costs_table(conn)
        row = conn.execute("""
            SELECT
                COALESCE(SUM(total_api_cost), 0.0),
                COALESCE(SUM(electricity_cost), 0.0),
                COALESCE(SUM(total_cost), 0.0),
                COALESCE(AVG(avg_cpu_temp_c), 0.0),
                COALESCE(MAX(peak_cpu_temp_c), 0.0),
                COALESCE(SUM(electricity_kwh), 0.0),
                COALESCE(SUM(candidates_generated), 0),
                COALESCE(SUM(candidates_passed), 0),
                COUNT(*)
            FROM daily_costs
            WHERE date BETWEEN ? AND ?
        """, (start, end)).fetchone()
    finally:
        conn.close()

    days = row[8] or 1
    return {
        "total_api_cost": round(row[0], 2),
        "total_electricity_cost": round(row[1], 2),
        "total_cost": round(row[2], 2),
        "daily_avg_cost": round(row[2] / days, 2),
        "avg_cpu_temp": round(row[3], 1),
        "peak_cpu_temp": round(row[4], 1),
        "total_kwh": round(row[5], 3),
        "candidates_generated": row[6],
        "candidates_passed": row[7],
        "days_recorded": days,
        "start_date": start,
        "end_date": end,
    }


def get_monthly_summary() -> dict:
    """Query daily_costs for the current calendar month."""
    now = datetime.now(timezone.utc)
    start = now.strftime("%Y-%m-01")
    end = now.strftime("%Y-%m-%d")
    days_in_month = calendar.monthrange(now.year, now.month)[1]

    conn = sqlite3.connect(str(DB_PATH))
    try:
        _ensure_daily_costs_table(conn)
        row = conn.execute("""
            SELECT
                COALESCE(SUM(total_api_cost), 0.0),
                COALESCE(SUM(electricity_cost), 0.0),
                COALESCE(SUM(total_cost), 0.0),
                COALESCE(SUM(el_chef_cost), 0.0),
                COALESCE(SUM(el_mecanico_cost), 0.0),
                COALESCE(SUM(la_perra_cost), 0.0),
                COALESCE(AVG(avg_cpu_temp_c), 0.0),
                COALESCE(MAX(peak_cpu_temp_c), 0.0),
                COALESCE(SUM(electricity_kwh), 0.0),
                COALESCE(SUM(candidates_generated), 0),
                COALESCE(SUM(candidates_passed), 0),
                COUNT(*),
                COALESCE(SUM(db_size_mb), 0.0)
            FROM daily_costs
            WHERE date BETWEEN ? AND ?
        """, (start, end)).fetchone()
    finally:
        conn.close()

    days = row[11] or 1
    total_cost = row[2]
    daily_avg = total_cost / days
    projected_annual = daily_avg * 365
    candidates_passed = row[10]
    cost_per_candidate = (total_cost / candidates_passed) if candidates_passed > 0 else 0.0

    # DB growth: difference between latest and earliest db_size_mb
    db_growth = 0.0
    conn2 = sqlite3.connect(str(DB_PATH))
    try:
        growth_row = conn2.execute("""
            SELECT
                (SELECT db_size_mb FROM daily_costs WHERE date BETWEEN ? AND ? ORDER BY date DESC LIMIT 1) -
                (SELECT db_size_mb FROM daily_costs WHERE date BETWEEN ? AND ? ORDER BY date ASC LIMIT 1)
        """, (start, end, start, end)).fetchone()
        if growth_row and growth_row[0]:
            db_growth = max(0.0, growth_row[0])
    finally:
        conn2.close()

    return {
        "month_name": now.strftime("%B"),
        "year": now.year,
        "total_api_cost": round(row[0], 2),
        "total_electricity_cost": round(row[1], 2),
        "total_cost": round(total_cost, 2),
        "el_chef_total": round(row[3], 2),
        "el_mecanico_total": round(row[4], 2),
        "la_perra_total": round(row[5], 2),
        "avg_cpu_temp": round(row[6], 1),
        "peak_cpu_temp": round(row[7], 1),
        "total_kwh": round(row[8], 3),
        "candidates_generated": row[9],
        "candidates_passed": row[10],
        "days_recorded": days,
        "daily_avg_cost": round(daily_avg, 2),
        "projected_annual": round(projected_annual, 2),
        "cost_per_candidate": round(cost_per_candidate, 2),
        "db_growth_mb": round(db_growth, 1),
        "projected_db_annual_mb": round(db_growth / days * 365, 1) if days > 0 else 0.0,
    }


# ---------------------------------------------------------------------------
# Market maker stats query
# ---------------------------------------------------------------------------

def query_mm_today() -> dict:
    """Query market maker round trips for today."""
    if not DB_PATH.exists():
        return {"round_trips": 0, "spread_collected": 0.0}
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    conn = sqlite3.connect(str(DB_PATH))
    try:
        row = conn.execute("""
            SELECT
                COALESCE(SUM(CASE WHEN spread_collected_usd > 0 THEN 1 ELSE 0 END), 0),
                COALESCE(SUM(spread_collected_usd), 0.0)
            FROM market_maker_trades
            WHERE timestamp LIKE ? AND status = 'FILLED'
        """, (today + "%",)).fetchone()
    except sqlite3.OperationalError:
        return {"round_trips": 0, "spread_collected": 0.0}
    finally:
        conn.close()
    return {
        "round_trips": row[0] if row else 0,
        "spread_collected": round(row[1], 4) if row else 0.0,
    }


# ---------------------------------------------------------------------------
# Strategy activity: El Chef / El Mecánico / Validated strategies
# ---------------------------------------------------------------------------

def _parse_log_tail(log_path: Path, lines: int = 50) -> list[str]:
    """Return the last *lines* lines of a log file, or empty list if missing."""
    if not log_path.exists():
        return []
    try:
        text = log_path.read_text(errors="replace")
        return text.splitlines()[-lines:]
    except OSError:
        return []


def get_el_chef_activity() -> dict:
    """Parse el_chef.log for last run info."""
    log_path = LOGS_DIR / "el_chef.log"
    if not log_path.exists():
        return {"last_run": None, "strategy": None, "backtest": None,
                "wr": None, "pf": None, "dd": None}

    try:
        text = log_path.read_text(errors="replace")
    except OSError:
        return {"last_run": None, "strategy": None, "backtest": None,
                "wr": None, "pf": None, "dd": None}

    lines = text.splitlines()

    # Use file mtime as fallback timestamp
    try:
        mtime = datetime.fromtimestamp(log_path.stat().st_mtime, tz=timezone.utc)
        file_ts = mtime.strftime("%Y-%m-%d %H:%M UTC")
    except OSError:
        file_ts = "unknown"

    last_run = None
    strategy = None
    backtest = None
    wr = pf = dd = None

    for line in reversed(lines):
        if "Starting strategy generation" in line:
            m = re.match(r"\[?(\d{4}-\d{2}-\d{2}[T ]\d{2}:\d{2})", line)
            if m:
                last_run = m.group(1)
            elif not last_run:
                last_run = file_ts
            break
        if "Generated:" in line and not strategy:
            m = re.search(r"Generated:\s*(\S+)", line)
            if m:
                strategy = m.group(1)
        if "Backtest failed" in line and backtest is None:
            backtest = "FAILED"
        if ("Backtest passed" in line or
                ("PASSED" in line.upper() and "backtest" in line.lower())) and backtest is None:
            backtest = "PASSED"
        if "WR" in line or "win_rate" in line.lower():
            m = re.search(r"(?:WR|win_rate)[=:\s]+(\d+\.?\d*)%?", line, re.IGNORECASE)
            if m and not wr:
                wr = m.group(1)
        if "PF" in line or "profit_factor" in line.lower():
            m = re.search(r"(?:PF|profit_factor)[=:\s]+(\d+\.?\d*)", line, re.IGNORECASE)
            if m and not pf:
                pf = m.group(1)
        if "DD" in line or "drawdown" in line.lower():
            m = re.search(r"(?:DD|drawdown|max_dd)[=:\s]+(\d+\.?\d*)%?", line, re.IGNORECASE)
            if m and not dd:
                dd = m.group(1)

    return {"last_run": last_run, "strategy": strategy, "backtest": backtest,
            "wr": wr, "pf": pf, "dd": dd}


def get_el_mecanico_activity() -> dict:
    """Parse el_mecanico.log for last run info."""
    log_path = LOGS_DIR / "el_mecanico.log"
    if not log_path.exists():
        return {"last_run": None, "strategy": None, "attempt": None,
                "max_attempts": 5, "result": None}
    try:
        tail = log_path.read_text(errors="replace").splitlines()
    except OSError:
        tail = []
    if not tail:
        return {"last_run": None, "strategy": None, "attempt": None,
                "max_attempts": 5, "result": None}

    last_run = None
    strategy = None
    attempt = None
    result = None

    for line in reversed(tail):
        if "Starting" in line or "starting" in line:
            m = re.match(r"\[?(\d{4}-\d{2}-\d{2}[T ]\d{2}:\d{2})", line)
            if m:
                last_run = m.group(1)
            elif not last_run:
                last_run = "see log"
            break
        if ("strategy" in line.lower() or "attempting" in line.lower()) and not strategy:
            m = re.search(r"(?:strategy|attempting)[=:\s]+(\S+)", line, re.IGNORECASE)
            if m:
                strategy = m.group(1)
        if "attempt" in line.lower() and attempt is None:
            m = re.search(r"attempt[=:\s]+(\d+)", line, re.IGNORECASE)
            if m:
                attempt = m.group(1)
        if "PASSED" in line.upper() and result is None:
            result = "PASSED"
        if "FAILED" in line.upper() and result is None:
            result = "FAILED"

    return {"last_run": last_run, "strategy": strategy, "attempt": attempt,
            "max_attempts": 5, "result": result}


def get_validated_strategies() -> dict:
    """List validated (non-candidate) strategy files and paper trade info."""
    validated = []
    if STRATEGIES_DIR.exists():
        for f in sorted(STRATEGIES_DIR.glob("*.py")):
            name = f.stem
            if name.startswith("candidate_") or name in ("__init__", "base"):
                continue
            validated.append(name)

    # Query paper trades grouped by strategy
    strategy_trades = {}
    if DB_PATH.exists():
        try:
            conn = sqlite3.connect(str(DB_PATH))
            rows = conn.execute("""
                SELECT strategy,
                       COUNT(*) as trade_count,
                       MIN(created_at) as first_trade
                FROM paper_trades
                GROUP BY strategy
            """).fetchall()
            conn.close()
            for row in rows:
                strategy_trades[row[0]] = {
                    "trade_count": row[1],
                    "first_trade": row[2],
                }
        except sqlite3.Error:
            pass

    return {"validated": validated, "strategy_trades": strategy_trades}


# ---------------------------------------------------------------------------
# Telegram message formats
# ---------------------------------------------------------------------------

def _temp_indicator(temp_c) -> str:
    """Return temperature status indicator."""
    if temp_c is None:
        return "N/A"
    if temp_c < 70:
        return f"{temp_c:.1f}°C ✓"
    elif temp_c <= 80:
        return f"{temp_c:.1f}°C ⚠️"
    else:
        return f"{temp_c:.1f}°C 🔴"


def _fmt(val, fmt_str=".2f", suffix="", prefix="$", na="N/A"):
    """Format a numeric value or return N/A."""
    if val is None:
        return na
    return f"{prefix}{val:{fmt_str}}{suffix}"


def format_telegram_message(findings: dict, claude_analysis: str, run_time: datetime,
                            hw_stats: dict, api_costs: dict, elec: dict,
                            mm_stats: dict,
                            chef: dict | None = None,
                            mecanico: dict | None = None,
                            strats: dict | None = None) -> str:
    """Format the daily summary as an HTML Telegram message."""
    date_str = run_time.strftime("%Y-%m-%d")
    time_str = run_time.strftime("%H:%M")

    audit_status = "PASSED" if findings["audit_passed"] else "FAILED"
    hash_status = findings["hash_status"]

    # Trade stats block
    ts = findings["trade_stats"]
    if ts["db_status"] == "NO_DATABASE":
        trade_block = "  No database — paper trading not started yet."
    elif ts["db_status"] != "OK":
        trade_block = f"  DB error: {ts['message']}"
    elif ts["total_trades"] == 0:
        trade_block = "  No completed trades yet."
    else:
        trade_block = (
            f"  Trades Today:    {ts['total_trades']}\n"
            f"  Actual WR:       {ts['actual_win_rate_pct']:.1f}% (predicted {BACKTEST_PREDICTED_WR}%)\n"
            f"  Market Maker:    {mm_stats['round_trips']} round trips"
        )

    # Strategy activity blocks
    chef = chef or {}
    mecanico = mecanico or {}
    strats = strats or {}

    chef_run = chef.get("last_run") or "not run yet"
    chef_strat = chef.get("strategy") or "none"
    chef_bt = chef.get("backtest") or "not run"
    chef_wr = f'{chef["wr"]}%' if chef.get("wr") else "N/A"
    chef_pf = chef.get("pf") or "N/A"
    chef_dd = f'{chef["dd"]}%' if chef.get("dd") else "N/A"

    mec_run = mecanico.get("last_run") or "not run yet"
    mec_strat = mecanico.get("strategy") or "none"
    mec_attempt = mecanico.get("attempt") or "N/A"
    mec_max = mecanico.get("max_attempts", 5)
    mec_result = mecanico.get("result") or "not run"

    validated = strats.get("validated", [])
    strategy_trades = strats.get("strategy_trades", {})
    in_paper = [v for v in validated if v in strategy_trades or v not in ("market_maker",)]
    strat_lines = ""
    for name in validated:
        info = strategy_trades.get(name)
        if info:
            first = info.get("first_trade", "")
            days = "?"
            if first:
                try:
                    first_dt = datetime.fromisoformat(first.replace("Z", "+00:00"))
                    days = (run_time - first_dt).days
                except (ValueError, TypeError):
                    pass
            strat_lines += f"\n  • {name}: Day {days}/14, {info['trade_count']} trades"
        else:
            strat_lines += f"\n  • {name}: awaiting first trade"

    strategy_block = (
        f"━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
        f"🍳 <b>EL CHEF</b>\n"
        f"  Last run:        {chef_run}\n"
        f"  Strategy:        {chef_strat}\n"
        f"  Backtest result: {chef_bt}\n"
        f"  WR: {chef_wr} | PF: {chef_pf} | DD: {chef_dd}\n"
        f"\n"
        f"━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
        f"🔧 <b>EL MECÁNICO</b>\n"
        f"  Last run:        {mec_run}\n"
        f"  Strategy:        {mec_strat}\n"
        f"  Attempt:         {mec_attempt}/{mec_max}\n"
        f"  Result:          {mec_result}\n"
        f"\n"
        f"━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
        f"📋 <b>VALIDATED STRATEGIES</b>\n"
        f"  Total validated: {len(validated)}\n"
        f"  In paper trading: {', '.join(validated) if validated else 'none'}"
        f"{strat_lines}\n"
    )

    # Hardware block
    temp_str = _temp_indicator(hw_stats.get("cpu_temp_c"))
    mem_str = f"{hw_stats['memory_pct']:.1f}%" if hw_stats.get("memory_pct") is not None else "N/A"
    db_str = f"{hw_stats['db_size_mb']:.1f}MB" if hw_stats.get("db_size_mb") is not None else "N/A"
    disk_str = f"{hw_stats['disk_usage_pct']:.1f}%" if hw_stats.get("disk_usage_pct") is not None else "N/A"

    # Cost block
    total_api = api_costs["total"]
    total_daily = total_api + elec["cost"]
    monthly_proj = total_daily * 30

    # Temperature alert
    temp_alert = ""
    cpu_temp = hw_stats.get("cpu_temp_c")
    if cpu_temp is not None and cpu_temp > 80:
        temp_alert = (
            f"\n🔴 TEMPERATURE ALERT: CPU at {cpu_temp:.1f}°C\n"
            f"Consider improving airflow or reducing load\n"
        )

    # Overall status
    status = "CLEAN" if (findings["audit_passed"] and "CLEAN" in claude_analysis) else "ACTION REQUIRED"

    msg = (
        f"🐕 <b>oneQuant Daily Audit</b> — {date_str}\n"
        f"\n"
        f"━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
        f"✅ <b>SYSTEM HEALTH</b>\n"
        f"  Engine Audit:    {audit_status}\n"
        f"  Engine Hash:     {hash_status}\n"
        f"\n"
        f"━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
        f"📊 <b>PAPER TRADING</b>\n"
        f"{trade_block}\n"
        f"\n"
        f"{strategy_block}"
        f"\n"
        f"━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
        f"🌡️ <b>HARDWARE</b>\n"
        f"  CPU Temp:        {temp_str}\n"
        f"  Memory:          {mem_str}\n"
        f"  DB Size:         {db_str}\n"
        f"  Disk Used:       {disk_str}\n"
        f"{temp_alert}"
        f"\n"
        f"━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
        f"⚡ <b>DAILY COSTS</b>\n"
        f"  El Chef API:     ${api_costs['el_chef']:.3f}\n"
        f"  El Mecánico:     ${api_costs['el_mecanico']:.3f}\n"
        f"  La Perra API:    ${api_costs['la_perra']:.3f}\n"
        f"  ─────────────────────────\n"
        f"  API Subtotal:    ${total_api:.3f}\n"
        f"  Electricity:     ${elec['cost']:.4f} ({elec['kwh']}kWh @ ${FPL_RATE})\n"
        f"  ─────────────────────────\n"
        f"  💰 TOTAL TODAY:  ${total_daily:.2f}\n"
        f"  📅 MONTHLY PROJ: ${monthly_proj:.2f}\n"
        f"\n"
        f"━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
        f"🤖 <b>AI ANALYSIS</b>\n"
        f"  {claude_analysis}\n"
        f"\n"
        f"━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
        f"Status: {status}\n"
        f"⏰ {time_str} UTC"
    )

    if len(msg) > 4000:
        msg = msg[:3990] + "\n<i>[truncated]</i>"
    return msg


def format_weekly_summary(summary: dict) -> str:
    """Format weekly cost summary for Telegram."""
    success_rate = 0.0
    if summary["candidates_generated"] > 0:
        success_rate = summary["candidates_passed"] / summary["candidates_generated"] * 100

    cost_per_pass = 0.0
    if summary["candidates_passed"] > 0:
        cost_per_pass = summary["total_cost"] / summary["candidates_passed"]

    # DB growth (approximate from weekly data)
    alerts = "None"

    msg = (
        f"📊 <b>oneQuant Weekly</b> — {summary['start_date']} to {summary['end_date']}\n"
        f"\n"
        f"💰 <b>WEEKLY COSTS</b>\n"
        f"  API:             ${summary['total_api_cost']:.2f}\n"
        f"  Electricity:     ${summary['total_electricity_cost']:.2f} ({summary['total_kwh']:.1f}kWh)\n"
        f"  ─────────────────────\n"
        f"  TOTAL:           ${summary['total_cost']:.2f}\n"
        f"  Daily avg:       ${summary['daily_avg_cost']:.2f}\n"
        f"\n"
        f"🧪 <b>STRATEGY PROGRESS</b>\n"
        f"  Generated:       {summary['candidates_generated']}\n"
        f"  Passed:          {summary['candidates_passed']}\n"
        f"  Success rate:    {success_rate:.1f}%\n"
        f"  Cost per pass:   ${cost_per_pass:.2f}\n"
        f"\n"
        f"🌡️ <b>HARDWARE TRENDS</b>\n"
        f"  Avg CPU Temp:    {summary['avg_cpu_temp']:.1f}°C\n"
        f"  Peak CPU Temp:   {summary['peak_cpu_temp']:.1f}°C\n"
        f"\n"
        f"⚠️ ALERTS: {alerts}"
    )
    return msg


def format_monthly_summary(summary: dict) -> str:
    """Format monthly cost summary for Telegram."""
    success_rate = 0.0
    if summary["candidates_generated"] > 0:
        success_rate = summary["candidates_passed"] / summary["candidates_generated"] * 100

    # Auto-generate recommendations
    recs = []
    if summary["peak_cpu_temp"] > 70:
        recs.append("• Consider improving cooling — peak temp exceeded 70°C")
    if summary["projected_annual"] > 300:
        recs.append("• Annual projection exceeds $300 — review API usage")
    if summary["cost_per_candidate"] > 5:
        recs.append("• High cost per passing strategy — tune generation parameters")
    if summary["projected_db_annual_mb"] > 2000:
        recs.append("• DB growth trending high — consider archiving old data")
    if not recs:
        recs.append("• All metrics within normal range ✓")

    msg = (
        f"📊 <b>oneQuant Monthly</b> — {summary['month_name']} {summary['year']}\n"
        f"\n"
        f"💰 <b>MONTHLY COSTS</b>\n"
        f"  API Total:       ${summary['total_api_cost']:.2f}\n"
        f"  ├ El Chef:       ${summary['el_chef_total']:.2f}\n"
        f"  ├ El Mecánico:   ${summary['el_mecanico_total']:.2f}\n"
        f"  └ La Perra:      ${summary['la_perra_total']:.2f}\n"
        f"  Electricity:     ${summary['total_electricity_cost']:.2f} ({summary['total_kwh']:.1f}kWh)\n"
        f"  ─────────────────────────\n"
        f"  TOTAL:           ${summary['total_cost']:.2f}\n"
        f"  Daily average:   ${summary['daily_avg_cost']:.2f}\n"
        f"  Annual proj:     ${summary['projected_annual']:.2f}\n"
        f"\n"
        f"🧪 <b>STRATEGY PIPELINE</b>\n"
        f"  Generated:       {summary['candidates_generated']}\n"
        f"  Passed backtest: {summary['candidates_passed']} ({success_rate:.1f}%)\n"
        f"  Cost per pass:   ${summary['cost_per_candidate']:.2f}\n"
        f"\n"
        f"🌡️ <b>HARDWARE HEALTH</b>\n"
        f"  Avg CPU Temp:    {summary['avg_cpu_temp']:.1f}°C\n"
        f"  Peak CPU Temp:   {summary['peak_cpu_temp']:.1f}°C\n"
        f"  DB Growth:       +{summary['db_growth_mb']:.1f}MB\n"
        f"  Projected/year:  {summary['projected_db_annual_mb']:.0f}MB\n"
        f"\n"
        f"💡 <b>RECOMMENDATIONS</b>\n"
        + "\n".join(recs)
    )
    return msg


# ---------------------------------------------------------------------------
# Telegram
# ---------------------------------------------------------------------------

def send_telegram(message: str) -> bool:
    """Send a Telegram message. Returns True on success."""
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        return False
    try:
        payload = {
            "chat_id": TELEGRAM_CHAT_ID,
            "text": message,
            "parse_mode": "HTML",
        }
        response = requests.post(TELEGRAM_API_URL, json=payload, timeout=15)
        response.raise_for_status()
        return True
    except requests.exceptions.RequestException as e:
        print(f"[la_perra] WARNING: Telegram send failed: {e}", file=sys.stderr)
        return False


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    run_time = datetime.now(timezone.utc)
    print(f"[la_perra] Starting daily audit at {run_time.strftime('%Y-%m-%d %H:%M')} UTC")

    # Check 1: Engine audit
    print("[la_perra] [1/4] Running engine audit...")
    audit_passed, audit_output = run_audit()
    status_str = "PASSED" if audit_passed else "FAILED"
    print(f"[la_perra] Audit: {status_str}")

    # Check 2: SHA256
    print("[la_perra] [2/4] Checking engine hash...")
    hash_match, actual_hash, expected_hash, hash_status = check_engine_hash()
    print(f"[la_perra] Hash: {hash_status}")
    if not hash_match:
        print(f"[la_perra] WARNING: engine.py hash mismatch!", file=sys.stderr)
        print(f"[la_perra]   Expected: {expected_hash}", file=sys.stderr)
        print(f"[la_perra]   Actual:   {actual_hash}", file=sys.stderr)

    # Check 3: Paper trades
    print("[la_perra] [3/4] Querying paper trades...")
    trade_stats = query_paper_trades()
    if trade_stats["db_status"] == "NO_DATABASE":
        print(f"[la_perra] DB: not found (paper trading not started)")
    elif trade_stats["db_status"] != "OK":
        print(f"[la_perra] DB: error — {trade_stats['message']}", file=sys.stderr)
    else:
        print(
            f"[la_perra] Trades: {trade_stats['total_trades']} "
            f"(WR actual={trade_stats['actual_win_rate_pct']:.1f}% "
            f"vs predicted={BACKTEST_PREDICTED_WR}% "
            f"divergence={trade_stats['divergence_pct']:+.1f}pp)"
        )

    # Check 4: Claude Haiku analysis
    print("[la_perra] [4/4] Calling Claude Haiku for analysis...")
    findings = {
        "audit_passed": audit_passed,
        "audit_output": audit_output,
        "hash_match": hash_match,
        "hash_status": hash_status,
        "actual_hash": actual_hash,
        "expected_hash": expected_hash,
        "trade_stats": trade_stats,
    }
    claude_analysis = call_claude(findings)
    print(f"[la_perra] Claude: {claude_analysis[:120]}...")

    # Collect hardware stats
    print("[la_perra] Collecting hardware stats...")
    hw_stats = get_hardware_stats()
    print(f"[la_perra] Hardware: temp={hw_stats['cpu_temp_c']}°C "
          f"mem={hw_stats['memory_pct']}% db={hw_stats['db_size_mb']}MB "
          f"disk={hw_stats['disk_usage_pct']}%")

    # Calculate costs
    api_costs = calculate_api_costs()
    elec = calculate_electricity_cost()
    print(f"[la_perra] Costs: API=${api_costs['total']:.3f} "
          f"Electricity=${elec['cost']:.4f} ({elec['kwh']}kWh)")

    # Market maker stats
    mm_stats = query_mm_today()

    # Strategy activity
    print("[la_perra] Collecting strategy activity...")
    chef = get_el_chef_activity()
    mecanico = get_el_mecanico_activity()
    strats = get_validated_strategies()
    print(f"[la_perra] Chef: {chef['strategy'] or 'none'} | "
          f"Mecánico: {mecanico['strategy'] or 'none'} | "
          f"Validated: {len(strats['validated'])}")

    # Save to database
    print("[la_perra] Saving daily costs to database...")
    save_daily_costs(hw_stats, api_costs, elec)
    print("[la_perra] Daily costs saved.")

    # Build daily message
    message = format_telegram_message(findings, claude_analysis, run_time,
                                      hw_stats, api_costs, elec, mm_stats,
                                      chef=chef, mecanico=mecanico, strats=strats)

    # Check if weekly summary needed (Saturday)
    if run_time.weekday() == 5:  # Saturday
        print("[la_perra] Saturday — appending weekly summary...")
        weekly = get_weekly_summary()
        message += "\n\n" + format_weekly_summary(weekly)

    # Check if monthly summary needed (last day of month)
    days_in_month = calendar.monthrange(run_time.year, run_time.month)[1]
    if run_time.day == days_in_month:
        print("[la_perra] Last day of month — appending monthly summary...")
        monthly = get_monthly_summary()
        message += "\n\n" + format_monthly_summary(monthly)

    # Truncate if needed (Telegram limit 4096)
    if len(message) > 4090:
        message = message[:4080] + "\n<i>[truncated]</i>"

    print(f"\n[la_perra] Summary:\n{message}\n")

    if TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID:
        ok = send_telegram(message)
        if ok:
            print("[la_perra] Telegram: sent successfully.")
        else:
            print("[la_perra] WARNING: Telegram send failed.", file=sys.stderr)
    else:
        print("[la_perra] Telegram: skipped (TELEGRAM_BOT_TOKEN/TELEGRAM_CHAT_ID not set).")

    sys.exit(0 if audit_passed else 1)


if __name__ == "__main__":
    main()
