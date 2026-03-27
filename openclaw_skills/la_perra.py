"""la_perra.py — Daily audit and monitoring script for oneQuant.

Runs 6 engine integrity tests, verifies SHA256 of engine.py, queries paper
trade win rate, calls Claude Haiku to analyze for suspicious patterns, and
sends a morning summary to Telegram.

All API keys loaded from onequant/.env only.
"""

import hashlib
import json
import os
import sqlite3
import subprocess
import sys
from datetime import datetime, timezone
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

HAIKU_MODEL = "claude-haiku-4-5-20251001"
TELEGRAM_API_URL = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"


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
# Telegram
# ---------------------------------------------------------------------------

def format_telegram_message(findings: dict, claude_analysis: str, run_time: datetime) -> str:
    """Format the morning summary as an HTML Telegram message."""
    date_str = run_time.strftime("%Y-%m-%d")
    time_str = run_time.strftime("%H:%M")

    audit_status = "PASSED" if findings["audit_passed"] else "FAILED"
    hash_status = findings["hash_status"]

    ts = findings["trade_stats"]
    if ts["db_status"] == "NO_DATABASE":
        trade_block = "  No database — paper trading not started yet."
    elif ts["db_status"] != "OK":
        trade_block = f"  DB error: {ts['message']}"
    elif ts["total_trades"] == 0:
        trade_block = "  No completed trades yet."
    else:
        div = ts["divergence_pct"]
        div_str = f"{div:+.1f}pp"
        if abs(div) > 20:
            div_str += " CRITICAL"
        elif abs(div) > 10:
            div_str += " WARNING"
        trade_block = (
            f"  Trades: {ts['total_trades']}  "
            f"(Wins: {ts['wins']} / Losses: {ts['losses']})\n"
            f"  Actual WR:    {ts['actual_win_rate_pct']:.1f}%\n"
            f"  Predicted WR: {ts['predicted_win_rate_pct']:.1f}%\n"
            f"  Divergence:   {div_str}"
        )

    msg = (
        f"<b>oneQuant Daily Audit</b> — {date_str}\n"
        f"\n"
        f"<b>Engine Audit:</b> {audit_status}\n"
        f"<b>Engine Hash:</b> {hash_status}\n"
        f"\n"
        f"<b>Paper Trading:</b>\n"
        f"{trade_block}\n"
        f"\n"
        f"<b>AI Analysis:</b>\n"
        f"{claude_analysis}\n"
        f"\n"
        f"<i>Run at {time_str} UTC</i>"
    )

    # Telegram message limit is 4096 chars
    if len(msg) > 4000:
        msg = msg[:3990] + "\n<i>[truncated]</i>"
    return msg


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

    # Format and send
    message = format_telegram_message(findings, claude_analysis, run_time)
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
