"""el_mecanico.py — AI-powered strategy fixer for oneQuant.

Reads REJECTED.md, finds near-miss strategies (WR 40–51%), and attempts
to fix them using Sonnet for diagnosis + Opus for implementation.

Rate limit: max 1 run per 20 hours (checked via onequant/.last_mecanico_run).
Max 5 fix attempts per strategy before marking as "unresolvable".
"""

import importlib.util
import json
import os
import re
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

# ---------------------------------------------------------------------------
# Path setup
# ---------------------------------------------------------------------------

REPO_ROOT = Path(__file__).resolve().parent.parent
ONEQUANT_DIR = REPO_ROOT / "onequant"

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

import anthropic  # noqa: E402
from backtest.engine import BacktestConfig, run_backtest  # noqa: E402
from backtest.metrics import calculate_metrics  # noqa: E402
from strategies.base import BaseStrategy as _BaseStrategy  # noqa: E402

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

RATE_LIMIT_FILE = ONEQUANT_DIR / ".last_mecanico_run"
ATTEMPTS_FILE = ONEQUANT_DIR / "logs" / "mecanico_attempts.json"
STRATEGIES_DIR = ONEQUANT_DIR / "strategies"
RESULTS_DIR = ONEQUANT_DIR / "results"
RATE_LIMIT_SECONDS = 20 * 3600  # 20 hours
MAX_ATTEMPTS = 5

# Near-miss thresholds
WR_NEAR_MISS_LOW = 40.0
WR_NEAR_MISS_HIGH = 51.0

# Pass thresholds
WR_MIN = 0.55
PF_MIN = 1.0
DD_MAX = 20.0

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------

LOG_DIR = ONEQUANT_DIR / "logs"


def log(msg: str) -> None:
    ts = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{ts}] [el_mecanico] {msg}")


# ---------------------------------------------------------------------------
# Rate limiting
# ---------------------------------------------------------------------------

def check_rate_limit() -> bool:
    if not RATE_LIMIT_FILE.exists():
        return True
    try:
        last_run = float(RATE_LIMIT_FILE.read_text().strip())
        elapsed = time.time() - last_run
        if elapsed < RATE_LIMIT_SECONDS:
            remaining_h = (RATE_LIMIT_SECONDS - elapsed) / 3600
            log(f"Rate limited: last run {elapsed/3600:.1f}h ago, next in {remaining_h:.1f}h")
            return False
        return True
    except (ValueError, OSError):
        return True


def write_rate_limit() -> None:
    RATE_LIMIT_FILE.write_text(str(time.time()))


# ---------------------------------------------------------------------------
# Attempt tracking
# ---------------------------------------------------------------------------

def load_attempts() -> dict:
    if ATTEMPTS_FILE.exists():
        try:
            return json.loads(ATTEMPTS_FILE.read_text())
        except (json.JSONDecodeError, OSError):
            return {}
    return {}


def save_attempts(attempts: dict) -> None:
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    ATTEMPTS_FILE.write_text(json.dumps(attempts, indent=2))


# ---------------------------------------------------------------------------
# REJECTED.md parser
# ---------------------------------------------------------------------------

def parse_near_misses(rejected_md: str) -> list[dict]:
    """Parse REJECTED.md and return strategies with WR between 40–51%.

    Returns list of dicts with keys: name, wr, pf, max_dd, trades, pnl,
    tested_date, section_text, file_ref.
    """
    # Split into sections by "## "
    sections = re.split(r'\n(?=## )', rejected_md)
    near_misses = []

    for section in sections:
        if not section.strip():
            continue

        # Extract strategy name
        name_match = re.search(r'^## (.+?)(?:\s*\(.*\))?\s*$', section, re.MULTILINE)
        if not name_match:
            continue
        name = name_match.group(1).strip()

        # Skip strategies already marked unresolvable
        if "unresolvable" in section.lower():
            continue

        # Extract WR from the table row (| Trades | WR | PF | ... format)
        # Look for data rows (not header rows)
        wr_match = re.search(
            r'\|\s*[\d,]+\s*\|\s*([\d.]+)%\s*\|\s*([\d.]+)\s*\|\s*-?([\d.]+)%\s*\|',
            section,
        )
        if not wr_match:
            continue

        wr = float(wr_match.group(1))
        pf = float(wr_match.group(2))
        max_dd = float(wr_match.group(3))

        if WR_NEAR_MISS_LOW <= wr <= WR_NEAR_MISS_HIGH:
            # Extract test date
            date_match = re.search(r'\*\*Tested:\*\*\s*(\d{4}-\d{2}-\d{2})', section)
            tested_date = date_match.group(1) if date_match else "unknown"

            # Extract file reference
            file_match = re.search(r'\*\*File:\*\*\s*`([^`]+)`', section)
            file_ref = file_match.group(1) if file_match else None

            near_misses.append({
                "name": name,
                "wr": wr,
                "pf": pf,
                "max_dd": max_dd,
                "tested_date": tested_date,
                "section_text": section.strip(),
                "file_ref": file_ref,
            })

    # Sort by tested_date descending (most recent first)
    near_misses.sort(key=lambda x: x["tested_date"], reverse=True)
    return near_misses


# ---------------------------------------------------------------------------
# Strategy code reader
# ---------------------------------------------------------------------------

def find_strategy_code(name: str, file_ref: str | None) -> str | None:
    """Try to find the strategy source code. Returns None if deleted."""
    if file_ref:
        # file_ref might be like "strategies/breakout.py" or "strategies/bb_reversion.py"
        path = ONEQUANT_DIR / file_ref
        if path.exists():
            return path.read_text(encoding="utf-8")

    # Try common name patterns
    slug = re.sub(r"[^a-z0-9]+", "_", name.lower()).strip("_")
    for pattern in [f"{slug}.py", f"candidate_{slug}.py"]:
        path = STRATEGIES_DIR / pattern
        if path.exists():
            return path.read_text(encoding="utf-8")

    return None


# ---------------------------------------------------------------------------
# Anthropic API calls
# ---------------------------------------------------------------------------

def call_sonnet_diagnose(near_miss: dict, strategy_code: str | None) -> str:
    """Call Sonnet to diagnose why the near-miss strategy failed and suggest a fix."""
    client = anthropic.Anthropic(api_key=os.environ["ANTHROPIC_API_KEY"])

    code_section = ""
    if strategy_code:
        code_section = f"""
STRATEGY SOURCE CODE:
---
{strategy_code}
---"""

    prompt = f"""You are a quant strategy debugger for a Bitcoin trading bot.

This strategy was REJECTED but is a near-miss — it has potential:

REJECTION REPORT:
---
{near_miss['section_text']}
---
{code_section}

WORKING REFERENCE — Mean Reversion Config A (WR 76.8%, PF 1.47):
- SELL only in BULL_TREND regime (200 EMA slope filter)
- RSI > 75 + 1.5% above 20 EMA + volume confirmed
- SL 6% / TP 4% — fires 2-5x/week

The near-miss has WR {near_miss['wr']}% and PF {near_miss['pf']}.

Analyze:
1. Root cause of failure (be specific)
2. ONE concrete modification to fix it (not a rewrite)
3. Expected WR improvement from this change
4. New SL/TP if needed

Be specific: name exact parameters, thresholds, or filters to change.
Adding a regime filter (BULL_TREND or BEAR_TREND) is often the fix."""

    response = client.messages.create(
        model="claude-sonnet-4-6",
        max_tokens=500,
        messages=[{"role": "user", "content": prompt}],
    )
    return response.content[0].text


def call_opus_implement(
    near_miss: dict,
    strategy_code: str | None,
    diagnosis: str,
    base_py_content: str,
) -> str:
    """Call Opus to implement the fix as a new candidate strategy file."""
    client = anthropic.Anthropic(api_key=os.environ["ANTHROPIC_API_KEY"])

    slug = slugify(near_miss['name'])
    class_stem = slug.title().replace('_', '')

    prompt = f"""Write a VERY SHORT Python strategy (<50 lines, <1200 tokens). Inline all math in generate_signal. No helpers, no comments, no docstrings, no blank lines between code.

FIX TO APPLY: {diagnosis[:300]}

EXACT structure (do not deviate):
from strategies.base import BaseStrategy, Signal

class Fixed{class_stem}Strategy(BaseStrategy):
    name: str = "fixed_{slug}"
    timeframe: str = "15m"
    required_candles: int = 230
    def generate_signal(self, candles: list[dict]) -> Signal:
        closes = [c["close"] for c in candles]
        volumes = [c["volume"] for c in candles]
        # ... compute indicators inline (RSI via Wilder, EMA via multiplier loop)
        # ... regime filter using 200 EMA slope
        # ... return Signal("BUY"|"SELL"|"SKIP", confidence, reason)

Only stdlib (math). ONLY raw Python code, zero markdown."""

    response = client.messages.create(
        model="claude-opus-4-6",
        max_tokens=1500,
        messages=[
            {"role": "user", "content": prompt},
        ],
    )
    code = response.content[0].text
    # Strip markdown fences if present
    code = re.sub(r'^```(?:python)?\s*\n', '', code)
    code = re.sub(r'\n```\s*$', '', code)
    return code, response.stop_reason


# ---------------------------------------------------------------------------
# Code helpers (reused from el_chef pattern)
# ---------------------------------------------------------------------------

def extract_class_name(code: str) -> str:
    match = re.search(r"^class\s+(\w+Strategy)\s*\(BaseStrategy\)", code, re.MULTILINE)
    if match:
        return match.group(1)
    raise ValueError("Could not find a class inheriting BaseStrategy")


def extract_strategy_name_attr(code: str) -> str:
    match = re.search(r'name\s*:\s*str\s*=\s*["\']([^"\']+)["\']', code)
    if match:
        return match.group(1)
    try:
        class_name = extract_class_name(code)
        return class_name.replace("Strategy", "").strip()
    except ValueError:
        return "unknown_fixed"


def slugify(name: str) -> str:
    slug = name.lower().strip()
    slug = re.sub(r"[^a-z0-9]+", "_", slug)
    return slug.strip("_")


def save_strategy(code: str, slug: str) -> Path:
    file_path = STRATEGIES_DIR / f"candidate_fixed_{slug}.py"
    file_path.write_text(code, encoding="utf-8")
    return file_path


def dynamic_import_strategy(file_path: Path, class_name: str) -> type:
    spec = importlib.util.spec_from_file_location("candidate_fixed_strategy", file_path)
    module = importlib.util.module_from_spec(spec)
    sys.modules["candidate_fixed_strategy"] = module
    spec.loader.exec_module(module)
    StrategyClass = getattr(module, class_name)
    if not (isinstance(StrategyClass, type) and issubclass(StrategyClass, _BaseStrategy)):
        raise TypeError(f"{class_name} does not subclass BaseStrategy")
    return StrategyClass


# ---------------------------------------------------------------------------
# Backtest and evaluation
# ---------------------------------------------------------------------------

def run_candidate_backtest(StrategyClass: type):
    instance = StrategyClass()
    cfg = BacktestConfig(
        strategy=instance,
        timeframe="15m",
        initial_capital=250.0,
        stop_loss_pct=0.06,
        take_profit_pct=0.04,
        min_confidence=0.55,
        order_type="limit",
        symbol="BTCUSD",
    )
    result = run_backtest(cfg)
    metrics = calculate_metrics(result)
    return metrics, result


def evaluate_metrics(metrics) -> tuple[bool, str]:
    failures = []
    wr_pct = metrics.win_rate * 100.0
    if metrics.win_rate < WR_MIN:
        failures.append(f"WR {wr_pct:.1f}% < {WR_MIN*100:.0f}%")
    if metrics.profit_factor < PF_MIN:
        failures.append(f"PF {metrics.profit_factor:.3f} < {PF_MIN}")
    if metrics.max_drawdown > DD_MAX:
        failures.append(f"MaxDD {metrics.max_drawdown:.1f}% > {DD_MAX}%")
    if failures:
        return False, "; ".join(failures)
    return True, "PASS"


def log_result(slug: str, strategy_name: str, metrics, passed: bool, reason: str) -> None:
    RESULTS_DIR.mkdir(parents=True, exist_ok=True)
    ts = int(time.time())
    log_path = RESULTS_DIR / f"mecanico_{slug}_{ts}.json"
    data = {
        "strategy_name": strategy_name,
        "slug": slug,
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "source": "el_mecanico",
        "verdict": "PASS" if passed else "FAIL",
        "win_rate_pct": round(metrics.win_rate * 100.0, 2),
        "profit_factor": round(metrics.profit_factor, 3),
        "max_drawdown_pct": round(metrics.max_drawdown, 2),
        "total_trades": metrics.total_trades,
        "total_pnl": round(metrics.total_pnl, 2),
        "failure_reason": None if passed else reason,
    }
    try:
        log_path.write_text(json.dumps(data, indent=2))
        log(f"Result logged → {log_path.name}")
    except OSError as e:
        log(f"WARNING: could not write result log: {e}")


def mark_unresolvable(near_miss_name: str) -> None:
    """Append 'UNRESOLVABLE' note to the strategy's section in REJECTED.md."""
    rejected_path = STRATEGIES_DIR / "REJECTED.md"
    if not rejected_path.exists():
        return
    content = rejected_path.read_text(encoding="utf-8")
    marker = f"## {near_miss_name}"
    if marker in content and "UNRESOLVABLE" not in content.split(marker)[1].split("\n---")[0]:
        note = (
            f"\n\n**UNRESOLVABLE** — El Mecánico attempted {MAX_ATTEMPTS} fixes, "
            f"none passed thresholds. No further attempts.\n"
        )
        # Insert note right after the section header line
        idx = content.index(marker)
        # Find the next "---" section divider or end of file
        rest = content[idx:]
        next_divider = rest.find("\n---\n")
        if next_divider == -1:
            content = content + note
        else:
            insert_at = idx + next_divider
            content = content[:insert_at] + note + content[insert_at:]
        rejected_path.write_text(content, encoding="utf-8")
        log(f"Marked '{near_miss_name}' as UNRESOLVABLE in REJECTED.md")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    log("Starting strategy fix pipeline...")

    # 1. Rate limit
    if not check_rate_limit():
        sys.exit(0)

    # 2. Read REJECTED.md
    rejected_path = STRATEGIES_DIR / "REJECTED.md"
    if not rejected_path.exists():
        log("No REJECTED.md found — nothing to fix")
        sys.exit(0)

    rejected_md = rejected_path.read_text(encoding="utf-8")

    # 3. Find near-misses (WR 40–51%)
    near_misses = parse_near_misses(rejected_md)
    if not near_misses:
        log("No near-miss strategies found (WR 40-51%) — nothing to fix")
        sys.exit(0)

    log(f"Found {len(near_misses)} near-miss strategies:")
    for nm in near_misses:
        log(f"  - {nm['name']} (WR {nm['wr']}%, PF {nm['pf']}, tested {nm['tested_date']})")

    # 4. Check attempt counts
    attempts = load_attempts()
    target = None
    for nm in near_misses:
        key = slugify(nm["name"])
        count = attempts.get(key, {}).get("count", 0)
        if count >= MAX_ATTEMPTS:
            log(f"  Skipping {nm['name']}: {count}/{MAX_ATTEMPTS} attempts exhausted")
            continue
        target = nm
        break

    if target is None:
        log("All near-miss strategies have exhausted fix attempts — nothing to do")
        sys.exit(0)

    target_slug = slugify(target["name"])
    attempt_num = attempts.get(target_slug, {}).get("count", 0) + 1
    log(f"Targeting: {target['name']} (attempt {attempt_num}/{MAX_ATTEMPTS})")

    # 5. Find original strategy code (may be deleted)
    strategy_code = find_strategy_code(target["name"], target.get("file_ref"))
    if strategy_code:
        log(f"Found original source code ({len(strategy_code)} chars)")
    else:
        log("Original source code not found (deleted) — working from rejection report only")

    # 6. Read base.py
    base_py_content = (STRATEGIES_DIR / "base.py").read_text(encoding="utf-8")

    # 7. Call Sonnet for diagnosis
    log("Calling claude-sonnet-4-6 for diagnosis...")
    diagnosis = call_sonnet_diagnose(target, strategy_code)
    log(f"Diagnosis received ({len(diagnosis)} chars)")

    # 8. Call Opus for implementation
    log("Calling claude-opus-4-6 for implementation...")
    code, stop_reason = call_opus_implement(target, strategy_code, diagnosis, base_py_content)
    log(f"Opus response: {len(code)} chars, stop_reason={stop_reason}")

    # If truncated, try to salvage by closing any open function
    if stop_reason == "max_tokens":
        log("Response truncated — attempting to salvage code")
        lines = code.rstrip().split("\n")
        # Remove last incomplete line
        if lines and not lines[-1].rstrip().endswith((":", ")", "'", '"', ",")):
            pass  # last line looks complete enough
        else:
            while lines and lines[-1].strip() == "":
                lines.pop()
        # Ensure there's a return Signal at the end of generate_signal
        # Find indentation from the method body
        indent = "        "
        for ln in lines:
            stripped = ln.lstrip()
            if stripped.startswith("closes") or stripped.startswith("volumes"):
                indent = ln[:len(ln) - len(stripped)]
                break
        lines.append(f'{indent}return Signal("SKIP", 0.0, "truncated fallback")')
        code = "\n".join(lines)

    # 9. Write rate limit after API calls succeed
    write_rate_limit()

    # 10. Strip markdown fences if present
    code = re.sub(r'```(?:python)?\s*\n?', '', code)
    code = code.strip()

    # Debug: dump first 300 chars if class not found initially
    if not re.search(r'class\s+\w+Strategy\s*\(BaseStrategy\)', code, re.MULTILINE):
        log(f"DEBUG: Code starts with: {repr(code[:300])}")

    # Parse and save
    try:
        class_name = extract_class_name(code)
    except ValueError as e:
        log(f"ERROR: {e}")
        attempts.setdefault(target_slug, {"count": 0, "history": []})
        attempts[target_slug]["count"] = attempt_num
        attempts[target_slug]["history"].append({
            "attempt": attempt_num,
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "error": str(e),
        })
        save_attempts(attempts)
        sys.exit(1)

    strategy_name = extract_strategy_name_attr(code)
    slug = slugify(strategy_name)
    file_path = save_strategy(code, slug)
    log(f"Strategy saved → {file_path}")

    # 11. Import
    try:
        StrategyClass = dynamic_import_strategy(file_path, class_name)
    except (SyntaxError, ImportError, AttributeError, TypeError) as e:
        log(f"ERROR: Import failed: {e}")
        file_path.unlink(missing_ok=True)
        attempts.setdefault(target_slug, {"count": 0, "history": []})
        attempts[target_slug]["count"] = attempt_num
        attempts[target_slug]["history"].append({
            "attempt": attempt_num,
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "error": f"ImportError: {e}",
        })
        save_attempts(attempts)
        sys.exit(1)

    # 12. Run backtest
    log("Running backtest...")
    try:
        metrics, result = run_candidate_backtest(StrategyClass)
    except Exception as e:
        log(f"ERROR: Backtest failed: {e}")
        file_path.unlink(missing_ok=True)
        attempts.setdefault(target_slug, {"count": 0, "history": []})
        attempts[target_slug]["count"] = attempt_num
        attempts[target_slug]["history"].append({
            "attempt": attempt_num,
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "error": f"BacktestError: {e}",
        })
        save_attempts(attempts)
        sys.exit(1)

    # 13. Evaluate
    passed, reason = evaluate_metrics(metrics)
    wr_pct = metrics.win_rate * 100.0
    log(f"Results: WR={wr_pct:.1f}% PF={metrics.profit_factor:.3f} "
        f"DD={metrics.max_drawdown:.1f}% Trades={metrics.total_trades}")
    log_result(slug, strategy_name, metrics, passed, reason)

    # 14. Track attempt
    attempts.setdefault(target_slug, {"count": 0, "history": []})
    attempts[target_slug]["count"] = attempt_num
    attempts[target_slug]["history"].append({
        "attempt": attempt_num,
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "result": "PASS" if passed else "FAIL",
        "wr": round(wr_pct, 1),
        "pf": round(metrics.profit_factor, 3),
        "max_dd": round(metrics.max_drawdown, 1),
        "trades": metrics.total_trades,
        "reason": reason,
    })
    save_attempts(attempts)

    if not passed:
        log(f"FAILED: {reason}")
        file_path.unlink(missing_ok=True)
        log("Strategy file deleted.")
        if attempt_num >= MAX_ATTEMPTS:
            mark_unresolvable(target["name"])
        sys.exit(0)

    # 15. Passed!
    log(f"PASSED! Strategy fixed: {strategy_name}")
    log(f"File: {file_path}")
    log("Running validation suite...")
    import subprocess
    proc = subprocess.run(
        [sys.executable, "validate.py"],
        cwd=str(ONEQUANT_DIR),
        check=False,
    )
    if proc.returncode == 0:
        log(f"Strategy VALIDATED: {file_path}")
    else:
        log(f"Passed backtest but FAILED validation (exit {proc.returncode})")
        log(f"File retained for manual review: {file_path}")


if __name__ == "__main__":
    main()
