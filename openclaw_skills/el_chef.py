"""el_chef.py — AI-powered strategy generator for oneQuant.

Uses claude-sonnet-4-6 to propose a new strategy concept, then claude-opus-4-6
to write the full strategy implementation, then backtests it and filters by
WR >= 55%, PF >= 1.0, MaxDD <= 20%.

Rate limit: max 1 run per 20 hours (checked via onequant/.last_chef_run).
All API keys loaded from onequant/.env only.
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
# Path setup — must happen before any onequant imports
# ---------------------------------------------------------------------------

REPO_ROOT = Path(__file__).resolve().parent.parent
ONEQUANT_DIR = REPO_ROOT / "onequant"

# Load .env BEFORE sys.path manipulation (config.py exits if Coinbase vars missing)
try:
    from dotenv import load_dotenv
    load_dotenv(ONEQUANT_DIR / ".env")
except ImportError:
    # Fall back to manual .env parse if python-dotenv not installed
    env_path = ONEQUANT_DIR / ".env"
    if env_path.exists():
        for line in env_path.read_text().splitlines():
            line = line.strip()
            if line and not line.startswith("#") and "=" in line:
                k, _, v = line.partition("=")
                os.environ.setdefault(k.strip(), v.strip())

sys.path.insert(0, str(ONEQUANT_DIR))

import anthropic  # noqa: E402  (after path setup)
from backtest.engine import BacktestConfig, run_backtest  # noqa: E402
from backtest.metrics import calculate_metrics  # noqa: E402
from strategies.base import BaseStrategy as _BaseStrategy  # noqa: E402

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

RATE_LIMIT_FILE = ONEQUANT_DIR / ".last_chef_run"
RESULTS_DIR = ONEQUANT_DIR / "results"
STRATEGIES_DIR = ONEQUANT_DIR / "strategies"
RATE_LIMIT_SECONDS = 20 * 3600  # 20 hours

# Thresholds — using raw Metrics field units:
#   win_rate is 0.0–1.0, max_drawdown is positive percentage (e.g. 2.5 = 2.5%)
WR_MIN = 0.55
PF_MIN = 1.0
DD_MAX = 20.0


# ---------------------------------------------------------------------------
# Rate limiting
# ---------------------------------------------------------------------------

def check_rate_limit() -> bool:
    """Return True if allowed to run, False if rate-limited."""
    if not RATE_LIMIT_FILE.exists():
        return True
    try:
        last_run = float(RATE_LIMIT_FILE.read_text().strip())
        elapsed = time.time() - last_run
        if elapsed < RATE_LIMIT_SECONDS:
            remaining_h = (RATE_LIMIT_SECONDS - elapsed) / 3600
            print(f"[el_chef] Rate limit: last run was {elapsed/3600:.1f}h ago. "
                  f"Next run allowed in {remaining_h:.1f}h.")
            return False
        return True
    except (ValueError, OSError):
        return True  # Treat unreadable file as "never run"


def write_rate_limit() -> None:
    """Record current timestamp as the last run time."""
    RATE_LIMIT_FILE.write_text(str(time.time()))


# ---------------------------------------------------------------------------
# Context readers
# ---------------------------------------------------------------------------

def read_results_context() -> str:
    """Read recent candidate results from results/ and return a summary string."""
    RESULTS_DIR.mkdir(parents=True, exist_ok=True)
    json_files = sorted(RESULTS_DIR.glob("*.json"))[-10:]  # last 10
    if not json_files:
        return "No previous candidate results on file."

    lines = []
    for f in json_files:
        try:
            data = json.loads(f.read_text())
            lines.append(
                f"- {data.get('strategy_name', '?')} → {data.get('verdict', '?')} "
                f"(WR={data.get('win_rate_pct', 0):.1f}%, "
                f"PF={data.get('profit_factor', 0):.3f}, "
                f"DD={data.get('max_drawdown_pct', 0):.1f}%) "
                f"Reason: {data.get('failure_reason', 'N/A')}"
            )
        except (json.JSONDecodeError, OSError):
            continue

    return "\n".join(lines) if lines else "No readable previous results."


# ---------------------------------------------------------------------------
# Anthropic API calls
# ---------------------------------------------------------------------------

def call_sonnet_for_concept(rejected_md: str, results_context: str) -> str:
    """Call claude-sonnet-4-6 to propose ONE new strategy concept (text only)."""
    client = anthropic.Anthropic(api_key=os.environ["ANTHROPIC_API_KEY"])

    prompt = f"""You are a quant strategy researcher for a Bitcoin trading bot.

REJECTED STRATEGIES (do not repeat or closely resemble these):
---
{rejected_md}
---

PREVIOUS CANDIDATE RESULTS (also do not repeat strategies that already failed):
---
{results_context}
---

Suggest ONE new trading strategy concept for 15-minute BTC-USD candles.

Requirements:
- Must have positive expected value BEFORE costs: (WR × TP%) − ((1−WR) × SL%) > 0.55% fee drag
- Must be implementable using only: timestamp, open, high, low, close, volume per candle
- No external data sources (no news, no fear/greed index, no on-chain data)
- Must be distinct from: Donchian Breakout, VWAP Momentum, Bollinger Band Reversion,
  RSI+EMA Mean Reversion, RSI+MACD Momentum, and any strategy listed above

In your response describe:
1. The core signal logic (entry condition)
2. Regime filter (if any)
3. Expected win rate and mathematical justification
4. Suggested SL% and TP% and the EV calculation showing positive expectancy
5. Why this specific market microstructure edge exists in 15m BTC-USD data

Respond with text only — no Python code, no markdown code blocks."""

    response = client.messages.create(
        model="claude-sonnet-4-6",
        max_tokens=1024,
        messages=[{"role": "user", "content": prompt}],
    )
    return response.content[0].text


def call_opus_for_code(concept: str, base_py_content: str) -> str:
    """Call claude-opus-4-6 to write a complete strategy Python file."""
    client = anthropic.Anthropic(api_key=os.environ["ANTHROPIC_API_KEY"])

    prompt = f"""You are writing a Python trading strategy for a crypto bot.

BASE CLASS (inherit from this exactly — copy the import path verbatim):
---
{base_py_content}
---

STRATEGY CONCEPT TO IMPLEMENT:
---
{concept}
---

Write a complete, importable Python file for this strategy.

STRICT REQUIREMENTS:
1. First line of imports must be exactly: from strategies.base import BaseStrategy, Signal
2. Define exactly ONE class that inherits BaseStrategy
3. The class MUST have these class-level annotations:
   name: str = "Your Strategy Name Here"   # unique, descriptive, max 50 chars
   timeframe: str = "15m"
   required_candles: int = <N>             # minimum candles needed, at least 22
4. Implement generate_signal(self, candles: list[dict]) -> Signal
   - candles: list of dicts with keys timestamp, open, high, low, close, volume
   - ordered oldest-first; length is guaranteed to equal required_candles
   - Return Signal("BUY", confidence, reason), Signal("SELL", confidence, reason),
     or Signal("SKIP", 0.0, reason)
   - confidence is a float 0.0–1.0 scaled by signal quality
5. All helper functions must be standalone module-level functions, NOT methods
6. Only Python stdlib allowed: math, statistics, collections — NO external imports
7. No database access, no API calls, no file I/O, no print statements
8. Class name must be CamelCase ending in "Strategy"
9. Module-level constants for all tunable parameters (RSI periods, thresholds, etc.)

Return ONLY the Python source code. No markdown fences. No explanation text."""

    response = client.messages.create(
        model="claude-opus-4-6",
        max_tokens=4096,
        messages=[{"role": "user", "content": prompt}],
    )
    return response.content[0].text


# ---------------------------------------------------------------------------
# Code parsing helpers
# ---------------------------------------------------------------------------

def extract_class_name(code: str) -> str:
    """Extract the strategy class name from generated code."""
    match = re.search(r"^class\s+(\w+Strategy)\s*\(BaseStrategy\)", code, re.MULTILINE)
    if match:
        return match.group(1)
    raise ValueError(
        "Could not find a class inheriting BaseStrategy in generated code. "
        "Expected pattern: 'class FooStrategy(BaseStrategy):'"
    )


def extract_strategy_name_attr(code: str) -> str:
    """Extract the strategy's name attribute string value."""
    match = re.search(r'name\s*:\s*str\s*=\s*["\']([^"\']+)["\']', code)
    if match:
        return match.group(1)
    # Fallback: derive from class name
    try:
        class_name = extract_class_name(code)
        return class_name.replace("Strategy", "").strip()
    except ValueError:
        return "unknown_strategy"


def slugify(name: str) -> str:
    """Convert a strategy name to a safe filename slug."""
    slug = name.lower().strip()
    slug = re.sub(r"[^a-z0-9]+", "_", slug)
    return slug.strip("_")


# ---------------------------------------------------------------------------
# Strategy file management
# ---------------------------------------------------------------------------

def save_strategy(code: str, slug: str) -> Path:
    """Write the generated strategy code to strategies/candidate_{slug}.py."""
    file_path = STRATEGIES_DIR / f"candidate_{slug}.py"
    file_path.write_text(code, encoding="utf-8")
    return file_path


def dynamic_import_strategy(file_path: Path, class_name: str) -> type:
    """Import the strategy class from the generated file."""
    spec = importlib.util.spec_from_file_location("candidate_strategy", file_path)
    module = importlib.util.module_from_spec(spec)
    sys.modules["candidate_strategy"] = module
    spec.loader.exec_module(module)

    StrategyClass = getattr(module, class_name)
    if not (isinstance(StrategyClass, type) and issubclass(StrategyClass, _BaseStrategy)):
        raise TypeError(
            f"{class_name} does not subclass the real BaseStrategy from strategies.base"
        )
    return StrategyClass


# ---------------------------------------------------------------------------
# Backtest and evaluation
# ---------------------------------------------------------------------------

def run_candidate_backtest(StrategyClass: type):
    """Run backtest on the candidate strategy and return (Metrics, BacktestResult)."""
    instance = StrategyClass()
    cfg = BacktestConfig(
        strategy=instance,
        timeframe="15m",
        initial_capital=250.0,
        stop_loss_pct=0.06,
        take_profit_pct=0.04,
        min_confidence=0.55,
        order_type="limit",
    )
    result = run_backtest(cfg)
    metrics = calculate_metrics(result)
    return metrics, result


def evaluate_metrics(metrics) -> tuple[bool, str]:
    """Return (passed, reason). Checks WR, PF, and MaxDD thresholds."""
    failures = []
    wr_pct = metrics.win_rate * 100.0
    if metrics.win_rate < WR_MIN:
        failures.append(f"WR {wr_pct:.1f}% < {WR_MIN*100:.0f}% threshold")
    if metrics.profit_factor < PF_MIN:
        failures.append(f"PF {metrics.profit_factor:.3f} < {PF_MIN} threshold")
    if metrics.max_drawdown > DD_MAX:
        failures.append(f"MaxDD {metrics.max_drawdown:.1f}% exceeds {DD_MAX}% limit")
    if failures:
        return False, "; ".join(failures)
    return True, "PASS"


def log_result(
    slug: str,
    strategy_name: str,
    metrics,
    passed: bool,
    reason: str,
) -> None:
    """Write a JSON result log to results/{slug}_{ts}.json."""
    RESULTS_DIR.mkdir(parents=True, exist_ok=True)
    ts = int(time.time())
    log_path = RESULTS_DIR / f"{slug}_{ts}.json"
    data = {
        "strategy_name": strategy_name,
        "slug": slug,
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "verdict": "PASS" if passed else "FAIL",
        "win_rate_pct": round(metrics.win_rate * 100.0, 2),
        "profit_factor": round(metrics.profit_factor, 3),
        "max_drawdown_pct": round(metrics.max_drawdown, 2),
        "total_trades": metrics.total_trades,
        "total_pnl": round(metrics.total_pnl, 2),
        "failure_reason": None if passed else reason,
    }
    try:
        log_path.write_text(json.dumps(data, indent=2), encoding="utf-8")
        print(f"[el_chef] Result logged → {log_path.name}")
    except OSError as e:
        print(f"[el_chef] WARNING: could not write result log: {e}", file=sys.stderr)


def log_error(slug: str, strategy_name: str, error: str) -> None:
    """Write an ERROR log for import/syntax failures."""
    RESULTS_DIR.mkdir(parents=True, exist_ok=True)
    ts = int(time.time())
    log_path = RESULTS_DIR / f"{slug}_{ts}.json"
    data = {
        "strategy_name": strategy_name,
        "slug": slug,
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "verdict": "ERROR",
        "win_rate_pct": 0.0,
        "profit_factor": 0.0,
        "max_drawdown_pct": 0.0,
        "total_trades": 0,
        "total_pnl": 0.0,
        "failure_reason": error,
    }
    try:
        log_path.write_text(json.dumps(data, indent=2), encoding="utf-8")
    except OSError:
        pass


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    print("[el_chef] Starting strategy generation pipeline...")

    # 1. Rate limit check
    if not check_rate_limit():
        sys.exit(0)

    # 2. Ensure results dir exists
    RESULTS_DIR.mkdir(parents=True, exist_ok=True)

    # 3. Read context files
    rejected_md_path = STRATEGIES_DIR / "REJECTED.md"
    if rejected_md_path.exists():
        rejected_md = rejected_md_path.read_text(encoding="utf-8")
    else:
        rejected_md = "No rejected strategies documented yet."

    base_py_content = (STRATEGIES_DIR / "base.py").read_text(encoding="utf-8")
    results_context = read_results_context()

    # 4. First API call: Sonnet proposes concept
    print("[el_chef] Calling claude-sonnet-4-6 for strategy concept...")
    concept = call_sonnet_for_concept(rejected_md, results_context)
    print(f"\n[el_chef] Concept received:\n{concept}\n")

    # 5. Second API call: Opus writes the code
    print("[el_chef] Calling claude-opus-4-6 to write strategy code...")
    code = call_opus_for_code(concept, base_py_content)

    # 6. Write rate limit NOW (after both API calls succeed, before risky ops)
    write_rate_limit()
    print("[el_chef] Rate limit updated.")

    # 7. Parse class and strategy name
    try:
        class_name = extract_class_name(code)
    except ValueError as e:
        print(f"[el_chef] ERROR: {e}", file=sys.stderr)
        log_error("unknown", "unknown", str(e))
        sys.exit(1)

    strategy_name = extract_strategy_name_attr(code)
    slug = slugify(strategy_name)
    print(f"[el_chef] Generated: {class_name} (slug: {slug})")

    # 8. Save strategy file
    file_path = save_strategy(code, slug)
    print(f"[el_chef] Strategy saved → {file_path}")

    # 9. Dynamic import and validation
    try:
        StrategyClass = dynamic_import_strategy(file_path, class_name)
    except (SyntaxError, ImportError, AttributeError, TypeError) as e:
        print(f"[el_chef] ERROR: Failed to import generated strategy: {e}", file=sys.stderr)
        file_path.unlink(missing_ok=True)
        log_error(slug, strategy_name, f"{type(e).__name__}: {e}")
        sys.exit(1)

    # 10. Run backtest
    print("[el_chef] Running backtest...")
    try:
        metrics, result = run_candidate_backtest(StrategyClass)
    except Exception as e:
        print(f"[el_chef] ERROR: Backtest failed: {e}", file=sys.stderr)
        file_path.unlink(missing_ok=True)
        log_error(slug, strategy_name, f"BacktestError: {e}")
        sys.exit(1)

    # 11. Evaluate
    passed, reason = evaluate_metrics(metrics)
    wr_pct = metrics.win_rate * 100.0
    print(
        f"[el_chef] Results: WR={wr_pct:.1f}% PF={metrics.profit_factor:.3f} "
        f"DD={metrics.max_drawdown:.1f}% Trades={metrics.total_trades}"
    )
    log_result(slug, strategy_name, metrics, passed, reason)

    if not passed:
        print(f"[el_chef] REJECTED: {reason}")
        file_path.unlink(missing_ok=True)
        print(f"[el_chef] Strategy file deleted.")
        sys.exit(0)

    # 12. Passed — run validate.py
    print(f"\n[el_chef] PASSED thresholds! Running validation suite...")
    import subprocess
    proc = subprocess.run(
        [sys.executable, "validate.py"],
        cwd=str(ONEQUANT_DIR),
        check=False,
    )
    if proc.returncode == 0:
        print(f"\n[el_chef] Strategy VALIDATED: {file_path}")
    else:
        print(f"\n[el_chef] Strategy passed backtest but FAILED validation (exit {proc.returncode})")
        print(f"[el_chef] File retained for manual review: {file_path}")


if __name__ == "__main__":
    main()
