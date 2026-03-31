"""Three-level circuit breaker system.

Daily: 5% loss limit
Weekly: 10% loss limit
Per-strategy: 5 consecutive losses OR WR drops 15% below backtest
"""

import json
import logging
from datetime import datetime, timedelta
from pathlib import Path

from safety.kill_switch import activate_kill_switch

logger = logging.getLogger(__name__)

CIRCUIT_BREAKER_STATE_FILE = Path(
    "/root/oneQuant/onequant/logs/circuit_breaker_state.json"
)

DAILY_LOSS_LIMIT = 0.05      # 5%
WEEKLY_LOSS_LIMIT = 0.10     # 10%
CONSECUTIVE_LOSS_LIMIT = 5   # 5 in a row
WR_DIVERGENCE_LIMIT = 0.15   # 15% below backtest


class CircuitBreaker:
    def __init__(self, account_balance: float, telegram_func=None):
        self.account_balance = account_balance
        self.telegram_func = telegram_func
        self.state = self._load_state()

    def _load_state(self) -> dict:
        if CIRCUIT_BREAKER_STATE_FILE.exists():
            return json.loads(CIRCUIT_BREAKER_STATE_FILE.read_text())
        return {
            "daily_pnl": 0.0,
            "weekly_pnl": 0.0,
            "daily_reset": datetime.utcnow().date().isoformat(),
            "weekly_reset": datetime.utcnow().date().isoformat(),
            "daily_breaker_active": False,
            "weekly_breaker_active": False,
            "strategy_states": {},
        }

    def _save_state(self) -> None:
        CIRCUIT_BREAKER_STATE_FILE.parent.mkdir(parents=True, exist_ok=True)
        CIRCUIT_BREAKER_STATE_FILE.write_text(json.dumps(self.state, indent=2))

    def _reset_if_needed(self) -> None:
        today = datetime.utcnow().date().isoformat()

        # Reset daily at midnight UTC
        if self.state["daily_reset"] != today:
            self.state["daily_pnl"] = 0.0
            self.state["daily_reset"] = today
            self.state["daily_breaker_active"] = False
            logger.info("Daily circuit breaker reset")

        # Reset weekly on Monday
        days_since_monday = datetime.utcnow().weekday()
        last_monday = (
            datetime.utcnow() - timedelta(days=days_since_monday)
        ).date().isoformat()
        if self.state["weekly_reset"] != last_monday:
            self.state["weekly_pnl"] = 0.0
            self.state["weekly_reset"] = last_monday
            self.state["weekly_breaker_active"] = False
            logger.info("Weekly circuit breaker reset")

    def record_trade_pnl(self, strategy_name: str, pnl_usd: float, won: bool) -> None:
        """Call this after every trade closes."""
        self._reset_if_needed()

        self.state["daily_pnl"] += pnl_usd
        self.state["weekly_pnl"] += pnl_usd

        if strategy_name not in self.state["strategy_states"]:
            self.state["strategy_states"][strategy_name] = {
                "consecutive_losses": 0,
                "total_trades": 0,
                "total_wins": 0,
                "paused": False,
                "pause_until": None,
            }

        s = self.state["strategy_states"][strategy_name]
        s["total_trades"] += 1
        if won:
            s["total_wins"] += 1
            s["consecutive_losses"] = 0
        else:
            s["consecutive_losses"] += 1

        self._check_daily_breaker()
        self._check_weekly_breaker()
        self._check_strategy_breaker(strategy_name)

        self._save_state()

    def _check_daily_breaker(self) -> None:
        loss_pct = abs(self.state["daily_pnl"]) / self.account_balance
        if self.state["daily_pnl"] < 0 and loss_pct >= DAILY_LOSS_LIMIT:
            self.state["daily_breaker_active"] = True
            msg = (
                f"DAILY CIRCUIT BREAKER\n"
                f"Loss: ${abs(self.state['daily_pnl']):.2f} ({loss_pct * 100:.1f}%)\n"
                f"Limit: {DAILY_LOSS_LIMIT * 100}%\n"
                f"Trading paused until midnight UTC"
            )
            logger.critical(msg)
            if self.telegram_func:
                self.telegram_func(msg)

    def _check_weekly_breaker(self) -> None:
        loss_pct = abs(self.state["weekly_pnl"]) / self.account_balance
        if self.state["weekly_pnl"] < 0 and loss_pct >= WEEKLY_LOSS_LIMIT:
            self.state["weekly_breaker_active"] = True
            msg = (
                f"WEEKLY CIRCUIT BREAKER\n"
                f"Loss: ${abs(self.state['weekly_pnl']):.2f} ({loss_pct * 100:.1f}%)\n"
                f"Limit: {WEEKLY_LOSS_LIMIT * 100}%\n"
                f"Trading paused 48 hours"
            )
            logger.critical(msg)
            if self.telegram_func:
                self.telegram_func(msg)
            activate_kill_switch(f"Weekly loss limit hit: {loss_pct * 100:.1f}%")

    def _check_strategy_breaker(self, strategy_name: str) -> None:
        s = self.state["strategy_states"][strategy_name]

        if s["consecutive_losses"] >= CONSECUTIVE_LOSS_LIMIT:
            s["paused"] = True
            pause_until = (datetime.utcnow() + timedelta(hours=24)).isoformat()
            s["pause_until"] = pause_until
            msg = (
                f"STRATEGY PAUSED: {strategy_name}\n"
                f"Reason: {CONSECUTIVE_LOSS_LIMIT} consecutive losses\n"
                f"Paused for 24 hours\n"
                f"Resumes: {pause_until}"
            )
            logger.warning(msg)
            if self.telegram_func:
                self.telegram_func(msg)

    def check_strategy_wr_divergence(
        self,
        strategy_name: str,
        backtest_wr: float,
        live_wr: float,
    ) -> bool:
        """Call weekly to check WR divergence. Returns False if diverged."""
        divergence = backtest_wr - live_wr
        if divergence >= WR_DIVERGENCE_LIMIT:
            msg = (
                f"WR DIVERGENCE: {strategy_name}\n"
                f"Backtest WR: {backtest_wr * 100:.1f}%\n"
                f"Live WR: {live_wr * 100:.1f}%\n"
                f"Divergence: {divergence * 100:.1f}%\n"
                f"Moving back to paper trading"
            )
            logger.warning(msg)
            if self.telegram_func:
                self.telegram_func(msg)
            return False
        return True

    def is_trading_allowed(self, strategy_name: str = None) -> tuple[bool, str]:
        """Returns (allowed, reason). Check this before every trade."""
        from safety.kill_switch import is_kill_switch_active

        self._reset_if_needed()

        if is_kill_switch_active():
            return False, "Kill switch active"

        if self.state["daily_breaker_active"]:
            return False, "Daily loss limit hit (5%)"

        if self.state["weekly_breaker_active"]:
            return False, "Weekly loss limit hit (10%)"

        if strategy_name:
            s = self.state["strategy_states"].get(strategy_name, {})
            if s.get("paused"):
                pause_until = s.get("pause_until")
                if pause_until:
                    if datetime.utcnow().isoformat() < pause_until:
                        return False, f"Strategy paused until {pause_until}"
                    else:
                        s["paused"] = False
                        s["consecutive_losses"] = 0
                        self._save_state()

        return True, "OK"

    def get_status_report(self) -> dict:
        self._reset_if_needed()
        return {
            "daily_pnl": self.state["daily_pnl"],
            "weekly_pnl": self.state["weekly_pnl"],
            "daily_breaker": self.state["daily_breaker_active"],
            "weekly_breaker": self.state["weekly_breaker_active"],
            "strategy_states": self.state["strategy_states"],
        }
