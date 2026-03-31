"""Per-trade fee verification.

Every trade checks actual fee charged.
If fee > expected, pause trading and alert.
"""

import logging

from safety.kill_switch import activate_kill_switch

logger = logging.getLogger(__name__)

EXPECTED_MAKER_FEE = 0.0000
FEE_TOLERANCE = 0.0001  # 0.01% tolerance


class FeeMonitor:
    def __init__(self, telegram_func=None):
        self.telegram_func = telegram_func
        self.fee_violations = 0
        self.total_fees_paid = 0.0

    def verify_trade_fee(
        self,
        order_id: str,
        trade_value_usd: float,
        actual_fee_usd: float,
        order_type: str = "LIMIT",
    ) -> bool:
        """Returns True if fee is acceptable. Activates kill switch if fee is way off."""
        self.total_fees_paid += actual_fee_usd

        fee_pct = actual_fee_usd / trade_value_usd if trade_value_usd > 0 else 0

        if fee_pct > FEE_TOLERANCE:
            self.fee_violations += 1
            msg = (
                f"FEE ALERT: Order {order_id}\n"
                f"Expected: $0.00\n"
                f"Actual: ${actual_fee_usd:.4f} ({fee_pct * 100:.4f}%)\n"
                f"Trade value: ${trade_value_usd:.2f}\n"
                f"ACTION: Trading paused"
            )
            logger.critical(msg)

            if self.telegram_func:
                self.telegram_func(msg)

            # Activate kill switch if fees are wrong 3 times
            if self.fee_violations >= 3:
                activate_kill_switch(
                    f"Fee structure changed: {fee_pct * 100:.4f}% per trade"
                )
            return False

        logger.info("Fee verified: $%.4f (expected $0.00)", actual_fee_usd)
        return True

    def get_daily_fee_report(self) -> dict:
        return {
            "total_fees_paid": self.total_fees_paid,
            "fee_violations": self.fee_violations,
            "status": "CLEAN" if self.fee_violations == 0 else "VIOLATIONS DETECTED",
        }
