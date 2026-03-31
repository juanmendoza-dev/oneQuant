"""Validates every order before it goes to exchange.

Ensures LIMIT only, size limits, stop loss required.
"""

import logging

logger = logging.getLogger(__name__)


class OrderValidator:
    MIN_ORDER_USD = 10.0
    MAX_ORDER_PCT = 0.25

    def validate(
        self,
        order_type: str,
        side: str,
        quantity_btc: float,
        price: float,
        stop_loss_price: float,
        account_balance: float,
    ) -> tuple[bool, str]:
        """Returns (valid, reason). Rejects any MARKET order automatically."""
        # CRITICAL: Never allow market orders
        if order_type.upper() != "LIMIT":
            logger.critical("MARKET ORDER BLOCKED: Only LIMIT orders allowed")
            return False, "MARKET orders not allowed"

        # Stop loss required
        if not stop_loss_price or stop_loss_price <= 0:
            return False, "Stop loss required"

        # Validate stop loss direction
        order_value = quantity_btc * price
        if side == "BUY" and stop_loss_price >= price:
            return False, "Buy stop loss must be below entry"
        if side == "SELL" and stop_loss_price <= price:
            return False, "Sell stop loss must be above entry"

        # Minimum order size
        if order_value < self.MIN_ORDER_USD:
            return False, f"Order too small: ${order_value:.2f} < $10"

        # Maximum order size
        max_order = account_balance * self.MAX_ORDER_PCT
        if order_value > max_order:
            return False, f"Order too large: ${order_value:.2f} > ${max_order:.2f} (25% cap)"

        return True, "OK"
