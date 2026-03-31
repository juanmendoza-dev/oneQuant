"""Position sizing using 2% risk rule.

Never risk more than 2% of account per trade.
"""


class PositionSizer:
    MAX_RISK_PCT = 0.02   # 2% max risk per trade
    MAX_POSITION_PCT = 0.25  # 25% max position size

    def calculate_position_size(
        self,
        account_balance: float,
        entry_price: float,
        stop_loss_price: float,
    ) -> dict:
        """Calculate position size based on the 2% risk rule.

        Returns:
            dict with keys: valid, position_size_usd, quantity_btc,
            risk_amount, risk_pct, reason.
        """
        risk_per_unit = abs(entry_price - stop_loss_price)

        max_risk_usd = account_balance * self.MAX_RISK_PCT

        if risk_per_unit == 0:
            return {"valid": False, "reason": "Stop loss equals entry price"}

        quantity_btc = max_risk_usd / risk_per_unit
        position_size_usd = quantity_btc * entry_price

        # Cap at 25% of account
        max_position_usd = account_balance * self.MAX_POSITION_PCT
        if position_size_usd > max_position_usd:
            position_size_usd = max_position_usd
            quantity_btc = position_size_usd / entry_price

        # Minimum order size check ($10 minimum on Binance.US)
        if position_size_usd < 10:
            return {
                "valid": False,
                "reason": f"Position too small: ${position_size_usd:.2f} (min $10)",
            }

        return {
            "valid": True,
            "position_size_usd": round(position_size_usd, 2),
            "quantity_btc": round(quantity_btc, 8),
            "risk_amount": round(max_risk_usd, 2),
            "risk_pct": self.MAX_RISK_PCT * 100,
        }
