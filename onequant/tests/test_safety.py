"""Safety system test suite.

Tests all safety components:
  - PositionSizer (2% risk rule)
  - Kill switch (activate/deactivate)
  - OrderValidator (LIMIT only, size limits)
  - CircuitBreaker (daily/weekly/strategy)
  - FeeMonitor (fee verification)
"""

import json
import os
import sys
from pathlib import Path

# Ensure project root is on path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))


# ---------------------------------------------------------------------------
# Test 1 — Position Sizer
# ---------------------------------------------------------------------------


def test_position_sizer():
    from safety.position_sizer import PositionSizer

    sizer = PositionSizer()

    # Normal case: $300 account, 2.88% stop
    result = sizer.calculate_position_size(
        account_balance=300,
        entry_price=85000,
        stop_loss_price=82550,  # 2.88% stop
    )
    assert result["valid"] is True
    assert result["risk_amount"] == 6.0  # 2% of $300
    assert result["position_size_usd"] > 0
    assert result["quantity_btc"] > 0
    print(f"  Position size: ${result['position_size_usd']}")
    print(f"  Quantity BTC: {result['quantity_btc']}")
    print(f"  Risk: ${result['risk_amount']} ({result['risk_pct']}%)")

    # Edge case: stop loss equals entry
    result2 = sizer.calculate_position_size(
        account_balance=300,
        entry_price=85000,
        stop_loss_price=85000,
    )
    assert result2["valid"] is False
    print(f"  Zero stop: {result2['reason']}")

    # Edge case: tiny account (position too small)
    result3 = sizer.calculate_position_size(
        account_balance=5,
        entry_price=85000,
        stop_loss_price=84000,
    )
    assert result3["valid"] is False
    print(f"  Tiny account: {result3['reason']}")

    # Edge case: very tight stop should cap at 25%
    result4 = sizer.calculate_position_size(
        account_balance=300,
        entry_price=85000,
        stop_loss_price=84999,  # $1 stop → huge position, capped at 25%
    )
    assert result4["valid"] is True
    assert result4["position_size_usd"] <= 300 * 0.25 + 0.01
    print(f"  25% cap applied: ${result4['position_size_usd']}")

    print("  PASS")


# ---------------------------------------------------------------------------
# Test 2 — Kill Switch
# ---------------------------------------------------------------------------


def test_kill_switch():
    from safety.kill_switch import (
        activate_kill_switch,
        deactivate_kill_switch,
        is_kill_switch_active,
        get_kill_switch_reason,
        KILL_SWITCH_FILE,
    )

    # Clean up first
    if KILL_SWITCH_FILE.exists():
        KILL_SWITCH_FILE.unlink()

    assert is_kill_switch_active() is False
    print("  Inactive: OK")

    activate_kill_switch("Test activation")
    assert is_kill_switch_active() is True
    reason = get_kill_switch_reason()
    assert "Test activation" in reason
    print(f"  Active: OK (reason: {reason.strip()})")

    deactivate_kill_switch()
    assert is_kill_switch_active() is False
    print("  Deactivated: OK")

    print("  PASS")


# ---------------------------------------------------------------------------
# Test 3 — Order Validator
# ---------------------------------------------------------------------------


def test_order_validator():
    from safety.order_validator import OrderValidator

    validator = OrderValidator()

    # Should reject market order
    valid, reason = validator.validate(
        order_type="MARKET",
        side="BUY",
        quantity_btc=0.001,
        price=85000,
        stop_loss_price=84000,
        account_balance=300,
    )
    assert valid is False
    assert "MARKET" in reason
    print(f"  Market blocked: {reason}")

    # Should accept limit order
    valid, reason = validator.validate(
        order_type="LIMIT",
        side="BUY",
        quantity_btc=0.0008,
        price=85000,
        stop_loss_price=84000,
        account_balance=300,
    )
    assert valid is True
    print(f"  Limit accepted: {reason}")

    # Should reject missing stop loss
    valid, reason = validator.validate(
        order_type="LIMIT",
        side="BUY",
        quantity_btc=0.001,
        price=85000,
        stop_loss_price=0,
        account_balance=300,
    )
    assert valid is False
    print(f"  No SL rejected: {reason}")

    # Should reject wrong SL direction (buy with SL above entry)
    valid, reason = validator.validate(
        order_type="LIMIT",
        side="BUY",
        quantity_btc=0.001,
        price=85000,
        stop_loss_price=86000,
        account_balance=300,
    )
    assert valid is False
    print(f"  Bad SL direction: {reason}")

    # Should reject oversized order (>25% of account)
    valid, reason = validator.validate(
        order_type="LIMIT",
        side="BUY",
        quantity_btc=0.01,
        price=85000,
        stop_loss_price=84000,
        account_balance=300,
    )
    assert valid is False
    assert "25%" in reason
    print(f"  Oversized rejected: {reason}")

    # Should reject undersized order (<$10)
    valid, reason = validator.validate(
        order_type="LIMIT",
        side="BUY",
        quantity_btc=0.0000001,
        price=85000,
        stop_loss_price=84000,
        account_balance=300,
    )
    assert valid is False
    assert "small" in reason
    print(f"  Undersized rejected: {reason}")

    print("  PASS")


# ---------------------------------------------------------------------------
# Test 4 — Circuit Breaker
# ---------------------------------------------------------------------------


def test_circuit_breaker():
    from safety.circuit_breaker import CircuitBreaker, CIRCUIT_BREAKER_STATE_FILE
    from safety.kill_switch import deactivate_kill_switch, KILL_SWITCH_FILE

    # Clean state
    if CIRCUIT_BREAKER_STATE_FILE.exists():
        CIRCUIT_BREAKER_STATE_FILE.unlink()
    if KILL_SWITCH_FILE.exists():
        KILL_SWITCH_FILE.unlink()

    cb = CircuitBreaker(account_balance=300)

    # Should allow trading initially
    allowed, reason = cb.is_trading_allowed()
    assert allowed is True
    print(f"  Initial: trading allowed ({reason})")

    # Simulate 5 consecutive losses for strategy breaker
    for i in range(5):
        cb.record_trade_pnl("test_strategy", -2.0, False)

    # Strategy should be paused
    allowed, reason = cb.is_trading_allowed("test_strategy")
    assert allowed is False
    assert "paused" in reason.lower()
    print(f"  5 losses: {reason}")

    # Other strategies should still work
    allowed, reason = cb.is_trading_allowed("other_strategy")
    assert allowed is True
    print(f"  Other strategy: {reason}")

    # Test daily breaker (5% of $300 = $15)
    if CIRCUIT_BREAKER_STATE_FILE.exists():
        CIRCUIT_BREAKER_STATE_FILE.unlink()
    if KILL_SWITCH_FILE.exists():
        KILL_SWITCH_FILE.unlink()

    cb2 = CircuitBreaker(account_balance=300)
    cb2.record_trade_pnl("daily_test", -16.0, False)  # >5%
    allowed, reason = cb2.is_trading_allowed()
    assert allowed is False
    assert "Daily" in reason
    print(f"  Daily breaker: {reason}")

    # Clean up
    if CIRCUIT_BREAKER_STATE_FILE.exists():
        CIRCUIT_BREAKER_STATE_FILE.unlink()
    deactivate_kill_switch()

    print("  PASS")


# ---------------------------------------------------------------------------
# Test 5 — Fee Monitor
# ---------------------------------------------------------------------------


def test_fee_monitor():
    from safety.fee_monitor import FeeMonitor
    from safety.kill_switch import deactivate_kill_switch, KILL_SWITCH_FILE

    if KILL_SWITCH_FILE.exists():
        KILL_SWITCH_FILE.unlink()

    fm = FeeMonitor()

    # 0 fee should pass
    ok = fm.verify_trade_fee("order_1", 100.0, 0.0)
    assert ok is True
    print("  Zero fee: OK")

    # Tiny fee within tolerance should pass
    ok = fm.verify_trade_fee("order_2", 100.0, 0.005)
    assert ok is True
    print("  Tiny fee ($0.005): OK")

    # Fee above tolerance should fail
    ok = fm.verify_trade_fee("order_3", 100.0, 0.50)
    assert ok is False
    print("  High fee ($0.50): BLOCKED")

    report = fm.get_daily_fee_report()
    assert report["fee_violations"] == 1
    print(f"  Report: {report}")

    # Clean up
    deactivate_kill_switch()

    print("  PASS")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------


def main():
    print("=" * 50)
    print("oneQuant Safety System Tests")
    print("=" * 50)

    tests = [
        ("Position Sizer", test_position_sizer),
        ("Kill Switch", test_kill_switch),
        ("Order Validator", test_order_validator),
        ("Circuit Breaker", test_circuit_breaker),
        ("Fee Monitor", test_fee_monitor),
    ]

    results = []
    for name, fn in tests:
        print(f"\n  Test: {name}")
        try:
            fn()
            results.append((name, True))
            print(f"  Result: PASS")
        except Exception as e:
            results.append((name, False))
            print(f"  Result: FAIL — {e}")

    print("\n" + "=" * 50)
    print("  Test Summary")
    print("=" * 50)
    all_passed = True
    for name, passed in results:
        symbol = "PASS" if passed else "FAIL"
        print(f"  [{symbol}] {name}")
        if not passed:
            all_passed = False

    if all_passed:
        print("\n  ALL TESTS PASSED")
    else:
        print("\n  SOME TESTS FAILED")
        sys.exit(1)


if __name__ == "__main__":
    main()
