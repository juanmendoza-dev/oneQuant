"""Emergency kill switch.

Creates/removes a file to stop all trading.
Checked before every trade cycle.
"""

import logging
from pathlib import Path

KILL_SWITCH_FILE = Path("/root/oneQuant/KILL_SWITCH")
logger = logging.getLogger(__name__)


def is_kill_switch_active() -> bool:
    """Returns True if kill switch file exists."""
    return KILL_SWITCH_FILE.exists()


def activate_kill_switch(reason: str = "Manual") -> None:
    """Creates kill switch file. Stops all trading."""
    KILL_SWITCH_FILE.write_text(f"KILL SWITCH ACTIVE\nReason: {reason}\n")
    logger.critical("KILL SWITCH ACTIVATED: %s", reason)


def deactivate_kill_switch() -> None:
    """Removes kill switch file. Resumes trading."""
    if KILL_SWITCH_FILE.exists():
        KILL_SWITCH_FILE.unlink()
        logger.info("Kill switch deactivated. Trading resumed.")


def get_kill_switch_reason() -> str:
    """Returns reason kill switch was activated."""
    if KILL_SWITCH_FILE.exists():
        return KILL_SWITCH_FILE.read_text()
    return "Not active"
