"""Abstract base class that every trading strategy inherits."""

from abc import ABC, abstractmethod
from dataclasses import dataclass


@dataclass
class Signal:
    """Trading signal produced by a strategy.

    Attributes:
        direction:  'BUY', 'SELL', or 'SKIP'.
        confidence: Float from 0.0 to 1.0 indicating signal strength.
        reason:     Human-readable explanation of why this signal was generated.
    """

    direction: str
    confidence: float
    reason: str


class BaseStrategy(ABC):
    """Base class all strategies must inherit.

    Subclasses must set ``name`` and ``timeframe`` and implement
    ``generate_signal``.
    """

    name: str = ""
    timeframe: str = "15m"
    required_candles: int = 22

    @abstractmethod
    def generate_signal(self, candles: list[dict]) -> Signal:
        """Evaluate the last N candles and return a trading signal.

        Args:
            candles: List of candle dicts ordered oldest-first. Each dict
                     has keys: timestamp, open, high, low, close, volume.

        Returns:
            A Signal with direction, confidence, and reason.
        """
        ...
