from abc import ABC, abstractmethod
from typing import Any, Dict

from models.signal import SignalCreate


class BaseExchange(ABC):
    """
    Contract every exchange adapter must implement.
    Adding Binance, Coinbase etc. means creating a new file that
    subclasses this — no changes to core logic required.
    """

    @abstractmethod
    def place_order(self, signal: SignalCreate) -> Dict[str, Any]:
        """
        Submit an order based on the normalised signal.
        Must return a dict with at least {"order_id": str, "status": str}.
        Raise on failure so callers can catch and log.
        """
        ...

    @abstractmethod
    def cancel_order(self, order_id: str, symbol: str) -> Dict[str, Any]:
        """Cancel an open order by its exchange-assigned id."""
        ...

    @abstractmethod
    def get_balance(self, coin: str = "USDT") -> Dict[str, Any]:
        """Return available balance for *coin*."""
        ...

    @property
    @abstractmethod
    def name(self) -> str:
        """Human-readable exchange name, e.g. 'bybit'."""
        ...
