from abc import ABC, abstractmethod
from typing import Any, Dict

from models.signal import SignalCreate


class BaseExchange(ABC):
    @abstractmethod
    def place_order(self, signal: SignalCreate) -> Dict[str, Any]:
        # must return at least {"order_id": str, "status": str}, raise on failure
        ...

    @abstractmethod
    def cancel_order(self, order_id: str, symbol: str) -> Dict[str, Any]:
        ...

    @abstractmethod
    def get_balance(self, coin: str = "USDT") -> Dict[str, Any]:
        ...

    @property
    @abstractmethod
    def name(self) -> str:
        ...
