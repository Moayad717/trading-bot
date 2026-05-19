from __future__ import annotations

from datetime import datetime
from enum import Enum
from typing import Optional

from pydantic import BaseModel, Field, field_validator


class Action(str, Enum):
    BUY = "buy"
    SELL = "sell"


class OrderType(str, Enum):
    MARKET = "market"
    LIMIT = "limit"


class SignalStatus(str, Enum):
    PENDING = "pending"
    FILLED = "filled"
    FAILED = "failed"


class SignalCreate(BaseModel):
    """Normalised signal produced by a source parser before hitting an exchange."""

    action: Action
    symbol: str
    quantity: float
    price: Optional[float] = None          # required for limit orders
    order_type: OrderType = OrderType.MARKET
    category: str = "linear"               # bybit: linear/spot/inverse
    source: str = "unknown"

    @field_validator("symbol")
    @classmethod
    def upper_symbol(cls, v: str) -> str:
        return v.upper().strip()

    @field_validator("quantity")
    @classmethod
    def positive_quantity(cls, v: float) -> float:
        if v <= 0:
            raise ValueError("quantity must be positive")
        return v


class Signal(SignalCreate):
    """Full signal record as stored in the database."""

    id: Optional[int] = None
    timestamp: datetime = Field(default_factory=datetime.utcnow)
    status: SignalStatus = SignalStatus.PENDING
    exchange: str = ""
    order_id: Optional[str] = None
    error_message: Optional[str] = None

    class Config:
        from_attributes = True
