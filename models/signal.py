from __future__ import annotations

from datetime import datetime
from enum import Enum
from typing import Optional

from pydantic import BaseModel, Field, field_validator

from config import now_local


class Action(str, Enum):
    BUY = "buy"
    SELL = "sell"


class OrderType(str, Enum):
    MARKET = "market"
    LIMIT = "limit"


class SignalStatus(str, Enum):
    PENDING = "pending"
    OPEN = "open"      # order placed on exchange, waiting for price to hit
    FILLED = "filled"  # order actually executed
    FAILED = "failed"


class SignalCreate(BaseModel):
    action: Action
    symbol: str
    quantity: float
    price: Optional[float] = None
    order_type: OrderType = OrderType.MARKET
    category: str = "linear"  # bybit: linear/spot/inverse
    source: str = "unknown"
    stop_loss: Optional[float] = None
    take_profit: Optional[float] = None

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
    id: Optional[int] = None
    timestamp: datetime = Field(default_factory=now_local)
    status: SignalStatus = SignalStatus.PENDING
    exchange: str = ""
    order_id: Optional[str] = None
    error_message: Optional[str] = None

    class Config:
        from_attributes = True
