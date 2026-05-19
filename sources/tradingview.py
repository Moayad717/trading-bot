"""
TradingView webhook payload → normalised SignalCreate.

TradingView allows fully custom JSON bodies in alert messages.
This parser accepts the most common field-name conventions and falls
back gracefully so the exact payload format can evolve without
touching any other module.

Supported payload shapes (all fields case-insensitive):
  { "action": "buy",  "symbol": "BTCUSDT", "qty": 0.001 }
  { "side":   "long", "ticker": "ETHUSDT", "amount": 0.5, "price": 3000, "type": "limit" }
  { "signal": "sell", "asset":  "SOLUSDT", "quantity": 10 }
"""
from __future__ import annotations

from typing import Any, Dict, Optional

from models.signal import Action, OrderType, SignalCreate

SOURCE_NAME = "tradingview"

# Field aliases → canonical name
_ACTION_KEYS = ("action", "side", "signal", "direction", "type_signal")
_SYMBOL_KEYS = ("symbol", "ticker", "asset", "instrument", "pair")
_QUANTITY_KEYS = ("quantity", "qty", "amount", "size", "contracts")
_PRICE_KEYS = ("price", "limit_price", "entry_price")
_ORDER_TYPE_KEYS = ("order_type", "type", "ordertype", "order")
_CATEGORY_KEYS = ("category", "market_type", "contract_type")

# Aliases that map to Action values
_BUY_ALIASES = {"buy", "long", "bull", "bullish", "1", "entry_long"}
_SELL_ALIASES = {"sell", "short", "bear", "bearish", "-1", "entry_short"}


def _find(payload: Dict[str, Any], keys: tuple) -> Optional[Any]:
    """Return the first value whose lower-cased key is in *keys*."""
    normalised = {k.lower(): v for k, v in payload.items()}
    for key in keys:
        if key in normalised:
            return normalised[key]
    return None


def _parse_action(raw: Any) -> Action:
    if raw is None:
        raise ValueError("Signal payload is missing an action/side field")
    val = str(raw).lower().strip()
    if val in _BUY_ALIASES:
        return Action.BUY
    if val in _SELL_ALIASES:
        return Action.SELL
    raise ValueError(f"Unrecognised action value: '{raw}'")


def _parse_order_type(raw: Any) -> OrderType:
    if raw is None:
        return OrderType.MARKET
    val = str(raw).lower().strip()
    if "limit" in val:
        return OrderType.LIMIT
    return OrderType.MARKET


def parse(payload: Dict[str, Any]) -> SignalCreate:
    """
    Parse a raw TradingView webhook dict into a SignalCreate.
    Raises ValueError with a descriptive message on bad input.
    """
    action = _parse_action(_find(payload, _ACTION_KEYS))

    raw_symbol = _find(payload, _SYMBOL_KEYS)
    if raw_symbol is None:
        raise ValueError("Signal payload is missing a symbol/ticker field")
    symbol = str(raw_symbol).upper().strip()

    raw_qty = _find(payload, _QUANTITY_KEYS)
    if raw_qty is None:
        raise ValueError("Signal payload is missing a quantity/qty field")
    try:
        quantity = float(raw_qty)
    except (TypeError, ValueError):
        raise ValueError(f"Invalid quantity value: '{raw_qty}'")

    raw_price = _find(payload, _PRICE_KEYS)
    price: Optional[float] = None
    if raw_price is not None:
        try:
            price = float(raw_price)
        except (TypeError, ValueError):
            raise ValueError(f"Invalid price value: '{raw_price}'")

    order_type = _parse_order_type(_find(payload, _ORDER_TYPE_KEYS))

    raw_category = _find(payload, _CATEGORY_KEYS)
    category = str(raw_category).lower() if raw_category else "linear"

    return SignalCreate(
        action=action,
        symbol=symbol,
        quantity=quantity,
        price=price,
        order_type=order_type,
        category=category,
        source=SOURCE_NAME,
    )
