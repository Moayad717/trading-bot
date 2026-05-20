from __future__ import annotations

from typing import Any, Dict, Optional

from models.signal import Action, OrderType, SignalCreate

SOURCE_NAME = "tradingview"

# supported field names — TradingView alert payloads vary a lot
_ACTION_KEYS      = ("action", "side", "signal", "direction", "type_signal")
_SYMBOL_KEYS      = ("symbol", "ticker", "asset", "instrument", "pair", "tv_instrument")
_QUANTITY_KEYS    = ("quantity", "qty", "amount", "size", "contracts")
_PRICE_KEYS       = ("price", "limit_price", "entry_price")
_ORDER_TYPE_KEYS  = ("order_type", "type", "ordertype", "order")
_CATEGORY_KEYS    = ("category", "market_type", "contract_type")
_STOP_LOSS_KEYS     = ("stop_loss", "sl", "stop", "stoploss")
_TAKE_PROFIT_KEYS   = ("take_profit", "tp", "target", "takeprofit")
_TRIGGER_PRICE_KEYS = ("trigger_price", "trigger")
_PATTERN_TYPE_KEYS  = ("pattern", "pattern_type", "rule")

_BUY_ALIASES  = {"buy", "long", "bull", "bullish", "1", "entry_long", "enter_long"}
_SELL_ALIASES = {"sell", "short", "bear", "bearish", "-1", "entry_short", "enter_short"}


def _find(payload: Dict[str, Any], keys: tuple) -> Optional[Any]:
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
    return OrderType.LIMIT if "limit" in str(raw).lower() else OrderType.MARKET


def _to_float(raw: Any, field: str) -> Optional[float]:
    if raw is None:
        return None
    try:
        return float(raw)
    except (TypeError, ValueError):
        raise ValueError(f"Invalid {field} value: '{raw}'")


def parse(payload: Dict[str, Any]) -> SignalCreate:
    action = _parse_action(_find(payload, _ACTION_KEYS))

    # Symbol — strip ".P" suffix (e.g. LINKUSDT.P → LINKUSDT for Bybit API)
    raw_symbol = _find(payload, _SYMBOL_KEYS)
    if raw_symbol is None:
        raise ValueError("Signal payload is missing a symbol/ticker field")
    symbol = str(raw_symbol).upper().strip()
    if symbol.endswith(".P"):
        symbol = symbol[:-2]

    # Nested order object present in the 3Commas/Pine Script payload format
    _order = payload.get("order")
    order_obj: Dict[str, Any] = _order if isinstance(_order, dict) else {}

    # Quantity: flat fields first, then order.amount
    raw_qty = _find(payload, _QUANTITY_KEYS)
    if raw_qty is None:
        raw_qty = order_obj.get("amount")
    if raw_qty is None:
        raise ValueError("Signal payload is missing a quantity/qty field")
    try:
        quantity = float(raw_qty)
    except (TypeError, ValueError):
        raise ValueError(f"Invalid quantity value: '{raw_qty}'")

    # Price: flat fields first, then order.price
    raw_price = _find(payload, _PRICE_KEYS)
    if raw_price is None:
        raw_price = order_obj.get("price")
    price = _to_float(raw_price, "price")

    # Order type: flat fields first, then order.order_type
    # Guard against _find matching "order" key and returning the nested dict
    raw_order_type = _find(payload, _ORDER_TYPE_KEYS)
    if isinstance(raw_order_type, dict):
        raw_order_type = order_obj.get("order_type")
    order_type = _parse_order_type(raw_order_type)

    # Category
    raw_category = _find(payload, _CATEGORY_KEYS)
    category = str(raw_category).lower() if raw_category and not isinstance(raw_category, dict) else "linear"

    # Trigger price and pattern type — flat fields only
    trigger_price = _to_float(_find(payload, _TRIGGER_PRICE_KEYS), "trigger_price")
    raw_pattern = _find(payload, _PATTERN_TYPE_KEYS)
    pattern_type: Optional[str] = str(raw_pattern).strip() if raw_pattern and not isinstance(raw_pattern, dict) else None

    # Stop loss — flat fields only
    stop_loss = _to_float(_find(payload, _STOP_LOSS_KEYS), "stop_loss")

    # Take profit — flat scalar first, then derive from take_profit.steps[0].price_percent
    # Guard against _find matching "take_profit" key and returning the nested dict
    raw_tp = _find(payload, _TAKE_PROFIT_KEYS)
    take_profit: Optional[float] = None

    if raw_tp is not None and not isinstance(raw_tp, dict):
        take_profit = _to_float(raw_tp, "take_profit")
    elif price is not None:
        tp_obj = payload.get("take_profit")
        if isinstance(tp_obj, dict) and tp_obj.get("enabled"):
            steps = tp_obj.get("steps", [])
            if steps and isinstance(steps[0], dict):
                pct = steps[0].get("price_percent")
                if pct is not None:
                    try:
                        pct = float(pct)
                        # pct is already signed (negative for shorts), so this works for both
                        take_profit = round(price * (1 + pct / 100), 8)
                    except (TypeError, ValueError):
                        pass

    return SignalCreate(
        action=action,
        symbol=symbol,
        quantity=quantity,
        price=price,
        order_type=order_type,
        category=category,
        source=SOURCE_NAME,
        stop_loss=stop_loss,
        take_profit=take_profit,
        trigger_price=trigger_price,
        pattern_type=pattern_type,
    )
