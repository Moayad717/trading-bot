from __future__ import annotations

import logging
import re
from decimal import ROUND_DOWN, Decimal
from typing import Any, Dict, List, Optional

from pybit.exceptions import InvalidRequestError
from pybit.unified_trading import HTTP

from config import settings
from exchanges.base import BaseExchange
from models.signal import Action, OrderType, SignalCreate

logger = logging.getLogger(__name__)

# Module-level cache shared across all BybitExchange instances.
# Maps "category:symbol" → qtyStep string e.g. "0.001".
_qty_step_cache: Dict[str, str] = {}

# Bybit v5 ErrCode when orderLinkId has already been used — confirmed live
# (2026-08-31): InvalidRequestError('OrderLinkedID is duplicate (ErrCode: 110072)').
_DUPLICATE_LINK_ID_ERRCODE = 110072
_MAX_LINK_ID_SEQ = 20  # generous cap; TP/CTP get cancelled+replaced repeatedly


def build_order_link_id(of_id: str, role: str) -> str:
    """<of_id>_<role> — e.g. "1787220720000_51_L_TP". of_id is capped at 18
    chars in production; role suffixes are at most 5 chars ("_CTP19"), well
    inside Bybit's 36-char orderLinkId limit."""
    return f"{of_id}_{role}"


# Roles that only ever reduce a position — TP/CTP/SL/CLOSE, optionally followed
# by a retry sequence number (_TP2, _CTP3, ...). Entries (_E/_CE) never get a
# sequence suffix and are deliberately excluded from this pattern.
_CLOSING_ROLE_RE = re.compile(r"_(?:TP|CTP|SL|CLOSE)\d*$")


def is_closing_order(order: Dict[str, Any]) -> bool:
    """True if this order can only reduce a position — never open or grow one.

    Primary signal is the orderLinkId role suffix (reliable: we control what
    we tag every order with). Falls back to Bybit's own reduceOnly flag for
    orders that predate orderLinkId tagging or come from outside our own
    placement code — but reduceOnly alone is NOT reliable on its own for this
    purpose: see BYBIT_QUIRKS.md — Bybit auto-applies it to some closing
    orders and not others depending on quota state at placement time, and a
    plain resting order with reduceOnly unset can still be closing-only in
    intent (before Bybit gets to classify it).
    """
    if _CLOSING_ROLE_RE.search(str(order.get("orderLinkId") or "")):
        return True
    return bool(order.get("reduceOnly"))


class BybitExchange(BaseExchange):
    def __init__(self) -> None:
        self._client = HTTP(
            testnet=settings.TESTNET,
            api_key=settings.BYBIT_API_KEY,
            api_secret=settings.BYBIT_API_SECRET,
        )

    @property
    def name(self) -> str:
        return "bybit"

    def _get_qty_step(self, symbol: str, category: str = "linear") -> str:
        """Fetch the qtyStep for a symbol from Bybit and cache it.
        Falls back to '0.001' (3 dp, conservative) on any error."""
        key = f"{category}:{symbol}"
        if key in _qty_step_cache:
            return _qty_step_cache[key]
        try:
            response: Any = self._client.get_instruments_info(category=category, symbol=symbol)
            items = response.get("result", {}).get("list", [])
            if items:
                step = str(items[0].get("lotSizeFilter", {}).get("qtyStep", "0.001"))
                _qty_step_cache[key] = step
                logger.info("qtyStep cached: symbol=%s step=%s", symbol, step)
                return step
        except Exception as exc:
            logger.warning("Could not fetch qtyStep for %s — defaulting to 0.001: %s", symbol, exc)
        _qty_step_cache[key] = "0.001"
        return "0.001"

    def round_qty(self, qty: float, symbol: str, category: str = "linear") -> float:
        """Round qty DOWN to the nearest qtyStep for the given symbol."""
        step = self._get_qty_step(symbol, category)
        step_d = Decimal(step)
        qty_d  = Decimal(str(qty)).quantize(step_d, rounding=ROUND_DOWN)
        return float(qty_d)

    def _lookup_by_link_id(self, symbol: str, order_link_id: str, category: str = "linear") -> Dict[str, Any]:
        """Fetch an order's current info by orderLinkId — open orders first, then
        history, since a duplicate-link-id retry may arrive after the original
        order already filled or was cancelled."""
        resp: Any = self._client.get_open_orders(category=category, symbol=symbol, orderLinkId=order_link_id)
        lst = resp.get("result", {}).get("list", [])
        if not lst:
            resp = self._client.get_order_history(category=category, symbol=symbol, orderLinkId=order_link_id)
            lst = resp.get("result", {}).get("list", [])
        if not lst:
            raise RuntimeError(f"orderLinkId={order_link_id} reported duplicate by Bybit but not found in open orders or history")
        o = lst[0]
        return {"order_id": o.get("orderId", ""), "status": o.get("orderStatus", ""), "raw": resp}

    def _submit_with_link_id_retry(
        self, params: Dict[str, Any], base_link_id: Optional[str], allow_sequence: bool,
    ) -> Dict[str, Any]:
        """Place an order, attaching orderLinkId=base_link_id if given.

        allow_sequence=True (TP/CTP/SL — routinely cancelled and re-placed):
          on a duplicate-link-id rejection, retry with "2", "3", ... appended,
          up to _MAX_LINK_ID_SEQ attempts.

        allow_sequence=False (entries — must NEVER get a numbered retry): on a
        duplicate-link-id rejection, the entry was already placed by an earlier
        request for the exact same signal. Look up and return that existing
        order instead of placing a second one — this is what turns Bybit's own
        duplicate-orderLinkId rejection into the exchange-level duplicate-order
        guard, rather than something our own code has to prevent.
        """
        symbol   = params["symbol"]
        category = params.get("category", "linear")

        if base_link_id is None:
            response: Any = self._client.place_order(**params)
            self._raise_for_error(response)
            result = response.get("result", {})
            return {"order_id": result.get("orderId", ""), "status": result.get("orderStatus", ""), "raw": response}

        attempt = 0
        link_id = base_link_id
        while True:
            attempt += 1
            try:
                response = self._client.place_order(**{**params, "orderLinkId": link_id})
                self._raise_for_error(response)
                result = response.get("result", {})
                return {"order_id": result.get("orderId", ""), "status": result.get("orderStatus", ""), "raw": response}
            except InvalidRequestError as exc:
                if getattr(exc, "status_code", None) != _DUPLICATE_LINK_ID_ERRCODE:
                    raise
                if not allow_sequence:
                    logger.info(
                        "Entry orderLinkId=%s already exists on Bybit — treating as "
                        "already placed (duplicate-order guard), not an error.",
                        link_id,
                    )
                    return self._lookup_by_link_id(symbol, link_id, category)
                if attempt >= _MAX_LINK_ID_SEQ:
                    raise RuntimeError(
                        f"Exhausted {_MAX_LINK_ID_SEQ} orderLinkId sequence attempts for "
                        f"base={base_link_id} — all duplicates"
                    ) from exc
                link_id = f"{base_link_id}{attempt + 1}"
                logger.info(
                    "orderLinkId=%s already used — retrying as %s (attempt %d/%d)",
                    base_link_id, link_id, attempt + 1, _MAX_LINK_ID_SEQ,
                )

    def place_order(self, signal: SignalCreate) -> Dict[str, Any]:
        side = "Buy" if signal.action == Action.BUY else "Sell"
        qty  = self.round_qty(signal.quantity, signal.symbol, signal.category)

        if signal.price is None:
            raise ValueError("price is required for limit orders")

        params: Dict[str, Any] = {
            "category":    signal.category,
            "symbol":      signal.symbol,
            "side":        side,
            "orderType":   "Limit",
            "qty":         str(qty),
            "price":       str(signal.price),
            "timeInForce": "GTC",
            "positionIdx": 1 if side == "Buy" else 2,
        }

        if signal.stop_loss is not None:
            params["stopLoss"] = str(signal.stop_loss)

        # Entries get NO sequence suffix: a duplicate submission for the same
        # of_id must be rejected by Bybit and treated as "already placed", not
        # silently retried under a new tag — that's what actually kills the
        # duplicate-order problem, at the exchange instead of after the fact.
        link_id = None
        if signal.of_id:
            role = "CE" if (signal.pattern_type or "").upper() == "COUNTER" else "E"
            link_id = build_order_link_id(signal.of_id, role)

        return self._submit_with_link_id_retry(params, link_id, allow_sequence=False)

    def place_tp_order(
        self,
        symbol: str,
        side: str,
        qty: float,
        price: float,
        position_idx: int,
        category: str = "linear",
        order_link_id_base: Optional[str] = None,
    ) -> Dict[str, Any]:
        params: Dict[str, Any] = {
            "category": category,
            "symbol": symbol,
            "side": side,
            "orderType": "Limit",
            "qty": str(qty),
            "price": str(price),
            "timeInForce": "GTC",
            "positionIdx": position_idx,
        }
        return self._submit_with_link_id_retry(params, order_link_id_base, allow_sequence=True)

    def place_conditional_sl(
        self,
        symbol: str,
        position_idx: int,
        side: str,
        qty: float,
        trigger_price: float,
        trigger_direction: int,
        order_link_id_base: Optional[str] = None,
        category: str = "linear",
    ) -> Dict[str, Any]:
        """Attach a stop-loss to the ORIGINAL as a real conditional Limit order,
        not a position-level set_trading_stop field.

        set_trading_stop has exactly ONE stop slot per position side: when two
        counters on the same side fill close together, the second call silently
        overwrites the first, leaving the first original's SL gone with no
        error raised. Confirmed with real data (2026-08-28, five counters
        filled within 3 seconds; four of the five originals' stops had already
        been overwritten, and none of the five were ever actually sold on the
        exchange despite the DB marking all five completed).

        A real order does not have this ceiling — each original gets its own
        order_id, and cancel_close_original can cancel exactly one by id
        instead of wiping every stop on that side.

        trigger_direction: 1 = Rise (short original: side="Buy", trigger above
        current price), 2 = Fall (long original: side="Sell", trigger below).

        No reduceOnly — confirmed live (2026-08-31) that Bybit does not apply
        the reduce-only quota to a conditional/untriggered order at all (it
        placed and stayed reduceOnly=False on a side where the quota was
        already 100% consumed for plain resting orders); positionIdx already
        prevents this order from opening anything on the wrong side.
        """
        params: Dict[str, Any] = {
            "category":          category,
            "symbol":            symbol,
            "side":              side,
            "orderType":         "Limit",
            "qty":               str(qty),
            "price":             str(trigger_price),
            "triggerPrice":      str(trigger_price),
            "triggerBy":         "LastPrice",
            "triggerDirection":  trigger_direction,
            "timeInForce":       "GTC",
            "positionIdx":       position_idx,
        }
        return self._submit_with_link_id_retry(params, order_link_id_base, allow_sequence=True)

    def place_partial_sl(
        self,
        symbol: str,
        position_idx: int,
        sl_trigger_price: float,
        sl_size: float,
        category: str = "linear",
    ) -> Dict[str, Any]:
        """DEPRECATED — superseded by place_conditional_sl (see its docstring for
        why). Kept only so cancel_partial_sl remains usable for legacy rows
        whose SL was placed before that change (sl_placed=1, sl_order_id=NULL)
        and therefore still lives as a position-level set_trading_stop field,
        not a real order. Do not call this for new SL placements.

        tpslMode="Partial" targets only sl_size contracts, leaving the rest of
        the position untouched.  slOrderType="Limit" with slLimitPrice=trigger
        means Bybit places a limit at the trigger price when triggered; if price
        gaps through the level the order may not fill (accepted trade-off per spec).
        """
        params: Dict[str, Any] = {
            "category":    category,
            "symbol":      symbol,
            "tpslMode":    "Partial",
            "slSize":      str(sl_size),
            "stopLoss":    str(sl_trigger_price),   # Bybit v5 field (not slTriggerPrice)
            "slTriggerBy":  "LastPrice",
            "slOrderType":  "Limit",
            "slLimitPrice": str(sl_trigger_price),
            "positionIdx": position_idx,
        }
        response: Any = self._client.set_trading_stop(**params)
        self._raise_for_error(response)
        return response.get("result", {})

    def cancel_partial_sl(
        self,
        symbol: str,
        position_idx: int,
        category: str = "linear",
    ) -> Dict[str, Any]:
        """Remove a LEGACY position-level Partial SL by resetting slSize to 0.
        Only relevant for rows predating place_conditional_sl (see its
        docstring) — current SLs are real orders, cancelled via cancel_order."""
        params: Dict[str, Any] = {
            "category":    category,
            "symbol":      symbol,
            "tpslMode":    "Partial",
            "stopLoss":    "0",   # clears the stop price; slSize alone is not enough
            "slSize":      "0",
            "positionIdx": position_idx,
        }
        response: Any = self._client.set_trading_stop(**params)
        self._raise_for_error(response)
        return response.get("result", {})

    def place_limit_close_order(
        self,
        symbol: str,
        side: str,
        qty: float,
        price: float,
        position_idx: int,
        category: str = "linear",
        order_link_id: Optional[str] = None,
    ) -> Dict[str, Any]:
        """Place a Limit order to close an open position at the given price."""
        params: Dict[str, Any] = {
            "category":    category,
            "symbol":      symbol,
            "side":        side,
            "orderType":   "Limit",
            "qty":         str(qty),
            "price":       str(price),
            "timeInForce": "GTC",
            "positionIdx": position_idx,
        }
        if order_link_id:
            params["orderLinkId"] = order_link_id
        response: Any = self._client.place_order(**params)
        self._raise_for_error(response)
        result = response.get("result", {})
        return {
            "order_id": result.get("orderId", ""),
            "status": result.get("orderStatus", ""),
            "raw": response,
        }

    def get_execution_history(
        self, symbol: str = "", category: str = "linear", limit: int = 200
    ) -> List[Dict[str, Any]]:
        """Return position-closing executions (closedSize > 0), newest first.
        Paginates automatically — Bybit caps each page at 100.
        Pass symbol to filter by one contract; omit to get all symbols.
        Each returned dict has: orderId, execTime (ms str), closedSize, side, symbol.
        """
        results: List[Dict[str, Any]] = []
        cursor: str = ""
        remaining = limit
        while remaining > 0:
            params: Dict[str, Any] = {
                "category": category,
                "limit":    min(remaining, 100),
            }
            if symbol:
                params["symbol"] = symbol
            if cursor:
                params["cursor"] = cursor
            response: Any = self._client.get_executions(**params)
            self._raise_for_error(response)
            page_result = response.get("result", {})
            for ex in page_result.get("list", []):
                if float(ex.get("closedSize", 0)) > 0:
                    results.append({
                        "orderId":    ex["orderId"],
                        "execTime":   ex["execTime"],
                        "closedSize": ex["closedSize"],
                        "side":       ex["side"],
                        "symbol":     ex.get("symbol", ""),
                    })
            cursor     = page_result.get("nextPageCursor", "")
            remaining -= min(remaining, 100)
            if not cursor:
                break
        return results

    def get_position_size(self, symbol: str, side: str, category: str = "linear") -> float:
        """Return the current open position size for a specific symbol and side ('Buy'/'Sell')."""
        response: Any = self._client.get_positions(category=category, symbol=symbol)
        self._raise_for_error(response)
        for pos in response.get("result", {}).get("list", []):
            if pos.get("side") == side:
                return float(pos.get("size", 0))
        return 0.0

    def get_positions(self, category: str = "linear", settle_coin: str = "USDT") -> List[Dict[str, Any]]:
        """Return all open positions for the given settle currency."""
        response: Any = self._client.get_positions(category=category, settleCoin=settle_coin)
        self._raise_for_error(response)
        return response.get("result", {}).get("list", [])

    def get_open_orders(self, category: str = "linear", settle_coin: str = "USDT") -> List[Dict[str, Any]]:
        """Return active orders for one page (limit 50). Use get_all_open_orders for full list."""
        response: Any = self._client.get_open_orders(
            category=category, settleCoin=settle_coin, limit=50
        )
        self._raise_for_error(response)
        return response.get("result", {}).get("list", [])

    def get_all_open_orders(self, category: str = "linear", settle_coin: str = "USDT") -> List[Dict[str, Any]]:
        """Return all active orders across all pages by following the cursor."""
        orders: List[Dict[str, Any]] = []
        cursor: str = ""
        while True:
            params: Dict[str, Any] = {
                "category": category,
                "settleCoin": settle_coin,
                "limit": 50,
            }
            if cursor:
                params["cursor"] = cursor
            response: Any = self._client.get_open_orders(**params)
            self._raise_for_error(response)
            result = response.get("result", {})
            orders.extend(result.get("list", []))
            cursor = result.get("nextPageCursor", "")
            if not cursor:
                break
        return orders

    def get_order_history(
        self, category: str = "linear", settle_coin: str = "USDT", limit: int = 50
    ) -> List[Dict[str, Any]]:
        """Return recent order history (most recent first) for the given settle currency."""
        response: Any = self._client.get_order_history(
            category=category, settleCoin=settle_coin, limit=limit
        )
        self._raise_for_error(response)
        return response.get("result", {}).get("list", [])

    def get_equity(self) -> float:
        """Return totalEquity of the UNIFIED account in USDT."""
        response: Any = self._client.get_wallet_balance(accountType="UNIFIED")
        self._raise_for_error(response)
        accounts = response.get("result", {}).get("list", [])
        if accounts:
            return float(accounts[0].get("totalEquity", 0) or 0)
        return 0.0

    def cancel_order(self, order_id: str, symbol: str) -> Dict[str, Any]:
        response: Any = self._client.cancel_order(
            category="linear",
            symbol=symbol,
            orderId=order_id,
        )
        self._raise_for_error(response)
        return response.get("result", {})

    def get_api_key_info(self) -> Dict[str, Any]:
        response: Any = self._client.get_api_key_information()
        self._raise_for_error(response)
        return response.get("result", {})

    def get_balance(self, coin: str = "USDT") -> Dict[str, Any]:
        response: Any = self._client.get_wallet_balance(
            accountType="UNIFIED",
            coin=coin,
        )
        self._raise_for_error(response)
        coins = (
            response.get("result", {})
            .get("list", [{}])[0]
            .get("coin", [])
        )
        for entry in coins:
            if entry.get("coin") == coin:
                return entry
        return {}

    @staticmethod
    def _raise_for_error(response: Dict[str, Any]) -> None:
        # retCode=0 means success
        ret_code = response.get("retCode", -1)
        if ret_code != 0:
            msg = response.get("retMsg", "Unknown Bybit error")
            raise RuntimeError(f"Bybit API error {ret_code}: {msg}")
