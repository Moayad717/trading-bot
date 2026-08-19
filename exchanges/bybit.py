from __future__ import annotations

from typing import Any, Dict, List

from pybit.unified_trading import HTTP

from config import settings
from exchanges.base import BaseExchange
from models.signal import Action, OrderType, SignalCreate


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

    def place_order(self, signal: SignalCreate) -> Dict[str, Any]:
        side = "Buy" if signal.action == Action.BUY else "Sell"

        params: Dict[str, Any] = {
            "category": signal.category,
            "symbol": signal.symbol,
            "side": side,
            "orderType": signal.order_type.value.capitalize(),
            "qty": str(signal.quantity),
            "positionIdx": 1 if side == "Buy" else 2,
        }

        if signal.order_type == OrderType.LIMIT:
            if signal.price is None:
                raise ValueError("price is required for limit orders")
            params["price"] = str(signal.price)
            params["timeInForce"] = "GTC"

        if signal.stop_loss is not None:
            params["stopLoss"] = str(signal.stop_loss)

        response: Any = self._client.place_order(**params)
        self._raise_for_error(response)

        result = response.get("result", {})
        return {
            "order_id": result.get("orderId", ""),
            "status": result.get("orderStatus", ""),
            "raw": response,
        }

    def place_tp_order(
        self,
        symbol: str,
        side: str,
        qty: float,
        price: float,
        position_idx: int,
        category: str = "linear",
    ) -> Dict[str, Any]:
        params: Dict[str, Any] = {
            "category": category,
            "symbol": symbol,
            "side": side,
            "orderType": "Limit",
            "qty": str(qty),
            "price": str(price),
            "reduceOnly": True,
            "timeInForce": "GTC",
            "positionIdx": position_idx,
        }
        response: Any = self._client.place_order(**params)
        self._raise_for_error(response)
        result = response.get("result", {})
        return {
            "order_id": result.get("orderId", ""),
            "status": result.get("orderStatus", ""),
            "raw": response,
        }

    def place_market_close_order(
        self,
        symbol: str,
        side: str,
        qty: float,
        position_idx: int,
        category: str = "linear",
    ) -> Dict[str, Any]:
        """Place a reduce-only Market order to close an open position immediately."""
        params: Dict[str, Any] = {
            "category": category,
            "symbol": symbol,
            "side": side,
            "orderType": "Market",
            "qty": str(qty),
            "reduceOnly": True,
            "positionIdx": position_idx,
        }
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
