from __future__ import annotations

from typing import Any, Dict

from pybit.unified_trading import HTTP

from config import settings
from exchanges.base import BaseExchange
from models.signal import Action, OrderType, SignalCreate


class BybitExchange(BaseExchange):
    """
    Bybit Unified Trading adapter using pybit v5.
    Switches between testnet and live via settings.TESTNET.
    """

    def __init__(self) -> None:
        self._client = HTTP(
            testnet=settings.TESTNET,
            api_key=settings.BYBIT_API_KEY,
            api_secret=settings.BYBIT_API_SECRET,
        )

    @property
    def name(self) -> str:
        return "bybit"

    # ------------------------------------------------------------------
    # BaseExchange implementation
    # ------------------------------------------------------------------

    def place_order(self, signal: SignalCreate) -> Dict[str, Any]:
        side = "Buy" if signal.action == Action.BUY else "Sell"

        params: Dict[str, Any] = {
            "category": signal.category,
            "symbol": signal.symbol,
            "side": side,
            "orderType": signal.order_type.value.capitalize(),
            "qty": str(signal.quantity),
        }

        if signal.order_type == OrderType.LIMIT:
            if signal.price is None:
                raise ValueError("price is required for limit orders")
            params["price"] = str(signal.price)
            params["timeInForce"] = "GTC"

        response: Any = self._client.place_order(**params)
        self._raise_for_error(response)

        result = response.get("result", {})
        return {
            "order_id": result.get("orderId", ""),
            "status": result.get("orderStatus", ""),
            "raw": response,
        }

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

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _raise_for_error(response: Dict[str, Any]) -> None:
        """Bybit returns retCode=0 on success. Raise on anything else."""
        ret_code = response.get("retCode", -1)
        if ret_code != 0:
            msg = response.get("retMsg", "Unknown Bybit error")
            raise RuntimeError(f"Bybit API error {ret_code}: {msg}")
