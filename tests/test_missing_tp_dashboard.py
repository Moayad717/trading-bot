"""
Tests for the dashboard's missing-TP endpoints (2026-10-19):
  GET  /dashboard/missing-tps                — read-only report
  POST /dashboard/missing-tps/{id}/confirm    — the only place that can
                                                 actually place an order

Thin wrappers around missing_tp.find_naked_entries / confirm_missing_tp,
already covered thoroughly in tests/test_missing_tp_detection.py and
tests/test_missing_tp_confirm.py — these just check the routes wire
through correctly and don't swallow/misreport errors.
"""
import asyncio
import sqlite3
from unittest.mock import MagicMock, patch

from tests.conftest import insert_signal

from dashboard.routes import confirm_missing_tp_route, missing_tps


def _set_columns(db_path: str, signal_id: int, **cols) -> None:
    conn = sqlite3.connect(db_path)
    for col, val in cols.items():
        conn.execute(f"UPDATE signals SET {col}=? WHERE id=?", (val, signal_id))
    conn.commit()
    conn.close()


def test_missing_tps_report_empty_when_nothing_naked(tmp_db):
    exchange = MagicMock()
    exchange.get_positions.return_value = []
    with patch("exchanges.bybit.BybitExchange", return_value=exchange):
        result = asyncio.run(missing_tps())
    assert result == {"candidates": [], "count": 0}


def test_missing_tps_report_returns_real_candidate(tmp_db):
    sig_id = insert_signal(tmp_db, action="buy", of_id="LINK-20261013-1500-0050m-L",
                            quantity=2.0, symbol="LINKUSDT", category="linear", status="active")
    _set_columns(tmp_db, sig_id, order_id="entry-1", take_profit=11.5)

    exchange = MagicMock()
    exchange.get_positions.return_value = [
        {"symbol": "LINKUSDT", "side": "Buy", "size": "2.0", "positionIdx": 1, "markPrice": "11.2"}
    ]
    exchange.get_all_open_orders.return_value = []
    exchange.get_execution_history.return_value = []
    exchange._client.get_order_history.return_value = {"result": {"list": [{
        "orderId": "entry-1", "orderLinkId": "LINK-20261013-1500-0050m-L_E",
        "symbol": "LINKUSDT", "side": "Buy", "positionIdx": 1, "qty": "2.0",
        "orderStatus": "Filled", "reduceOnly": False, "updatedTime": "1700000000000",
    }], "nextPageCursor": ""}}
    exchange.round_qty.side_effect = lambda qty, symbol, category="linear": qty
    exchange._get_qty_step.return_value = "0.1"

    with patch("exchanges.bybit.BybitExchange", return_value=exchange):
        result = asyncio.run(missing_tps())

    assert result["count"] == 1
    assert result["candidates"][0]["signal_id"] == sig_id


def test_missing_tps_report_handles_exchange_error(tmp_db):
    with patch("exchanges.bybit.BybitExchange", side_effect=RuntimeError("Bybit down")):
        result = asyncio.run(missing_tps())
    assert "error" in result
    assert result["candidates"] == []


def test_confirm_route_places_and_returns_confirmed(tmp_db):
    sig_id = insert_signal(tmp_db, action="buy", of_id="LINK-20261013-1500-0050m-L",
                            quantity=2.0, symbol="LINKUSDT", category="linear", status="active")
    _set_columns(tmp_db, sig_id, order_id="entry-1", take_profit=11.5)

    exchange = MagicMock()
    exchange.get_positions.return_value = [
        {"symbol": "LINKUSDT", "side": "Buy", "size": "2.0", "positionIdx": 1, "markPrice": "11.2"}
    ]
    exchange.get_all_open_orders.return_value = []
    exchange.get_execution_history.return_value = []
    exchange._client.get_order_history.return_value = {"result": {"list": [
        {"updatedTime": "1700000000000"}
    ]}}
    exchange.round_qty.side_effect = lambda qty, symbol, category="linear": qty
    exchange._get_qty_step.return_value = "0.1"
    exchange.place_tp_order.return_value = {"order_id": "tp-confirmed-1"}

    with patch("exchanges.bybit.BybitExchange", return_value=exchange):
        result = asyncio.run(confirm_missing_tp_route(sig_id))

    assert result["confirmed"] is True
    assert result["tp_order_id"] == "tp-confirmed-1"


def test_confirm_route_returns_refusal_without_raising(tmp_db):
    """A refusal (e.g. unknown signal) is a normal 200-shaped response, not
    an exception — the caller needs the reason, not a stack trace."""
    exchange = MagicMock()
    with patch("exchanges.bybit.BybitExchange", return_value=exchange):
        result = asyncio.run(confirm_missing_tp_route(999999))

    assert result["confirmed"] is False
    assert "reason" in result
