"""
Tests for missing_tp.confirm_missing_tp — the one place that can actually
place a TP order now that main.py's 30s loop only notifies (2026-10-19).

Every check here re-verifies fresh against Bybit at the moment of
confirmation — it must never trust that whatever find_naked_entries (or a
Telegram message) said a moment ago is still true. Each test below covers
one specific way confirmation must refuse rather than place an order.
"""
import sqlite3
from unittest.mock import MagicMock

from tests.conftest import insert_signal

from missing_tp import confirm_missing_tp


def _set_columns(db_path: str, signal_id: int, **cols) -> None:
    conn = sqlite3.connect(db_path)
    for col, val in cols.items():
        conn.execute(f"UPDATE signals SET {col}=? WHERE id=?", (val, signal_id))
    conn.commit()
    conn.close()


def _get_tp_order_id(db_path: str, signal_id: int):
    conn = sqlite3.connect(db_path)
    row = conn.execute("SELECT tp_order_id FROM signals WHERE id=?", (signal_id,)).fetchone()
    conn.close()
    return row[0] if row else None


def _base_signal(tmp_db, **overrides):
    # conftest's insert_signal doesn't accept order_id/take_profit as
    # columns (not in its own INSERT statement) — set those separately
    # with a raw UPDATE after the row exists.
    order_id = overrides.pop("order_id", "entry-1")
    take_profit = overrides.pop("take_profit", 11.5)
    defaults = dict(
        action="buy", of_id="LINK-20261013-1500-0050m-L", quantity=2.0,
        symbol="LINKUSDT", category="linear", status="active",
    )
    defaults.update(overrides)
    sig_id = insert_signal(tmp_db, **defaults)
    _set_columns(tmp_db, sig_id, order_id=order_id, take_profit=take_profit)
    return sig_id


def _mock_exchange(position=None, open_orders=(), closing_execs=(), entry_history=None,
                    place_result=None, qty_step="0.1"):
    ex = MagicMock()
    ex.get_positions.return_value = [position] if position else []
    ex.get_all_open_orders.return_value = list(open_orders)
    ex.get_execution_history.return_value = list(closing_execs)
    ex._get_qty_step.return_value = qty_step
    ex.round_qty.side_effect = lambda qty, symbol, category="linear": qty
    ex._client.get_order_history.return_value = {
        "result": {"list": [entry_history] if entry_history else []}
    }
    ex.place_tp_order.return_value = place_result or {"order_id": "tp-new-1"}
    return ex


def test_happy_path_places_and_updates_db(tmp_db):
    sig_id = _base_signal(tmp_db, take_profit=11.5)
    position = {"symbol": "LINKUSDT", "side": "Buy", "size": "2.0", "positionIdx": 1, "markPrice": "11.2"}
    entry_hist = {"updatedTime": "1760000000000"}
    exchange = _mock_exchange(position=position, entry_history=entry_hist)

    result = confirm_missing_tp(exchange, sig_id)

    assert result["confirmed"] is True
    assert result["tp_order_id"] == "tp-new-1"
    exchange.place_tp_order.assert_called_once()
    kwargs = exchange.place_tp_order.call_args.kwargs
    assert kwargs["side"] == "Sell"
    assert kwargs["qty"] == 2.0
    assert kwargs["price"] == 11.5
    assert kwargs["order_link_id_base"] == "LINK-20261013-1500-0050m-L_TP"
    assert _get_tp_order_id(tmp_db, sig_id) == "tp-new-1"


def test_counter_signal_tagged_as_ctp(tmp_db):
    sig_id = _base_signal(tmp_db, action="sell", of_id="LINK-20261013-1500-0050m-S",
                           pattern_type="COUNTER", take_profit=11.5)
    position = {"symbol": "LINKUSDT", "side": "Sell", "size": "2.0", "positionIdx": 2, "markPrice": "11.9"}
    exchange = _mock_exchange(position=position, entry_history={"updatedTime": "1760000000000"})

    result = confirm_missing_tp(exchange, sig_id)

    assert result["confirmed"] is True
    kwargs = exchange.place_tp_order.call_args.kwargs
    assert kwargs["order_link_id_base"] == "LINK-20261013-1500-0050m-S_CTP"


def test_refuses_unknown_signal_id(tmp_db):
    exchange = _mock_exchange()
    result = confirm_missing_tp(exchange, 999999)
    assert result["confirmed"] is False
    exchange.place_tp_order.assert_not_called()


def test_refuses_untracked_symbol(tmp_db):
    sig_id = _base_signal(tmp_db, symbol="ILVUSDT", of_id="ILV-flow1", take_profit=5.5)
    exchange = _mock_exchange()
    result = confirm_missing_tp(exchange, sig_id)
    assert result["confirmed"] is False
    assert "not a tracked symbol" in result["reason"]
    exchange.place_tp_order.assert_not_called()


def test_refuses_non_active_status(tmp_db):
    sig_id = _base_signal(tmp_db, status="completed", take_profit=11.5)
    exchange = _mock_exchange()
    result = confirm_missing_tp(exchange, sig_id)
    assert result["confirmed"] is False
    assert "not active" in result["reason"]
    exchange.place_tp_order.assert_not_called()


def test_refuses_if_tp_order_id_already_set(tmp_db):
    """Someone else (or the signal's normal fill path) already handled this
    between detection and confirmation — must not double-place."""
    sig_id = _base_signal(tmp_db, take_profit=11.5)
    _set_columns(tmp_db, sig_id, tp_order_id="already-there")
    exchange = _mock_exchange()
    result = confirm_missing_tp(exchange, sig_id)
    assert result["confirmed"] is False
    assert "already" in result["reason"].lower()
    exchange.place_tp_order.assert_not_called()


def test_refuses_if_no_open_position(tmp_db):
    """The position itself is gone by confirm time — refuse rather than
    place a TP for nothing."""
    sig_id = _base_signal(tmp_db, take_profit=11.5)
    exchange = _mock_exchange(position=None)  # no positions at all
    result = confirm_missing_tp(exchange, sig_id)
    assert result["confirmed"] is False
    assert "no open position" in result["reason"].lower()
    exchange.place_tp_order.assert_not_called()


def test_refuses_if_price_already_passed_even_on_manual_confirm(tmp_db):
    """The core safety rule: even a human confirming must not be able to
    place a limit order whose price the market has already passed — it
    would fill instantly as a market order either way."""
    sig_id = _base_signal(tmp_db, take_profit=11.5)
    position = {"symbol": "LINKUSDT", "side": "Buy", "size": "2.0", "positionIdx": 1, "markPrice": "11.9"}
    exchange = _mock_exchange(position=position, entry_history={"updatedTime": "1760000000000"})

    result = confirm_missing_tp(exchange, sig_id)

    assert result["confirmed"] is False
    assert "already passed" in result["reason"]
    exchange.place_tp_order.assert_not_called()


def test_refuses_if_already_closed_by_someone(tmp_db):
    """Re-verified fresh at confirm time too — not just at detection time."""
    sig_id = _base_signal(tmp_db, take_profit=11.5)
    position = {"symbol": "LINKUSDT", "side": "Buy", "size": "2.0", "positionIdx": 1, "markPrice": "11.2"}
    closing_exec = {
        "orderId": "manual-close", "orderLinkId": "", "side": "Sell",
        "closedSize": "2.0", "execTime": "1760500000000", "symbol": "LINKUSDT",
    }
    exchange = _mock_exchange(
        position=position, entry_history={"updatedTime": "1760000000000"},
        closing_execs=[closing_exec],
    )

    result = confirm_missing_tp(exchange, sig_id)

    assert result["confirmed"] is False
    assert "already closed" in result["reason"].lower()
    exchange.place_tp_order.assert_not_called()


def test_refuses_if_would_over_cover_the_position(tmp_db):
    """Mohammad's rule 5: closing orders must never exceed the position."""
    sig_id = _base_signal(tmp_db, take_profit=11.5)
    position = {"symbol": "LINKUSDT", "side": "Buy", "size": "2.0", "positionIdx": 1, "markPrice": "11.2"}
    existing_tp = {
        "symbol": "LINKUSDT", "side": "Sell", "positionIdx": 1, "qty": "1.0",
        "orderLinkId": "some-other-flow_TP",
    }
    # 1.0 already resting + 2.0 about to be placed = 3.0 > position size 2.0
    exchange = _mock_exchange(
        position=position, open_orders=[existing_tp],
        entry_history={"updatedTime": "1760000000000"},
    )

    result = confirm_missing_tp(exchange, sig_id)

    assert result["confirmed"] is False
    assert "over the real position size" in result["reason"]
    exchange.place_tp_order.assert_not_called()


def test_refuses_if_no_take_profit_price_recorded(tmp_db):
    sig_id = insert_signal(tmp_db, action="buy", of_id="LINK-flow-x", quantity=2.0,
                            symbol="LINKUSDT", category="linear", status="active",
                            order_id="entry-x")
    exchange = _mock_exchange()
    result = confirm_missing_tp(exchange, sig_id)
    assert result["confirmed"] is False
    assert "take_profit" in result["reason"]
    exchange.place_tp_order.assert_not_called()


def test_refuses_if_no_entry_order_id(tmp_db):
    sig_id = insert_signal(tmp_db, action="buy", of_id="LINK-flow-y", quantity=2.0,
                            symbol="LINKUSDT", category="linear", status="active",
                            order_id=None)
    _set_columns(tmp_db, sig_id, take_profit=11.5)
    exchange = _mock_exchange()
    result = confirm_missing_tp(exchange, sig_id)
    assert result["confirmed"] is False
    assert "order_id" in result["reason"]
    exchange.place_tp_order.assert_not_called()
