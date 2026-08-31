"""
Tests for exchanges/bybit.py's orderLinkId helpers.

is_closing_order backs webhook.py's net-delta risk cap — the control that's
supposed to trim directional exposure. It used to key off Bybit's reduceOnly
flag alone, which broke when Bybit started applying that flag inconsistently
depending on quota state at placement time (see BYBIT_QUIRKS.md #1). The more
dangerous of the two call sites: an order incorrectly included in the
"orders to cancel" list there gets CANCELLED — a bug there means the risk cap
could cancel a real take-profit or stop-loss order to trim exposure, which is
a worse outcome than the exposure it exists to limit.
"""
import pytest

from exchanges.bybit import build_order_link_id, is_closing_order


@pytest.mark.parametrize("order,expected,label", [
    ({"orderLinkId": "1787220720000_51_L_E", "reduceOnly": False}, False, "entry, no seq"),
    ({"orderLinkId": "1787220720000_51_L_CE", "reduceOnly": False}, False, "counter entry"),
    ({"orderLinkId": "1787220720000_51_L_TP", "reduceOnly": False}, True,
     "TP with reduceOnly False (the exact Bybit-quirk case)"),
    ({"orderLinkId": "1787220720000_51_L_TP2", "reduceOnly": False}, True, "TP retry sequence"),
    ({"orderLinkId": "1787220720000_51_L_CTP15", "reduceOnly": True}, True, "CTP high sequence"),
    ({"orderLinkId": "1787220720000_51_L_SL", "reduceOnly": False}, True,
     "conditional SL — never carries reduceOnly"),
    ({"orderLinkId": "sig1015_TP", "reduceOnly": True}, True, "fallback-tag TP (no of_id)"),
    ({"orderLinkId": "", "reduceOnly": True}, True, "legacy order, no tag, reduceOnly True"),
    ({"orderLinkId": "", "reduceOnly": False}, False, "legacy order, no tag, opening"),
    ({"orderLinkId": None, "reduceOnly": False}, False, "orderLinkId missing entirely"),
])
def test_is_closing_order(order, expected, label):
    assert is_closing_order(order) is expected, label


def test_build_order_link_id_format():
    assert build_order_link_id("1787220720000_51_L", "TP") == "1787220720000_51_L_TP"
