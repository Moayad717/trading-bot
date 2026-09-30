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

v2 tag format (2026-09-30): the Pine-side of_id now carries the entry and
take-profit price ("YYMMDD-HHMM-TTTT-EEEEE-XXXXX"), and "TP" was renamed to
"ETP" so every field lines up in the same position across all four roles
(E/ETP/C/CTP). "_ETP" does NOT match the v1 regex (underscore followed by
TP/CTP/SL/CLOSE) — there's no "_TP" substring, the character before "TP" is
"E" not "_" — so
every entry's take-profit would have silently stopped being recognised as
closing-only. That's exactly the shape of the real 2026-09-16 incident (the
reconciler read a covered position as naked and placed a duplicate TP), just
guaranteed to fire on every entry instead of one. v1 "_TP" is kept in the
alternation permanently: v1-tagged orders keep resting on the exchange after
the switchover and must keep being recognised.
"""
import pytest

from exchanges.bybit import _MAX_SUFFIX, build_order_link_id, is_closing_order


@pytest.mark.parametrize("order,expected,label", [
    ({"orderLinkId": "1787220720000_51_L_E", "reduceOnly": False}, False, "v1 entry, no seq"),
    ({"orderLinkId": "1787220720000_51_L_CE", "reduceOnly": False}, False, "v1 counter entry"),
    ({"orderLinkId": "1787220720000_51_L_TP", "reduceOnly": False}, True,
     "v1 TP with reduceOnly False (the exact Bybit-quirk case)"),
    ({"orderLinkId": "1787220720000_51_L_TP2", "reduceOnly": False}, True, "v1 TP retry sequence"),
    ({"orderLinkId": "1787220720000_51_L_CTP15", "reduceOnly": True}, True, "v1 CTP high sequence"),
    ({"orderLinkId": "1787220720000_51_L_SL", "reduceOnly": False}, True,
     "v1 conditional SL — never carries reduceOnly"),
    ({"orderLinkId": "sig1015_TP", "reduceOnly": True}, True, "fallback-tag TP (no of_id)"),
    ({"orderLinkId": "", "reduceOnly": True}, True, "legacy order, no tag, reduceOnly True"),
    ({"orderLinkId": "", "reduceOnly": False}, False, "legacy order, no tag, opening"),
    ({"orderLinkId": None, "reduceOnly": False}, False, "orderLinkId missing entirely"),
    # v2 — the price-carrying format, "TP" renamed to "ETP"
    ({"orderLinkId": "260924-2215-0047-13390-13500_E", "reduceOnly": False}, False, "v2 entry"),
    ({"orderLinkId": "260924-2215-0047-13390-13500_C", "reduceOnly": False}, False, "v2 counter entry"),
    ({"orderLinkId": "260924-2215-0047-13390-13500_ETP", "reduceOnly": False}, True,
     "v2 entry TP — the exact rename that broke the old regex"),
    ({"orderLinkId": "260924-2215-0047-13390-13500_ETP2", "reduceOnly": False}, True,
     "v2 entry TP retry sequence"),
    ({"orderLinkId": "260924-2215-0047-13390-13500_CTP", "reduceOnly": True}, True, "v2 counter TP"),
])
def test_is_closing_order(order, expected, label):
    assert is_closing_order(order) is expected, label


def test_build_order_link_id_format():
    assert build_order_link_id("1787220720000_51_L", "TP") == "1787220720000_51_L_TP"
    assert (build_order_link_id("260924-2215-0047-13390-13500", "CTP")
            == "260924-2215-0047-13390-13500_CTP")


def test_build_order_link_id_rejects_over_length_of_id():
    # 36 - _MAX_SUFFIX(8) = 28 is the longest of_id that can never overflow
    # even with the worst-case "_CLOSE20" suffix.
    ok_of_id = "x" * (36 - _MAX_SUFFIX)
    assert len(build_order_link_id(ok_of_id, "E")) <= 36

    too_long = "x" * (36 - _MAX_SUFFIX + 1)
    with pytest.raises(ValueError, match="36-char limit"):
        build_order_link_id(too_long, "E")
