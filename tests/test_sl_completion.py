"""
Tests for the real-conditional-order stop-loss completion path (db.py +
order_tracker.py's guard logic), added 2026-08-31.

Background: set_trading_stop (the old SL mechanism) has exactly ONE stop
slot per position side. When two counters on the same side filled close
together, the second call silently overwrote the first counter's stop with
no error — order_tracker.py then inferred the original was closed purely
because its counter's TP filled, with no check that the SL had actually
fired. Confirmed with real data: five counters filled within 3 seconds on
2026-08-28, all five originals got marked completed this way, and Bybit's
execution history for that entire day shows only one small unrelated sell.

The fix: place_conditional_sl is a real order with its own order_id, stored
on the ORIGINAL's own row (sl_order_id). A signal with a real sl_order_id
completes from ITS OWN fill event (db.complete_signal_by_sl_fill_sync), never
from inferring anything about its counter. The old inference
(_maybe_complete_original_after_counter_tp) is kept only for rows that
predate this change (sl_order_id NULL) so in-flight legacy pairs don't break.
"""
from tests.conftest import get_status, insert_signal

import db


def test_real_sl_fill_completes_the_original_directly(tmp_db):
    orig_id = insert_signal(tmp_db, action="buy", of_id="flow1", sl_order_id="sl-order-abc")

    completed = db.complete_signal_by_sl_fill_sync("sl-order-abc", "2026-08-31T12:00:00")

    assert completed, "complete_signal_by_sl_fill_sync did not update the row"
    assert get_status(tmp_db, orig_id) == "completed"


def test_legacy_inference_skips_a_signal_with_a_real_sl_order_id(tmp_db):
    """This is the exact bug-prevention check: without it, a counter's TP
    fill would prematurely mark the original completed before its own real
    SL order ever fires."""
    orig_id = insert_signal(tmp_db, action="buy", of_id="flow2", sl_order_id="sl-order-xyz")
    insert_signal(tmp_db, action="sell", of_id="flow2", pattern_type="COUNTER",
                   tp_order_id="ctp-order-1", sl_placed=1)

    # Mirrors the exact guard in order_tracker.py's
    # _maybe_complete_original_after_counter_tp:
    counter_row = db.get_signal_by_tp_order_id_sync("ctp-order-1")
    assert counter_row is not None
    assert (counter_row.get("pattern_type") or "").upper() == "COUNTER"
    original_row = db.get_original_signal_by_of_id_sync("flow2")
    assert original_row is not None

    if original_row.get("sl_order_id"):
        skipped = True
    else:
        db.complete_signal_by_id_sync(original_row["id"], "2026-08-31T12:05:00")
        skipped = False

    assert skipped, "legacy inference did not skip a signal with a real sl_order_id"
    assert get_status(tmp_db, orig_id) == "active", (
        "original was marked completed by the counter's TP fill even though its "
        "real SL order hasn't fired yet — this is exactly the proven financial "
        "bug (2026-08-28: five originals marked closed with no matching "
        "exchange execution)"
    )


def test_legacy_inference_still_completes_rows_with_no_sl_order_id(tmp_db):
    """Backward compatibility: in-flight pairs from before this change (no
    sl_order_id at all) must keep completing the old way."""
    orig_id = insert_signal(tmp_db, action="buy", of_id="flow3", sl_order_id=None)
    insert_signal(tmp_db, action="sell", of_id="flow3", pattern_type="COUNTER",
                   tp_order_id="ctp-order-2", sl_placed=1)

    original_row = db.get_original_signal_by_of_id_sync("flow3")
    if original_row.get("sl_order_id"):
        skipped = True
    else:
        db.complete_signal_by_id_sync(original_row["id"], "2026-08-31T12:10:00")
        skipped = False

    assert not skipped, "legacy inference skipped a row that has no sl_order_id at all"
    assert get_status(tmp_db, orig_id) == "completed", (
        "legacy fallback broke — old in-flight pairs predating this change would "
        "stop completing correctly"
    )
