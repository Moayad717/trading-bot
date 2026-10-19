"""
Tests for main.py's _fix_missing_tps() — the background loop that finds
entries missing their own take-profit and tells a human about it.

Background (2026-10-19): this used to place TPs automatically. Two real
incidents (10-13, 10-16) happened when it placed a brand-new TP for an
entry a human had already closed by hand with an untagged order — both
filled instantly against unrelated trades, losing real money (confirmed
-5.02 USDT on 10-16). Placement is now manual-only: this loop only
notifies via Telegram, once per signal while the gap persists; the actual
detection logic (with the new safety checks) lives in missing_tp.py and is
tested separately in tests/test_missing_tp_detection.py and
tests/test_missing_tp_confirm.py. These tests cover only this loop's own
job — calling that detection, deduping notifications, and never placing
anything itself.
"""
import asyncio
from unittest.mock import MagicMock, patch

import main


def _make_sleep_stopper(after: int):
    """Async sleep replacement: lets `after` calls through, then raises to
    break out of the loop's `while True` deterministically."""
    calls = {"n": 0}

    async def _sleep(_seconds):
        calls["n"] += 1
        if calls["n"] > after:
            raise asyncio.CancelledError("test: stopping loop after N iterations")

    return _sleep


async def _run_n_cycles(n: int, exchange_mock, candidates_per_cycle):
    """candidates_per_cycle: list of candidate-lists, one per cycle (so a
    test can show the same or a changed picture across cycles)."""
    main._missing_tp_notified.clear()  # module-level state — must not leak between tests
    calls = {"n": -1}

    def _find(_exchange):
        calls["n"] += 1
        idx = min(calls["n"], len(candidates_per_cycle) - 1)
        return candidates_per_cycle[idx]

    with patch("main.asyncio.sleep", new=_make_sleep_stopper(n)), \
         patch("main.BybitExchange", return_value=exchange_mock), \
         patch("main.find_naked_entries", side_effect=_find), \
         patch("main.send_telegram") as mock_telegram:
        try:
            await main._fix_missing_tps()
        except asyncio.CancelledError:
            pass
    return mock_telegram


def _candidate(signal_id, needs_manual_handling=False, **overrides):
    base = {
        "signal_id": signal_id, "of_id": f"flow{signal_id}", "symbol": "LINKUSDT",
        "side": "Sell", "qty": 2.0, "price": 11.5, "pattern_type": None,
        "entry_order_id": f"entry-{signal_id}", "entry_updated_time": "1700000000000",
        "needs_manual_handling": needs_manual_handling,
    }
    base.update(overrides)
    return base


def _bare_exchange():
    ex = MagicMock()
    ex.get_positions.return_value = []
    ex.get_order_history.return_value = []
    return ex


def test_new_candidate_sends_one_telegram_message(tmp_db):
    candidate = _candidate(1)
    mock_telegram = asyncio.run(_run_n_cycles(1, _bare_exchange(), [[candidate]]))

    mock_telegram.assert_called_once()
    text = mock_telegram.call_args[0][0]
    assert "sig_id=1" in text
    assert "flow1" in text
    assert "confirm" in text.lower()


def test_same_candidate_is_not_re_notified_across_cycles(tmp_db):
    candidate = _candidate(1)
    mock_telegram = asyncio.run(_run_n_cycles(3, _bare_exchange(), [[candidate], [candidate], [candidate]]))

    assert mock_telegram.call_count == 1, "must notify once while the gap persists, not every cycle"


def test_resolved_candidate_can_notify_again_if_it_recurs(tmp_db):
    candidate = _candidate(1)
    # cycle 1: present, cycle 2: gone (resolved), cycle 3: present again
    mock_telegram = asyncio.run(_run_n_cycles(3, _bare_exchange(), [[candidate], [], [candidate]]))

    assert mock_telegram.call_count == 2


def test_two_candidates_in_one_cycle_send_two_separate_messages(tmp_db):
    c1, c2 = _candidate(1), _candidate(2)
    mock_telegram = asyncio.run(_run_n_cycles(1, _bare_exchange(), [[c1, c2]]))

    assert mock_telegram.call_count == 2
    texts = [call[0][0] for call in mock_telegram.call_args_list]
    assert any("sig_id=1" in t for t in texts)
    assert any("sig_id=2" in t for t in texts)


def test_needs_manual_handling_gets_a_different_message(tmp_db):
    candidate = _candidate(1, needs_manual_handling=True, price=12.78366)
    mock_telegram = asyncio.run(_run_n_cycles(1, _bare_exchange(), [[candidate]]))

    mock_telegram.assert_called_once()
    text = mock_telegram.call_args[0][0]
    assert "already passed" in text
    # must not invite a confirm action that will just be refused
    assert "/dashboard/missing-tps" not in text


def test_fix_missing_tps_never_places_an_order_itself(tmp_db):
    """The structural guarantee this whole rewrite exists for: this loop
    has no way to place anything anymore — not even indirectly."""
    candidate = _candidate(1)
    exchange = _bare_exchange()
    asyncio.run(_run_n_cycles(1, exchange, [[candidate]]))

    assert not hasattr(exchange, "place_tp_order") or not exchange.place_tp_order.called


def test_cancelled_tp_warning_only_considers_tracked_symbols(tmp_db, caplog):
    """A cancelled/rejected reduceOnly order on an untracked symbol (e.g. a
    user's own manual ILVUSDT position) must never even be logged about —
    this loop should not be looking at that symbol at all."""
    exchange = _bare_exchange()
    exchange.get_positions.return_value = [
        {"symbol": "LINKUSDT", "side": "Sell", "size": "2.0", "positionIdx": 2},
        {"symbol": "ILVUSDT", "side": "Buy", "size": "70.56", "positionIdx": 1},
    ]
    exchange.get_order_history.return_value = [
        {"orderId": "o1", "symbol": "LINKUSDT", "reduceOnly": True, "orderStatus": "Cancelled"},
        {"orderId": "o2", "symbol": "ILVUSDT", "reduceOnly": True, "orderStatus": "Cancelled"},
    ]

    main._cancelled_tp_warned.clear()
    with caplog.at_level("WARNING"):
        asyncio.run(_run_n_cycles(1, exchange, [[]]))

    warnings = [r.message for r in caplog.records if "TP detected" in r.message]
    assert len(warnings) == 1
    assert "LINKUSDT" in warnings[0]
    assert "ILVUSDT" not in warnings[0]


def test_cancelled_tp_warning_logs_once_per_order_id(tmp_db, caplog):
    exchange = _bare_exchange()
    exchange.get_positions.return_value = [
        {"symbol": "LINKUSDT", "side": "Sell", "size": "2.0", "positionIdx": 2},
    ]
    exchange.get_order_history.return_value = [
        {"orderId": "o1", "symbol": "LINKUSDT", "reduceOnly": True, "orderStatus": "Cancelled"},
    ]

    main._cancelled_tp_warned.clear()
    with caplog.at_level("WARNING"):
        asyncio.run(_run_n_cycles(3, exchange, [[], [], []]))

    warnings = [r for r in caplog.records if "Cancelled TP detected" in r.message]
    assert len(warnings) == 1


def test_detection_error_does_not_crash_the_cancelled_tp_check(tmp_db, caplog):
    """The two try/except blocks are split on purpose — a failure finding
    naked entries must not also silence the (unrelated) cancelled-TP
    warning in the same cycle."""
    exchange = _bare_exchange()
    exchange.get_positions.return_value = [
        {"symbol": "LINKUSDT", "side": "Sell", "size": "2.0", "positionIdx": 2},
    ]
    exchange.get_order_history.return_value = [
        {"orderId": "o1", "symbol": "LINKUSDT", "reduceOnly": True, "orderStatus": "Cancelled"},
    ]

    main._cancelled_tp_warned.clear()
    with patch("main.asyncio.sleep", new=_make_sleep_stopper(1)), \
         patch("main.BybitExchange", return_value=exchange), \
         patch("main.find_naked_entries", side_effect=RuntimeError("boom")), \
         patch("main.send_telegram"), \
         caplog.at_level("WARNING"):
        try:
            asyncio.run(main._fix_missing_tps())
        except asyncio.CancelledError:
            pass

    assert any("Cancelled TP detected" in r.message for r in caplog.records)
    assert any("Fix-TP loop error" in r.message for r in caplog.records)
