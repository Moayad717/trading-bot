"""Shared detection + confirmation logic for entries missing their own
take-profit (2026-10-19) — used by both main.py's 30-second notifier and
the dashboard's read-only report/confirm endpoints, so every caller sees
the exact same live truth, computed fresh from Bybit every time. Nothing
here caches or remembers state between calls.

Background — two real incidents this replaces automatic placement for:

  10-13: Fix-TP (then a different, older mechanism) placed brand-new TPs
  for two signals Mohammad had already closed by hand with his own partial
  TPs days earlier. Those new TPs filled against OTHER, unrelated long
  positions, consuming 6.9 LINK from them and triggering Bybit's own
  CancelByReduceOnly to cancel one of Mohammad's own resting orders.

  10-16: the same shape of mistake recurred for two different signals,
  this time via a database cleanup (clearing a dead tp_order_id reference)
  that put them back into "active, no TP" without checking whether the
  position itself had already been closed by an untagged order. Both
  fired instantly as market orders, again eating into unrelated trades.
  Cost: -5.02 USDT, confirmed from the live pm2 log.

  Both incidents share one root cause: the old check only asked "is there
  a TAGGED closing order for this entry" — which is structurally blind to
  anything a human closes by hand with an untagged order. find_naked_entries
  below adds a real check against Bybit's own execution history instead,
  tagged or not.

  A second, separate issue found the same week: the old mechanism scanned
  every symbol with an open position on the account, including a user's
  own unrelated manual positions on other coins sharing the same Bybit
  account (ILV/WAL/BLUR). find_naked_entries is scoped to
  settings.FIX_TP_SYMBOLS only.

No automatic placement happens anywhere in this module. find_naked_entries
only ever reports; confirm_missing_tp is the one place that can actually
place an order, and only when called explicitly (the dashboard's POST
confirm endpoint), re-verifying everything fresh at that moment rather
than trusting whatever find_naked_entries said a moment — or a Telegram
message — ago.
"""
from __future__ import annotations

import re
from typing import Any, Dict, List, TYPE_CHECKING

from config import settings
from db import (
    get_signal_by_order_id_sync,
    get_signal_for_tp_confirm_sync,
    set_tp_order_id_sync,
)
from exchanges.bybit import build_order_link_id, is_closing_order

if TYPE_CHECKING:
    from exchanges.bybit import BybitExchange

_CLOSING_TAG_RE = re.compile(r"^(.*)_(?:ETP|CTP|CLOSE|TP|SL)\d*$")
_ENTRY_TAG_RE = re.compile(r"^(.*)_(?:CE|E)$")
_SL_STOP_ORDER_TYPES = {"StopLoss", "PartialStopLoss"}
_HISTORY_PAGES = 6


def tracked_symbols() -> set[str]:
    return {s.strip() for s in settings.FIX_TP_SYMBOLS.split(",") if s.strip()}


def _price_passed(pos_side: str, mark_price: float, tp_price: float) -> bool:
    """True if the market has already gone past where this TP would sit —
    placing a resting limit order there would fill instantly as a market
    order instead of waiting for the price, which defeats the purpose of a
    take-profit and is exactly what caused real losses on 10-13 and 10-16.
    A long's TP (Sell) is passed once price has risen to/through it; a
    short's TP (Buy) is passed once price has fallen to/through it."""
    if pos_side == "Buy":
        return mark_price >= tp_price
    return mark_price <= tp_price


def _already_closed_by_someone(
    closing_execs: List[Dict[str, Any]],
    tp_side: str,
    entry_fill_ms: int,
    qty: float,
    qty_tolerance: float,
    used_exec_ids: set,
) -> bool:
    """A real closing execution for roughly this entry's own quantity,
    after its own fill time, already exists on Bybit — checked against
    actual executions, not just resting/historical ORDERS, so an untagged
    manual close (which carries no tag the old check could ever see) is
    caught too. used_exec_ids prevents one real execution from being
    claimed as "proof" for two different entries in the same scan."""
    for ex in closing_execs:
        order_id = ex.get("orderId")
        if not order_id or order_id in used_exec_ids:
            continue
        if ex.get("side") != tp_side:
            continue
        try:
            if int(ex.get("execTime", 0)) <= entry_fill_ms:
                continue
        except (TypeError, ValueError):
            continue
        try:
            if abs(float(ex.get("closedSize", 0)) - qty) > qty_tolerance:
                continue
        except (TypeError, ValueError):
            continue
        used_exec_ids.add(order_id)
        return True
    return False


def find_naked_entries(exchange: "BybitExchange") -> List[Dict[str, Any]]:
    """Scan every tracked symbol's open position and return entries that
    genuinely still need their own TP. Never places anything. Each result
    has needs_manual_handling=True when the price has already passed —
    confirm_missing_tp refuses those unconditionally, no matter who asks."""
    tracked = tracked_symbols()
    positions = exchange.get_positions(category="linear", settle_coin="USDT")
    active = [
        p for p in positions
        if float(p.get("size", 0)) > 0 and p.get("symbol") in tracked
    ]
    if not active:
        return []

    open_orders = exchange.get_all_open_orders(category="linear", settle_coin="USDT")
    candidates: List[Dict[str, Any]] = []

    for pos in active:
        symbol       = pos["symbol"]
        pos_side     = pos["side"]
        size         = float(pos["size"])
        position_idx = 1 if pos_side == "Buy" else 2
        tp_side      = "Sell" if pos_side == "Buy" else "Buy"
        try:
            mark_price = float(pos.get("markPrice") or 0)
        except (TypeError, ValueError):
            mark_price = 0.0

        covered_qty = sum(
            float(o.get("qty", 0))
            for o in open_orders
            if o.get("symbol") == symbol
            and o.get("side") == tp_side
            and o.get("positionIdx") == position_idx
            and not str(o.get("orderLinkId", "")).endswith("_SL")
            and o.get("stopOrderType") not in _SL_STOP_ORDER_TYPES
        )
        naked_qty = round(size - covered_qty, 3)
        if naked_qty <= 0:
            continue

        history: List[Dict[str, Any]] = []
        cursor = ""
        for _ in range(_HISTORY_PAGES):
            params: Dict[str, Any] = {"category": "linear", "symbol": symbol, "limit": 50}
            if cursor:
                params["cursor"] = cursor
            resp = exchange._client.get_order_history(**params)
            result = resp.get("result", {})
            history.extend(result.get("list", []))
            cursor = result.get("nextPageCursor", "")
            if not cursor:
                break

        tp_of_ids: set[str] = set()
        for o in open_orders + history:
            if is_closing_order(o) and o.get("orderLinkId"):
                m = _CLOSING_TAG_RE.match(o["orderLinkId"])
                if m:
                    tp_of_ids.add(m.group(1))

        entries = [
            o for o in history
            if o.get("side") == pos_side
            and o.get("positionIdx") == position_idx
            and o.get("orderStatus") == "Filled"
            and not o.get("reduceOnly")
        ]

        closing_execs = exchange.get_execution_history(symbol=symbol, category="linear", limit=200)
        try:
            qty_tolerance = float(exchange._get_qty_step(symbol)) * 1.01
        except Exception:
            qty_tolerance = 0.01
        used_exec_ids: set = set()

        seen: set[str] = set()
        for o in entries:
            if naked_qty <= 0:
                break

            m = _ENTRY_TAG_RE.match(o.get("orderLinkId") or "")
            if not m:
                continue  # untagged / pre-dates tagging — never guessed
            of_id = m.group(1)
            if of_id in seen or of_id in tp_of_ids:
                continue
            seen.add(of_id)

            entry_order_id = o.get("orderId", "")
            row = get_signal_by_order_id_sync(entry_order_id)
            if row is None or row.get("take_profit") is None:
                continue

            tp_price = float(row["take_profit"])
            category = row.get("category", "linear")
            fill_qty = exchange.round_qty(float(o.get("qty", 0)), symbol, category)
            if fill_qty <= 0:
                continue

            try:
                entry_fill_ms = int(o.get("updatedTime", 0) or 0)
            except (TypeError, ValueError):
                entry_fill_ms = 0

            if _already_closed_by_someone(
                closing_execs, tp_side, entry_fill_ms, fill_qty, qty_tolerance, used_exec_ids
            ):
                continue  # a human (or anything else) already closed this — never guessed otherwise

            naked_qty = round(naked_qty - fill_qty, 8)

            candidates.append({
                "signal_id":             row["id"],
                "of_id":                 of_id,
                "symbol":                symbol,
                "side":                  tp_side,
                "qty":                   fill_qty,
                "price":                 tp_price,
                "pattern_type":          row.get("pattern_type"),
                "entry_order_id":        entry_order_id,
                "entry_updated_time":    o.get("updatedTime"),
                "needs_manual_handling": _price_passed(pos_side, mark_price, tp_price),
            })

    return candidates


def confirm_missing_tp(exchange: "BybitExchange", signal_id: int) -> Dict[str, Any]:
    """The only place in this module that can place an order. Re-verifies
    everything fresh — never trusts a find_naked_entries result that might
    be seconds or minutes old. Refuses, with a clear reason, if anything
    no longer checks out."""
    sig = get_signal_for_tp_confirm_sync(signal_id)
    if sig is None:
        return {"confirmed": False, "reason": "No signal with that id."}

    if sig["symbol"] not in tracked_symbols():
        return {"confirmed": False, "reason": f"{sig['symbol']} is not a tracked symbol — refusing."}
    if sig["status"] != "active":
        return {"confirmed": False, "reason": f"Signal status is '{sig['status']}', not active — refusing."}
    if sig["tp_order_id"]:
        return {"confirmed": False, "reason": "Signal already has a tp_order_id — refusing (already handled)."}
    if sig["take_profit"] is None:
        return {"confirmed": False, "reason": "Signal has no recorded take_profit price — refusing."}
    if not sig["order_id"]:
        return {"confirmed": False, "reason": "Signal has no entry order_id on record — refusing."}

    action = (sig["action"] or "").lower()
    pos_side     = "Buy" if action == "buy" else "Sell"
    position_idx = 1 if pos_side == "Buy" else 2
    tp_side      = "Sell" if pos_side == "Buy" else "Buy"
    symbol       = sig["symbol"]
    category     = sig.get("category") or "linear"

    positions = exchange.get_positions(category="linear", settle_coin="USDT")
    pos = next(
        (p for p in positions if p.get("symbol") == symbol and p.get("side") == pos_side),
        None,
    )
    if pos is None or float(pos.get("size", 0)) <= 0:
        return {"confirmed": False, "reason": "No open position on that side anymore — refusing."}

    try:
        entry_fill_ms = 0
        entry_hist = exchange._client.get_order_history(
            category="linear", symbol=symbol, orderId=sig["order_id"]
        )
        lst = entry_hist.get("result", {}).get("list", [])
        if lst:
            entry_fill_ms = int(lst[0].get("updatedTime", 0) or 0)
    except Exception:
        entry_fill_ms = 0

    tp_price = float(sig["take_profit"])
    qty = exchange.round_qty(float(sig["quantity"]), symbol, category)
    if qty <= 0:
        return {"confirmed": False, "reason": "Rounded quantity is zero — refusing."}

    try:
        mark_price = float(pos.get("markPrice") or 0)
    except (TypeError, ValueError):
        mark_price = 0.0
    if _price_passed(pos_side, mark_price, tp_price):
        return {
            "confirmed": False,
            "reason": f"Price has already passed {tp_price} (mark={mark_price}) — a limit "
                      "order here would fill instantly, not rest as a real TP. This needs "
                      "manual handling on Bybit directly, not a normal TP placement.",
        }

    closing_execs = exchange.get_execution_history(symbol=symbol, category="linear", limit=200)
    try:
        qty_tolerance = float(exchange._get_qty_step(symbol)) * 1.01
    except Exception:
        qty_tolerance = 0.01
    if _already_closed_by_someone(closing_execs, tp_side, entry_fill_ms, qty, qty_tolerance, set()):
        return {
            "confirmed": False,
            "reason": "A closing execution for roughly this quantity already happened on "
                      "Bybit since the entry filled — this looks already closed, refusing "
                      "to place a duplicate.",
        }

    open_orders = exchange.get_all_open_orders(category="linear", settle_coin="USDT")
    covered_qty = sum(
        float(o.get("qty", 0))
        for o in open_orders
        if o.get("symbol") == symbol
        and o.get("side") == tp_side
        and o.get("positionIdx") == position_idx
        and not str(o.get("orderLinkId", "")).endswith("_SL")
        and o.get("stopOrderType") not in _SL_STOP_ORDER_TYPES
    )
    size = float(pos["size"])
    if covered_qty + qty > size + qty_tolerance:
        return {
            "confirmed": False,
            "reason": f"Placing this would push total closing coverage ({covered_qty + qty}) "
                      f"over the real position size ({size}) — refusing rather than risk "
                      "Bybit trimming someone else's order to make room.",
        }

    role = "CTP" if (sig.get("pattern_type") or "").upper() == "COUNTER" else "TP"
    result = exchange.place_tp_order(
        symbol=symbol,
        side=tp_side,
        qty=qty,
        price=tp_price,
        position_idx=position_idx,
        category=category,
        order_link_id_base=build_order_link_id(sig["of_id"], role) if sig["of_id"] else None,
    )
    tp_order_id = result.get("order_id", "")
    if not tp_order_id:
        return {"confirmed": False, "reason": f"Placement returned no order_id: {result}"}

    set_tp_order_id_sync(sig["order_id"], tp_order_id)

    return {"confirmed": True, "tp_order_id": tp_order_id, "symbol": symbol, "qty": qty, "price": tp_price}
