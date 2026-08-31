# Bybit v5 quirks — hard-won, not in the official docs (or buried)

Everything here was confirmed by direct testing against live Bybit accounts
during the 2026-08-30/31 engagement. Don't "fix" behavior described here
assuming it's a bug in our code before checking this file — several hours
were lost re-discovering some of these before they were written down.

## 1. `reduceOnly` is enforced server-side, not by the flag you send

In hedge mode, if an order's `side` + `positionIdx` combination can only
reduce the existing position (e.g. Sell on positionIdx=1 with only a long
open, or Buy on positionIdx=2 with only a short open), Bybit's own risk
engine decides what happens — **your `reduceOnly` value is irrelevant**:

- If there's remaining "reduce budget" (position size minus the qty already
  claimed by other resting orders on that exact symbol+side+positionIdx),
  Bybit accepts the order and **silently sets `reduceOnly: true` on it**,
  even if your request never included that field, or explicitly sent
  `reduceOnly: false`. Confirmed on a batch of 105 orders placed with no
  `reduceOnly` key at all — every single one came back tagged `true`.
- If that reduce budget is already saturated, a **fresh** order is
  **rejected outright** — `InvalidRequestError: orderQty will be truncated
  to zero (ErrCode: 110017)`. This is not about qty, qtyStep, or minimum
  notional — it reproduces at any qty once the side's reduce budget is
  full. Official docs (Reduce-Only Order help article) confirm this
  explicitly: *"a reduce-only order can only be placed less than or equal
  to the existing open position's contract size, and all other reduce-only
  orders exceeding the existing position's contract size will be
  automatically reduced or cancelled."* This applies whether or not you
  asked for `reduceOnly` — Bybit classifies the order as reduce-only from
  its side+positionIdx alone.

**Practical consequence:** you cannot force a Bybit hedge-mode closing
order to *not* be reduce-only by omitting the flag. "Remove reduceOnly
from all orders" is not achievable via order parameters for orders that
can only close a position. If a client asks for this, the real fix is
almost never the flag — it's whatever is consuming the reduce budget
(duplicate/stale resting orders, an old bulk order that should have been
split up, etc.).

## 2. Conditional (trigger) orders are a different animal

A `Limit` order with `triggerPrice` set (an "Untriggered" order until the
trigger fires) behaves completely differently from a plain resting order
under the above quota:

- **It is exempt from the reduce-only quota entirely.** Confirmed: placed
  successfully and stayed `reduceOnly: false` on a side where a plain
  order was rejected with 110017 for the exact same qty. This is what
  makes conditional stop-loss orders viable even on accounts where the
  quota is fully saturated.
- **But it is accepted even against a position of size zero.** Unlike a
  plain reduce-only order (which Bybit rejects outright when there's no
  position — "In the absence of an open position... the system will
  automatically reject the placing of any reduce-only orders"), a
  conditional order sails through and just sits `Untriggered` forever,
  providing zero real protection while looking exactly like a
  successfully-placed order in your own bookkeeping.
  **Don't rely on a rejection to detect "position already closed."**
  Check the real position size (`get_position_size`) yourself *before*
  placing a conditional order, not after a rejection that will never come.

## 3. Duplicate `orderLinkId`

Rejected with `InvalidRequestError: OrderLinkedID is duplicate (ErrCode:
110072)`. Reuse the same `orderLinkId` on a resubmit and you get this,
not a silent success and not the original order's data back — you have to
look the existing order up yourself (`get_open_orders`/`get_order_history`
filtered by `orderLinkId`) if you want its info.

## 4. `get_open_orders` filtered by a specific `orderId`/`orderLinkId` ignores status

When you call `get_open_orders` **without** an id filter, it behaves as
advertised — only genuinely resting (`New`/`PartiallyFilled`) orders come
back. But filter it by a **specific** `orderId` or `orderLinkId`, and it
returns that record **regardless of its current status** — `Cancelled`
and `Filled` orders show up too, not just live ones. Confirmed directly:
an order with `orderStatus: "Cancelled"` came back from a `get_open_orders`
call filtered by its exact `orderId`.

**Practical consequence:** don't use `get_open_orders(orderId=X)` (or the
`orderLinkId` equivalent) as a "is this order currently live?" check —
check the `orderStatus` field in the response, not just whether the list
is non-empty. (For the general "what's currently resting" case, the
unfiltered listing is fine and behaves normally.)

## 5. pybit raises exceptions — `_raise_for_error` on a response dict doesn't always run

`pybit`'s HTTP client raises `pybit.exceptions.InvalidRequestError`
directly for most non-zero `retCode` responses (110017, 110072, etc.) —
the call itself throws, so code that does
`response = client.place_order(...); raise_for_error(response)` never
reaches the second line on these errors; the exception propagates out of
the whole call. Any code that wraps a raw pybit call — including one-off
scripts, not just the app's own exchange wrapper — needs a real
`try/except`, not just a post-hoc response-dict check. This bit an
operational script mid-batch (`cancel_order` raised uncaught) with no
actual damage, but it's an easy way to crash something unexpectedly.

`InvalidRequestError` carries the real code as `exc.status_code` (an int)
— match on that, not on string content in the message, which can change
wording between Bybit API versions.
