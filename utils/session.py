from __future__ import annotations

from datetime import datetime, timezone
from typing import List
from zoneinfo import ZoneInfo

_BEIRUT = ZoneInfo("Asia/Beirut")

# Canonical session names used across the codebase — order matters for display.
SESSIONS: List[str] = [
    "Sydney",
    "Tokyo",
    "Pre-London",
    "London",
    "London/NY",
    "New York",
]


def classify_sessions(beirut_naive: datetime) -> List[str]:
    """Return every trading session active at the given naive Beirut datetime.

    Session windows are defined in UTC:
        Sydney      22:00 – 07:00  (crosses midnight)
        Tokyo       00:00 – 09:00
        Pre-London  06:00 – 08:00
        London      07:00 – 16:00
        London/NY   12:00 – 16:00
        New York    12:00 – 21:00

    Returns ["Off-Hours"] when no session is active.
    """
    utc = beirut_naive.replace(tzinfo=_BEIRUT).astimezone(timezone.utc)
    h = utc.hour

    result: List[str] = []
    if h >= 22 or h < 7:    result.append("Sydney")
    if 0 <= h < 9:          result.append("Tokyo")
    if 6 <= h < 8:          result.append("Pre-London")
    if 7 <= h < 16:         result.append("London")
    if 12 <= h < 16:        result.append("London/NY")
    if 12 <= h < 21:        result.append("New York")
    return result or ["Off-Hours"]
