"""Pure arithmetic: windows, limits and what a reply costs. No disk, no network.

A key is any object with the fields of store.Key; these functions read and change it in place.
"""
import json
import os
from datetime import datetime, timedelta, timezone

# Per-plan token windows. The REAL limits are loaded from config and kept out of the public repo: set REDBEARD_PLANS
# (JSON) in the environment, or drop a plans.json beside this file. The values below are EXAMPLES only, so the open
# code runs and shows the metering logic without publishing the real allowances. Customers only ever see percentages.
_EXAMPLE_PLANS = {
    "starter": {"five_hour": 100_000, "weekly": 500_000},
    "power": {"five_hour": 300_000, "weekly": 1_500_000},
    "max": {"five_hour": 1_000_000, "weekly": 5_000_000},
}


def _load_plans():
    raw = os.environ.get("REDBEARD_PLANS")
    if raw:
        return json.loads(raw)
    path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "plans.json")
    try:
        with open(path, encoding="utf-8") as f:
            return json.load(f)
    except FileNotFoundError:
        return _EXAMPLE_PLANS


PLANS = _load_plans()
FIVE_HOURS = 5 * 3600
DAYS_PER_PAYMENT = 30
READ_DIVISOR = 8          # a token read counts 1/8 of a token written (API.md starting rule)
WRITE_MICROS = 10         # pay-as-you-go: $10 per million written = 10 millionths of a dollar each
READ_MICROS = 1           # pay-as-you-go: $1 per million read


def week_start(now: int) -> int:
    """Sunday 00:00 UTC on or before `now`."""
    day = datetime.fromtimestamp(now, timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0)
    return int((day - timedelta(days=(day.weekday() + 1) % 7)).timestamp())


def refresh(key, now: int) -> None:
    """Empty any window that has ended."""
    if key.fh_start is not None and now >= key.fh_start + FIVE_HOURS:
        key.fh_start, key.fh_used = None, 0
    if key.wk_start != week_start(now):
        key.wk_start, key.wk_used = week_start(now), 0


def blocked(key, now: int):
    """None if the key may start a reply, else (reason, resets_at). Call refresh first."""
    if key.plan == "payg":
        return ("balance", None) if key.balance_micros <= 0 else None
    if key.paid_until is None or now >= key.paid_until:
        return ("expired", key.paid_until)
    limits = PLANS[key.plan]
    if key.fh_start is not None and key.fh_used >= limits["five_hour"]:
        return ("five_hour", key.fh_start + FIVE_HOURS)
    if key.wk_used >= limits["weekly"]:
        return ("weekly", key.wk_start + 7 * 86400)
    return None


def counted(prompt_tokens: int, completion_tokens: int) -> int:
    return completion_tokens + prompt_tokens // READ_DIVISOR


def charge(key, prompt_tokens: int, completion_tokens: int, now: int) -> None:
    """Add a finished reply to the key: counters for plans, money for pay-as-you-go."""
    if key.plan == "payg":
        key.balance_micros -= completion_tokens * WRITE_MICROS + prompt_tokens * READ_MICROS
        return
    if key.fh_start is None:
        key.fh_start = now  # the 5-hour window starts at first use
    n = counted(prompt_tokens, completion_tokens)
    key.fh_used += n
    key.wk_used += n


def extend(key, now: int) -> None:
    """One payment: 30 more days, counted from today if the key had already run out."""
    key.paid_until = max(now, key.paid_until or 0) + DAYS_PER_PAYMENT * 86400
