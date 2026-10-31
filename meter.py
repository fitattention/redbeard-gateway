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
# Pay-as-you-go prices in millionths of a dollar per MILLION tokens: $5 written, $0.50 read, $0.05 re-read from the
# model's cache (prompt_tokens_details.cached_tokens: text it already holds costs it almost nothing to read again).
WRITE_PER_M = 5_000_000
READ_PER_M = 500_000
CACHED_PER_M = 50_000
# Plans count tokens with the same weights as those prices, so a plan's allowance is worth the same work in any mix.
READ_DIVISOR = WRITE_PER_M // READ_PER_M        # 10: a token read counts 1/10 of a token written
CACHED_DIVISOR = WRITE_PER_M // CACHED_PER_M    # 100: a token re-read from the cache counts 1/100


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


def plan_stop(key, now: int):
    """None if the key's monthly plan can pay for a reply now, else (reason, resets_at). Call refresh first."""
    if key.plan == "payg":
        return ("balance", None)
    if key.paid_until is None or now >= key.paid_until:
        return ("expired", key.paid_until)
    limits = PLANS[key.plan]
    if key.fh_start is not None and key.fh_used >= limits["five_hour"]:
        return ("five_hour", key.fh_start + FIVE_HOURS)
    if key.wk_used >= limits["weekly"]:
        return ("weekly", key.wk_start + 7 * 86400)
    return None


def blocked(key, now: int):
    """None if the key may start a reply, else (reason, resets_at). Call refresh first.
    The plan pays first; prepaid credit on the key (extra usage) pays when the plan can't: a limit reached, the plan
    ended, or no plan at all (pay-as-you-go). The reason given is the plan's, so a customer sees what to wait for."""
    stop = plan_stop(key, now)
    return None if stop is None or key.balance_micros > 0 else stop


def counted(prompt_tokens: int, completion_tokens: int, cached_tokens: int = 0) -> int:
    """A reply in plan units (one unit = one token written). cached_tokens is the part of prompt_tokens re-read."""
    cached = min(cached_tokens, prompt_tokens)
    return completion_tokens + (prompt_tokens - cached) // READ_DIVISOR + cached // CACHED_DIVISOR


def price_micros(prompt_tokens: int, completion_tokens: int, cached_tokens: int = 0) -> int:
    """A reply's pay-as-you-go price in millionths of a dollar, rounded up."""
    cached = min(cached_tokens, prompt_tokens)
    total = completion_tokens * WRITE_PER_M + (prompt_tokens - cached) * READ_PER_M + cached * CACHED_PER_M
    return -(-total // 1_000_000)


def charge(key, prompt_tokens: int, completion_tokens: int, now: int, cached_tokens: int = 0) -> None:
    """Add a finished reply to the key: the plan's counters while the plan can pay, else money off the credit."""
    if plan_stop(key, now) is not None:
        key.balance_micros -= price_micros(prompt_tokens, completion_tokens, cached_tokens)
        return
    if key.fh_start is None:
        key.fh_start = now  # the 5-hour window starts at first use
    n = counted(prompt_tokens, completion_tokens, cached_tokens)
    key.fh_used += n
    key.wk_used += n


def extend(key, now: int) -> None:
    """One payment: 30 more days, counted from today if the key had already run out."""
    key.paid_until = max(now, key.paid_until or 0) + DAYS_PER_PAYMENT * 86400
