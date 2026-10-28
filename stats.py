"""Anonymous hourly totals for the whole service, for the owner's back office. No key, no fingerprint, no word of
content: a row is one hour x plan x source (the website chat or the API) and holds only counts and sums.

Written as things happen (one small UPSERT, like the key counters). "At once" (replies streaming at the same moment)
and "keys" (how many different keys were active in the hour) are worked out in memory and stored as numbers only.
A stats failure never costs a customer a reply: every write swallows its own errors.
File: REDBEARD_STATS_DB (default /var/lib/redbeard/stats.db), a separate file from the keys. Rows older than
KEEP_DAYS are deleted.
"""
import os
import sqlite3
import time

PATH = os.environ.get("REDBEARD_STATS_DB", "/var/lib/redbeard/stats.db")
KEEP_DAYS = 400
SPEED_BAR = 40          # tokens a second per reply: the owner's line for "fast"; slower replies are counted as slow
MIN_FOR_SPEED = 20      # replies shorter than this give no fair speed reading
STALE = 900             # a reply open longer than the gateway's read timeout is gone, whatever happened to it
COUNTS = ("requests", "prompt_tokens", "completion_tokens", "gen_tokens", "gen_ms", "timed", "wait_ms", "waited", "slow",
          "cut", "failed", "limit_5h", "limit_week", "expired", "empty", "searches", "fetches", "upstream_micros")
REFUSAL = {"five_hour": "limit_5h", "weekly": "limit_week", "expired": "expired", "balance": "empty"}

_db = None
_hour, _keys, _open, _next = 0, set(), {}, 0   # memory only: this hour's keys, and the replies streaming right now


def connect(path):
    c = sqlite3.connect(path, isolation_level=None, check_same_thread=False)
    c.execute("CREATE TABLE IF NOT EXISTS hourly (hour INTEGER NOT NULL, plan TEXT NOT NULL, source TEXT NOT NULL, "
              + ", ".join(f"{n} INTEGER NOT NULL DEFAULT 0" for n in COUNTS) + ", PRIMARY KEY (hour, plan, source))")
    c.execute("CREATE TABLE IF NOT EXISTS load (hour INTEGER PRIMARY KEY, at_once INTEGER NOT NULL, keys INTEGER NOT NULL)")
    columns = {row[1] for row in c.execute("PRAGMA table_info(hourly)")}
    if "upstream_micros" not in columns:
        c.execute("ALTER TABLE hourly ADD COLUMN upstream_micros INTEGER NOT NULL DEFAULT 0")
    return c


def db():
    global _db
    if _db is None:
        _db = connect(PATH)
    return _db


def hour_of(ts):
    return int(ts) // 3600 * 3600


def source(request):
    """A browser sends Origin on a POST; API clients (opencode, scripts) don't. Used only to split the totals."""
    return "chat" if request.headers.get("origin") else "api"


def add(plan, src, now=None, **counts):
    try:
        h = hour_of(now if now is not None else time.time())
        names = [n for n in counts if n in COUNTS]
        db().execute(f"INSERT INTO hourly (hour, plan, source, {', '.join(names)}) VALUES (?, ?, ?{', ?' * len(names)})"
                     f" ON CONFLICT(hour, plan, source) DO UPDATE SET {', '.join(f'{n} = {n} + excluded.{n}' for n in names)}",
                     (h, plan, src, *(int(counts[n]) for n in names)))
    except (sqlite3.Error, TypeError, ValueError):
        pass


def start(key_hash, now=None):
    """A reply began. Returns a ticket for end()."""
    global _hour, _next
    now = time.time() if now is None else now
    h = hour_of(now)
    if h != _hour:
        _hour = h
        _keys.clear()
        try:
            db().execute("DELETE FROM hourly WHERE hour < ?", (h - KEEP_DAYS * 86400,))
            db().execute("DELETE FROM load WHERE hour < ?", (h - KEEP_DAYS * 86400,))
        except sqlite3.Error:
            pass
    _keys.add(key_hash)
    for t in [t for t, began in _open.items() if began < now - STALE]:  # self-heal a reply that never reported back
        del _open[t]
    _next += 1
    _open[_next] = now
    try:
        db().execute("INSERT INTO load (hour, at_once, keys) VALUES (?, ?, ?) ON CONFLICT(hour) DO UPDATE SET"
                     " at_once = max(at_once, excluded.at_once), keys = max(keys, excluded.keys)",
                     (h, len(_open), len(_keys)))
    except sqlite3.Error:
        pass
    return _next


def end(ticket):
    _open.pop(ticket, None)


def finish(plan, src, prompt, completion, waited=None, writing=None, cut=False, now=None):
    """One reply done. waited: seconds to its first piece; writing: seconds from the first piece to the end."""
    prompt, completion = max(0, int(prompt or 0)), max(0, int(completion or 0))
    c = {"requests": 1, "prompt_tokens": prompt, "completion_tokens": completion, "cut": int(cut)}
    if waited is not None:
        c.update(wait_ms=round(waited * 1000), waited=1)
    if writing and writing > 0 and completion >= MIN_FOR_SPEED and not cut:
        c.update(gen_tokens=completion, gen_ms=round(writing * 1000), timed=1, slow=int(completion / writing < SPEED_BAR))
    add(plan, src, now, **c)


def rows(since):
    cols = ("hour", "plan", "source", *COUNTS)
    hourly = [dict(zip(cols, r)) for r in db().execute(f"SELECT {', '.join(cols)} FROM hourly WHERE hour >= ?"
                                                       " ORDER BY hour", (since,))]
    load = [dict(zip(("hour", "at_once", "keys"), r))
            for r in db().execute("SELECT hour, at_once, keys FROM load WHERE hour >= ? ORDER BY hour", (since,))]
    return hourly, load
