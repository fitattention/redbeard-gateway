"""The key rows on disk (SQLite), and making and hashing keys. Nothing else about a customer is stored."""
import hashlib
import os
import secrets
import sqlite3
import string
import time
from dataclasses import astuple, dataclass, fields

ALPHABET = string.ascii_letters + string.digits
PLAN_IDS = ("starter", "power", "max", "payg")
DEFAULT_DB = os.environ.get("REDBEARD_DB", os.path.join(os.path.dirname(os.path.abspath(__file__)), "keys.db"))


@dataclass
class Key:
    key_hash: str
    plan: str
    paid_until: int | None = None
    balance_micros: int = 0
    balance_cap: int = 0        # pay-as-you-go: the balance right after the last top-up, so the panel can draw a bar
    fh_start: int | None = None
    fh_used: int = 0
    wk_start: int = 0
    wk_used: int = 0
    revoked: int = 0
    paid_micros: int = 0        # all money paid onto this key through checkout: a running total, never which payment


COLUMNS = ",".join(f.name for f in fields(Key))


def new_raw_key() -> str:
    """rb_ + 43 characters from 62: 62**43 is just over 2**256."""
    return "rb_" + "".join(secrets.choice(ALPHABET) for _ in range(43))


def hash_key(raw: str) -> str:
    return hashlib.sha256(raw.encode()).hexdigest()


def connect(path: str) -> sqlite3.Connection:
    conn = sqlite3.connect(path, isolation_level=None, check_same_thread=False)  # app.py serialises writes
    conn.execute(
        "CREATE TABLE IF NOT EXISTS keys ("
        " key_hash TEXT PRIMARY KEY,"
        f" plan TEXT NOT NULL CHECK (plan IN {PLAN_IDS}),"
        " paid_until INTEGER, balance_micros INTEGER NOT NULL,"
        " fh_start INTEGER, fh_used INTEGER NOT NULL,"
        " wk_start INTEGER NOT NULL, wk_used INTEGER NOT NULL,"
        " revoked INTEGER NOT NULL, balance_cap INTEGER NOT NULL DEFAULT 0, paid_micros INTEGER NOT NULL DEFAULT 0)"
    )
    # Columns added later; give them to any table made before them (DEFAULT fills existing rows with 0).
    have = {r[1] for r in conn.execute("PRAGMA table_info(keys)")}
    for column in ("balance_cap", "paid_micros"):
        if column not in have:
            conn.execute(f"ALTER TABLE keys ADD COLUMN {column} INTEGER NOT NULL DEFAULT 0")
    # An open crypto order links an invoice to a key fingerprint; checkout.py deletes it 24 hours after it was made.
    conn.execute(
        "CREATE TABLE IF NOT EXISTS orders ("
        " order_id TEXT PRIMARY KEY, key_hash TEXT NOT NULL, kind TEXT NOT NULL, plan TEXT,"
        " cents INTEGER NOT NULL, invoice_id TEXT NOT NULL, created INTEGER NOT NULL, done INTEGER NOT NULL DEFAULT 0)"
    )
    # Money the shop never matched to a sale (paid from another wallet than the one typed and credited by hand, or a
    # direct payment with no order): the books count it. Only the key's fingerprint's first 8 (its Key ID), never a key.
    conn.execute(
        "CREATE TABLE IF NOT EXISTS manual_sales ("
        " id INTEGER PRIMARY KEY, paid_at INTEGER NOT NULL, usd REAL NOT NULL, sent REAL, chain TEXT, tx TEXT,"
        " plan TEXT, kind TEXT NOT NULL, key_id TEXT, note TEXT, recorded INTEGER NOT NULL)"
    )
    return conn


MANUAL_COLUMNS = ("id", "paid_at", "usd", "sent", "chain", "tx", "plan", "kind", "key_id", "note", "recorded")


def add_manual_sale(conn, paid_at, usd, kind, plan=None, key_id=None, sent=None, chain=None, tx=None, note=None):
    """A sale the shop never counted; returns its row id."""
    return conn.execute("INSERT INTO manual_sales (paid_at, usd, sent, chain, tx, plan, kind, key_id, note, recorded)"
                        " VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                        (int(paid_at), float(usd), sent, chain, tx, plan, kind, key_id, note, int(time.time()))).lastrowid


def manual_sales(conn):
    """Every sale recorded by hand, newest first."""
    return [dict(zip(MANUAL_COLUMNS, r))
            for r in conn.execute(f"SELECT {', '.join(MANUAL_COLUMNS)} FROM manual_sales ORDER BY paid_at DESC, id DESC")]


def get(conn: sqlite3.Connection, key_hash: str) -> Key | None:
    row = conn.execute(f"SELECT {COLUMNS} FROM keys WHERE key_hash = ?", (key_hash,)).fetchone()
    return Key(*row) if row else None


def put(conn: sqlite3.Connection, key: Key) -> None:
    marks = ",".join("?" * len(fields(Key)))
    conn.execute(f"INSERT OR REPLACE INTO keys ({COLUMNS}) VALUES ({marks})", astuple(key))
