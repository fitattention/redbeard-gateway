"""The key rows on disk (SQLite), and making and hashing keys. Nothing else about a customer is stored."""
import hashlib
import os
import secrets
import sqlite3
import string
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
        " revoked INTEGER NOT NULL, balance_cap INTEGER NOT NULL DEFAULT 0)"
    )
    # balance_cap was added later; give it to any table made before it (DEFAULT fills existing rows with 0).
    if not any(r[1] == "balance_cap" for r in conn.execute("PRAGMA table_info(keys)")):
        conn.execute("ALTER TABLE keys ADD COLUMN balance_cap INTEGER NOT NULL DEFAULT 0")
    # An open crypto order links an invoice to a key fingerprint; checkout.py deletes it 24 hours after it was made.
    conn.execute(
        "CREATE TABLE IF NOT EXISTS orders ("
        " order_id TEXT PRIMARY KEY, key_hash TEXT NOT NULL, kind TEXT NOT NULL, plan TEXT,"
        " cents INTEGER NOT NULL, invoice_id TEXT NOT NULL, created INTEGER NOT NULL, done INTEGER NOT NULL DEFAULT 0)"
    )
    return conn


def get(conn: sqlite3.Connection, key_hash: str) -> Key | None:
    row = conn.execute(f"SELECT {COLUMNS} FROM keys WHERE key_hash = ?", (key_hash,)).fetchone()
    return Key(*row) if row else None


def put(conn: sqlite3.Connection, key: Key) -> None:
    marks = ",".join("?" * len(fields(Key)))
    conn.execute(f"INSERT OR REPLACE INTO keys ({COLUMNS}) VALUES ({marks})", astuple(key))
