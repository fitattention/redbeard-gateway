"""Crypto checkout through Bitcart.

The customer's browser makes the key and sends only its fingerprint (SHA-256), so the raw key never reaches us.
Bitcart watches the chain; when it calls an invoice "complete", the fingerprint gets 30 days (or balance). We never
trust Bitcart's ping itself: every status check asks Bitcart directly. An order row (invoice <-> fingerprint) is
deleted a day after it was made once finished, a week after if unfinished (so support can still credit a late or short
payment with `admin.py credit ORDER_REF`). Settings: BITCART_API, BITCART_HOST_HEADER, BITCART_TOKEN, BITCART_STORE_ID,
REDBEARD_PUBLIC_BASE.
"""
import os
import re
import secrets
import time

import httpx
from fastapi import APIRouter, Request

import meter
import store

PRICES = {"starter": 5000, "power": 10000, "max": 20000}  # cents per 30 days; web/app/site.ts shows the same prices
TOPUP_MIN, TOPUP_MAX = 500, 100_000                        # pay-as-you-go top-up, cents ($5 to $1,000)
KEEP_ORDERS = 86400              # a finished order's invoice <-> key link: deleted after a day
KEEP_UNFINISHED = 7 * 86400      # an unfinished one (late or short payment) stays a week so support can fix it
NETWORKS = {"trx": "TRON", "bnb": "BNB Smart Chain", "eth": "Ethereum", "matic": "Polygon", "btc": "Bitcoin",
            "ltc": "Litecoin", "xmr": "Monero"}
FINGERPRINT = re.compile(r"^[0-9a-f]{64}$")
STORE_ID = os.environ.get("BITCART_STORE_ID", "")
PUBLIC_BASE = os.environ.get("REDBEARD_PUBLIC_BASE", "http://127.0.0.1:8041")
router = APIRouter()
_client = None


def gw():
    import app  # the gateway's shared helpers; imported late because app.py includes this router
    return app


def bitcart():
    global _client
    if _client is None:
        _client = httpx.AsyncClient(base_url=os.environ.get("BITCART_API", "http://127.0.0.1:10080"), timeout=30,
                                    headers={"Host": os.environ.get("BITCART_HOST_HEADER", ""),
                                             "Authorization": f"Bearer {os.environ.get('BITCART_TOKEN', '')}"})
    return _client


def view(inv, order, done=False):
    """What the checkout page needs: never the key, never the fingerprint."""
    status = inv.get("status")
    shown = ("paid" if done else "confirming" if status in ("paid", "confirmed", "complete")
             else "expired" if status in ("expired", "invalid", "refunded") else "waiting")
    methods = []
    for p in inv.get("payments") or []:
        if not p.get("payment_address"):
            continue
        chain = str(p.get("currency") or "").lower()          # the chain a token rides on, e.g. "trx" for USDT on TRON
        network = NETWORKS.get(chain, chain.upper())
        symbol = str(p.get("symbol") or chain).upper()
        token = bool(p.get("contract"))
        methods.append({"name": f"{symbol} · {network}" if token else (p.get("name") or symbol), "network": network,
                        "chain": chain, "symbol": symbol, "amount": p.get("amount"),
                        "address": p.get("payment_address"), "uri": p.get("payment_url"), "token": token,
                        "sender": p.get("user_address")})  # TRON/BNB: Bitcart only credits payments FROM this address
    return {"status": shown, "underpaid": inv.get("exception_status") == "paid_partial",
            "time_left": inv.get("time_left"), "price_usd": order["cents"] / 100,
            "kind": order["kind"], "plan": order["plan"], "methods": [] if done else methods}


def get_order(conn, order_id):
    row = conn.execute("SELECT order_id, key_hash, kind, plan, cents, invoice_id, created, done FROM orders"
                       " WHERE order_id = ?", (order_id,)).fetchone()
    keys = ("order_id", "key_hash", "kind", "plan", "cents", "invoice_id", "created", "done")
    return dict(zip(keys, row)) if row else None


def credit(conn, order_id):
    """Give the key what was paid for, exactly once. Synchronous, in one SQLite transaction."""
    conn.execute("BEGIN IMMEDIATE")
    try:
        o = get_order(conn, order_id)
        if o and not o["done"]:
            now = int(time.time())
            k = store.get(conn, o["key_hash"]) or store.Key(key_hash=o["key_hash"], plan=o["plan"] or "payg")
            meter.refresh(k, now)
            if o["kind"] == "plan":
                k.plan = o["plan"]  # paying for another plan switches to it; the days still stack
                meter.extend(k, now)
            else:
                k.balance_micros += o["cents"] * 10_000
                k.balance_cap = k.balance_micros   # a top-up fills the tank: the panel bar reads full again
            store.put(conn, k)
            conn.execute("UPDATE orders SET done = 1 WHERE order_id = ?", (order_id,))
        conn.execute("COMMIT")
    except Exception:
        conn.execute("ROLLBACK")
        raise


async def check(order_id):
    g = gw()
    o = get_order(g.conn(), order_id)
    if not o:
        return g.error(404, "unknown_order", "That order doesn't exist or has been cleared.")
    try:
        r = await bitcart().get(f"/invoices/{o['invoice_id']}")
        r.raise_for_status()
        inv = r.json()
    except (httpx.HTTPError, ValueError):
        return g.error(502, "payments_unavailable", "We can't reach the payment service right now. Your payment is safe.")
    if inv.get("status") == "complete":
        credit(g.conn(), order_id)
    o = get_order(g.conn(), order_id)
    return view(inv, o, done=bool(o["done"]))


@router.post("/v1/checkout")
async def create(request: Request):
    g = gw()
    if g.too_many("checkout", g.client_ip(request), 10):
        return g.error(429, "rate_limited", "Too many checkouts. Wait a minute.")
    try:
        body = await request.json()
        assert isinstance(body, dict)
    except (ValueError, AssertionError):
        return g.error(400, "bad_request", "The request body must be a JSON object.")
    key_hash, plan = str(body.get("key_hash", "")).lower(), body.get("plan")
    if not FINGERPRINT.match(key_hash):
        return g.error(400, "bad_fingerprint", "The key fingerprint must be 64 hex characters.")
    existing = store.get(g.conn(), key_hash)
    if existing and existing.revoked:
        return g.error(400, "key_revoked", "This key can't be renewed.")
    if plan in PRICES:
        if existing and existing.plan == "payg":
            return g.error(409, "wrong_kind", "Pay-as-you-go keys are topped up, not put on a plan.")
        kind, cents = "plan", PRICES[plan]
    elif plan == "payg":
        if existing and existing.plan != "payg":
            return g.error(409, "wrong_kind", "Plan keys are renewed, not topped up.")
        try:
            cents = round(float(body.get("amount_usd")) * 100)
        except (TypeError, ValueError):
            cents = 0
        if not TOPUP_MIN <= cents <= TOPUP_MAX:
            return g.error(400, "bad_amount", "Top up between $5 and $1,000.")
        kind = "topup"
    else:
        return g.error(400, "bad_plan", "Choose starter, power, max or payg.")

    now = int(time.time())
    g.conn().execute("DELETE FROM orders WHERE (done = 1 AND created < ?) OR created < ?",  # no lasting invoice <-> key link
                     (now - KEEP_ORDERS, now - KEEP_UNFINISHED))
    order_id = secrets.token_urlsafe(16)
    try:
        r = await bitcart().post("/invoices", json={
            "price": f"{cents / 100:.2f}", "store_id": STORE_ID, "order_id": order_id,
            "notification_url": f"{PUBLIC_BASE}/v1/checkout/{order_id}/notify",
            # for the back office's sales list: what was bought, and whether it renewed an existing key (never which)
            "metadata": {"kind": kind, "plan": plan if kind == "plan" else "payg", "renewal": existing is not None}})
        if r.status_code == 422 and "No wallet linked" in r.text:  # Bitcart's words while no wallet is connected
            return g.error(503, "payments_not_open",
                           "USDT payments open very soon: our wallets aren't connected yet. Nothing was charged.")
        r.raise_for_status()
        inv = r.json()
        assert inv.get("id")
    except (httpx.HTTPError, ValueError, AssertionError):
        return g.error(502, "payments_unavailable", "Payments are temporarily unavailable. Try again shortly.")
    g.conn().execute("INSERT INTO orders (order_id, key_hash, kind, plan, cents, invoice_id, created) VALUES"
                     " (?, ?, ?, ?, ?, ?, ?)", (order_id, key_hash, kind, plan if kind == "plan" else None, cents,
                                                inv["id"], now))
    return {"order_id": order_id, **view(inv, get_order(g.conn(), order_id))}


@router.get("/v1/checkout/{order_id}")
async def status(order_id: str):
    return await check(order_id)


EVM_ADDRESS = re.compile(r"^0x[0-9a-fA-F]{40}$")
SENDER_FORMAT = {"trx": re.compile(r"^T[1-9A-HJ-NP-Za-km-z]{33}$"), "bnb": EVM_ADDRESS, "matic": EVM_ADDRESS,
                 "eth": EVM_ADDRESS}


@router.post("/v1/checkout/{order_id}/sender")
async def sender(order_id: str, request: Request):
    """The address the customer will pay FROM. On TRON and BNB Smart Chain Bitcart matches a payment to an invoice by
    its sender (verified in Bitcart's genericprocessor.py), so without this nothing is ever credited. Set once."""
    g = gw()
    if g.too_many("sender", g.client_ip(request), 20):
        return g.error(429, "rate_limited", "Too many tries. Wait a minute.")
    o = get_order(g.conn(), order_id)
    if not o:
        return g.error(404, "unknown_order", "That order doesn't exist or has been cleared.")
    try:
        body = await request.json()
        chain, address = str(body.get("chain", "")).lower(), str(body.get("address", "")).strip()
    except (ValueError, AttributeError):
        return g.error(400, "bad_request", "The request body must be a JSON object.")
    if chain in SENDER_FORMAT and not SENDER_FORMAT[chain].match(address):
        return g.error(400, "bad_address", "That doesn't look like a wallet address on this network.")
    try:
        r = await bitcart().get(f"/invoices/{o['invoice_id']}")
        r.raise_for_status()
        inv = r.json()
        method = next((p for p in inv.get("payments") or [] if str(p.get("currency", "")).lower() == chain), None)
        if method is None:
            return g.error(400, "bad_network", "This order can't be paid on that network.")
        r = await bitcart().patch(f"/invoices/{o['invoice_id']}/details", json={"id": method["id"], "address": address})
    except (httpx.HTTPError, ValueError, KeyError):
        return g.error(502, "payments_unavailable", "We can't reach the payment service right now. Try again shortly.")
    if r.status_code == 422 and "once set" in r.text:
        return g.error(409, "sender_already_set", "The paying address for this order is already set.")
    if r.status_code == 422:
        return g.error(400, "bad_address", "That address isn't valid on this network, or this order is no longer open.")
    if r.status_code >= 400:
        return g.error(502, "payments_unavailable", "We can't reach the payment service right now. Try again shortly.")
    return await check(order_id)


@router.post("/v1/checkout/{order_id}/notify")
async def notify(order_id: str):
    """Bitcart's ping. Its content is ignored: check() asks Bitcart for the real status."""
    result = await check(order_id)
    return result if not isinstance(result, dict) else {"ok": True}
