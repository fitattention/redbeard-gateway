"""The owner's back office: GET /v1/admin/overview (everything on one page) and POST /v1/admin/run (admin.py's own
commands, so the page and the server command can never disagree).

One admin key, kept only as its SHA-256 in REDBEARD_ADMIN_HASH; while that is unset every admin route answers 404.
It reads what already exists and keeps the halves apart as they are on disk: sales come from Bitcart (the gateway's
own invoice token, nothing new), keys from the key table, usage from stats.py's anonymous hourly totals; a sale is
never joined to a key. Wallet balances are read from the public chains, so this server never holds Bitcart's wallet
access (which could change the address customers pay into). Something it can't read shows as null, never as 0.
Settings: REDBEARD_ADMIN_HASH, REDBEARD_GPU_HOURLY (dollars an hour for the GPU box: the cost estimate).
"""
import asyncio
import hashlib
import hmac
import os
import time
from collections import Counter
from dataclasses import replace
from datetime import datetime, timedelta, timezone
from statistics import median

import httpx
from fastapi import APIRouter, Request
from fastapi.responses import JSONResponse

import admin
import checkout
import meter
import stats
import store

router = APIRouter()
ADMIN_HASH = os.environ.get("REDBEARD_ADMIN_HASH", "").strip().lower()
GPU_HOURLY = float(os.environ.get("REDBEARD_GPU_HOURLY") or 0)
GPU_BILLING_START = float(os.environ.get("REDBEARD_GPU_BILLING_START") or 0)
GPU_RUNNING_SINCE = float(os.environ.get("REDBEARD_GPU_RUNNING_SINCE") or 0)
GPU_STORAGE_HOURLY = float(os.environ.get("REDBEARD_GPU_STORAGE_HOURLY") or 0)
LEGACY_GPU_HOURLY = float(os.environ.get("REDBEARD_LEGACY_GPU_HOURLY") or 0)
API_MODE = os.environ.get("REDBEARD_INFERENCE_MODE") == "api"
RETIRED_COMPONENT = os.environ.get("REDBEARD_RETIRED_COMPONENT", "sherlock-gpu")
COMMANDS = ("new", "extend", "topup", "revoke", "credit")
PLAN_BY_PRICE = {cents / 100: plan for plan, cents in checkout.PRICES.items()}  # sales made before metadata: guessed
RPC = {"bnb": "https://bsc-rpc.publicnode.com", "matic": "https://polygon-bor-rpc.publicnode.com",
       "eth": "https://ethereum-rpc.publicnode.com"}
TRONGRID = "https://api.trongrid.io/v1/accounts/"
DAY = 86400
_wallets = (0.0, [])  # (when read, rows): public chains are asked at most every two minutes


def gate(request):
    """None for the holder of the admin key, else the answer to send."""
    g = checkout.gw()
    if not ADMIN_HASH:
        return JSONResponse({"detail": "Not Found"}, status_code=404)
    if g.too_many("admin", g.client_ip(request), 30):
        return g.error(429, "rate_limited", "Too many requests. Wait a minute.")
    raw = g.bearer(request) or ""
    if not hmac.compare_digest(hashlib.sha256(raw.encode()).hexdigest(), ADMIN_HASH):
        return g.error(401, "invalid_key", "That admin key isn't valid.")
    return None


def explorer(chain, what, value):
    """A public explorer link for a transaction ("tx") or an address."""
    if chain == "trx":
        return f"https://tronscan.org/#/{'transaction' if what == 'tx' else 'address'}/{value}"
    host = {"bnb": "bscscan.com", "matic": "polygonscan.com", "eth": "etherscan.io"}.get(chain)
    return f"https://{host}/{what}/{value}" if host else None


def ts(text):
    return datetime.fromisoformat(text.replace("Z", "+00:00")).timestamp() if text else None


async def invoices():
    """Every Bitcart invoice, newest first. Raises httpx.HTTPError or ValueError when Bitcart can't be read."""
    out = []
    for page in range(50):  # ponytail: 5,000 sales; ask Bitcart by date range once the business outgrows it
        r = await checkout.bitcart().get("/invoices", params={"limit": 100, "offset": page * 100, "sort": "created",
                                                              "desc": "true"})
        r.raise_for_status()
        batch = r.json().get("result") or []
        out += batch
        if len(batch) < 100:
            return out
    return out


def sale(inv):
    paid = next((p for p in inv.get("payments") or [] if p.get("is_used")), None)
    chain = str((paid or {}).get("currency") or "").lower()
    meta = inv.get("metadata") or {}
    usd = float(inv.get("price") or 0)
    tx = (inv.get("tx_hashes") or [None])[0]
    return {"id": inv.get("id"), "order": inv.get("order_id"), "created": inv.get("created"),
            "paid_at": inv.get("paid_date"), "status": inv.get("status"),
            "underpaid": inv.get("exception_status") == "paid_partial", "usd": usd,
            "sent": float(inv.get("sent_amount") or 0), "plan": meta.get("plan") or PLAN_BY_PRICE.get(usd, "payg"),
            "renewal": meta.get("renewal"), "guessed": "plan" not in meta,
            "network": checkout.NETWORKS.get(chain, chain.upper()) if chain else None,
            "tx": tx, "tx_url": explorer(chain, "tx", tx) if tx and chain else None}


async def balance(chain, address, contract):
    """USDT at one address, read from that chain's public record."""
    c = checkout.gw().client()
    if chain == "trx":
        r = await c.get(TRONGRID + address, timeout=10)
        r.raise_for_status()
        account = (r.json().get("data") or [{}])[0]
        units = sum(int(v) for t in account.get("trc20", []) for k, v in t.items() if k == contract)
        return units / 10 ** 6  # ponytail: TRON USDT's 6 decimals; another TRC-20 token would need its own

    async def call(data):
        r = await c.post(RPC[chain], json={"jsonrpc": "2.0", "id": 1, "method": "eth_call",
                                           "params": [{"to": contract, "data": data}, "latest"]}, timeout=10)
        r.raise_for_status()
        return int(r.json()["result"], 16)
    units = await call("0x70a08231" + address[2:].lower().rjust(64, "0"))  # balanceOf(address)
    return units / 10 ** await call("0x313ce567")                          # decimals()


async def wallets(newest):
    """The receiving addresses on the newest invoice (so a wallet change shows by itself), with their USDT."""
    global _wallets
    if time.time() - _wallets[0] < 120:
        return _wallets[1]

    async def one(p):
        chain, address = str(p.get("currency") or "").lower(), p["payment_address"]
        try:
            usdt = round(await balance(chain, address, p.get("contract")), 2)
        except Exception:  # one chain's reader being down must not hide the others
            usdt = None
        return {"network": checkout.NETWORKS.get(chain, chain.upper()), "chain": chain, "address": address,
                "usdt": usdt, "url": explorer(chain, "address", address)}
    rows = list(await asyncio.gather(*(one(p) for p in (newest or {}).get("payments") or []
                                       if p.get("payment_address") and p.get("contract"))))
    _wallets = (time.time(), rows)
    return rows


def keys(conn, now):
    out = []
    for row in conn.execute(f"SELECT {store.COLUMNS} FROM keys"):
        k = store.Key(*row)
        v = replace(k)
        meter.refresh(v, now)  # on a copy: looking at a key never changes it
        payg = k.plan == "payg"
        live = k.balance_micros > 0 if payg else bool(k.paid_until and now < k.paid_until)
        limits = meter.PLANS.get(k.plan) or {}
        pct = lambda used, cap: round(min(100.0, 100 * used / cap), 1) if cap else None
        out.append({
            "fp": k.key_hash, "plan": k.plan,
            "status": "revoked" if k.revoked else "active" if live else "empty" if payg else "ended",
            "paid_until": checkout.gw().iso(k.paid_until),
            "days_left": None if payg or k.paid_until is None else round((k.paid_until - now) / DAY, 1),
            "five_hour_pct": None if payg else pct(v.fh_used, limits.get("five_hour")),
            "week_pct": None if payg else pct(v.wk_used, limits.get("weekly")),
            "balance_usd": round(k.balance_micros / 1e6, 2) if payg else None,
            "balance_left_pct": pct(k.balance_micros, max(k.balance_cap, k.balance_micros)) if payg else None})
    return out


def gpu_hours(since):
    """Hours the model answered, from the /status recorder's five-minute samples (an estimate of box time)."""
    if GPU_BILLING_START:
        return round(max(0, time.time() - max(since, GPU_RUNNING_SINCE or time.time())) / 3600, 1)
    g = checkout.gw()
    day = datetime.fromtimestamp(since, timezone.utc).strftime("%Y-%m-%d")
    names = list(g.UPSTREAMS)
    try:
        up = g.uptime_db().execute("SELECT COALESCE(SUM(up), 0) FROM uptime WHERE day >= ? AND component = ?",
                                   (day, RETIRED_COMPONENT if API_MODE else names[0] if names else "")).fetchone()[0]
    except Exception:
        return None
    return round(up * 300 / 3600, 1)


def retired_gpu_hours(since):
    """Read frozen pre-API GPU samples without reclassifying API uptime as GPU time."""
    day = datetime.fromtimestamp(since, timezone.utc).strftime("%Y-%m-%d")
    try:
        up = checkout.gw().uptime_db().execute(
            "SELECT COALESCE(SUM(up), 0) FROM uptime WHERE day >= ? AND component = ?",
            (day, RETIRED_COMPONENT)).fetchone()[0]
        return up * 300 / 3600
    except Exception:
        return None


def summary(sales, key_rows, hourly, hours, now):
    month = now - 30 * DAY
    out = {"revenue": None, "funnel": None}
    paid = [s for s in sales or [] if s["status"] == "complete"]
    if sales is not None:
        out["revenue"] = {name: round(sum(s["usd"] for s in paid if days is None or ts(s["paid_at"] or s["created"]) >= now - days * DAY), 2)
                          for name, days in (("24h", 1), ("7d", 7), ("30d", 30), ("all", None))}
        recent = [s for s in sales if ts(s["created"]) >= month]
        waits = [(ts(s["paid_at"]) - ts(s["created"])) / 60 for s in recent if s["status"] == "complete" and s["paid_at"]]
        out["funnel"] = {"started": len(recent), "paid": sum(s["status"] == "complete" for s in recent),
                         "expired": sum(s["status"] == "expired" for s in recent),
                         "underpaid": sum(s["underpaid"] for s in recent),
                         "minutes_to_pay": round(median(waits), 1) if waits else None}
    active = Counter(k["plan"] for k in key_rows if k["status"] == "active")
    out["keys"] = {"active": dict(active), "ending_7d": sum(k["status"] == "active" and k["days_left"] is not None
                                                            and k["days_left"] <= 7 for k in key_rows),
                   "ended": sum(k["status"] == "ended" for k in key_rows),
                   "payg_owed_usd": round(sum(k["balance_usd"] or 0 for k in key_rows if k["status"] == "active"), 2)}
    legacy = round(hours * GPU_HOURLY, 2) if hours is not None and GPU_HOURLY else None
    api_cost = sum(r.get("upstream_micros", 0) for r in hourly) / 1e6 if API_MODE else 0
    cost = round((legacy or 0) + api_cost, 2) if API_MODE else legacy
    current_gpu = None
    if GPU_BILLING_START:
        # Storage bills during image loading; GPU time starts when the instance runs.
        storage_hours = max(0, now - max(month, GPU_BILLING_START)) / 3600
        running_hours = max(0, now - max(month, GPU_RUNNING_SINCE or now)) / 3600
        current_gpu = storage_hours * GPU_STORAGE_HOURLY + running_hours * max(0, GPU_HOURLY - GPU_STORAGE_HOURLY)
        frozen_hours = retired_gpu_hours(month)
        legacy = round(frozen_hours * LEGACY_GPU_HOURLY, 2) if frozen_hours is not None else None
        api_cost = sum(r.get("upstream_micros", 0) for r in hourly) / 1e6
        cost = round(current_gpu + (legacy or 0) + api_cost, 2)
    out["gpu"] = {"hours_30d": hours, "hourly": GPU_HOURLY or None, "cost_30d": cost,
                  "margin_30d": round(out["revenue"]["30d"] - cost, 2) if out["revenue"] and cost is not None else None}
    if API_MODE:
        out["gpu"].update(mode="api", api_cost_30d=round(api_cost, 6), legacy_gpu_cost_30d=legacy)
    elif GPU_BILLING_START:
        out["gpu"].update(mode="gpu", api_cost_30d=round(api_cost, 6), legacy_gpu_cost_30d=legacy,
                          current_gpu_cost_30d=round(current_gpu, 6))
    used = Counter()
    for r in hourly:
        used[r["plan"]] += meter.counted(r["prompt_tokens"], r["completion_tokens"])
    total = sum(used.values())
    out["plans"] = {p: {"active_keys": active[p], "tokens_30d": used[p],
                        "revenue_30d": round(sum(s["usd"] for s in paid if s["plan"] == p and ts(s["paid_at"] or s["created"]) >= month), 2)
                        if sales is not None else None,
                        "cost_30d": (round(((legacy or 0) + (current_gpu or 0)) * used[p] / total +
                                          sum(r.get("upstream_micros", 0) for r in hourly if r["plan"] == p) / 1e6, 2)
                                     if (API_MODE or GPU_BILLING_START) and total else round(cost * used[p] / total, 2)
                                     if cost is not None and total else None)}
                    for p in store.PLAN_IDS}
    return out


@router.get("/v1/admin/overview")
async def overview(request: Request):
    refused = gate(request)
    if refused is not None:
        return refused
    g, now = checkout.gw(), int(time.time())
    try:
        raw = await invoices()
        sales = [sale(i) for i in raw]
    except (httpx.HTTPError, ValueError):
        raw, sales = None, None
    key_rows = keys(g.conn(), now)
    by_invoice = {s["id"]: s for s in sales or []}
    orders = []
    for ref, kind, plan, cents, invoice_id, created in g.conn().execute(
            "SELECT order_id, kind, plan, cents, invoice_id, created FROM orders WHERE done = 0 ORDER BY created DESC"):
        s = by_invoice.get(invoice_id)
        orders.append({"ref": ref, "kind": kind, "plan": plan or "payg", "usd": cents / 100, "created": g.iso(created),
                       "status": s["status"] if s else None, "sent": s["sent"] if s else None,
                       "underpaid": bool(s and s["underpaid"]),
                       "stuck": bool(s and (s["underpaid"] or s["status"] in ("paid", "confirmed", "complete")
                                            or (s["status"] == "expired" and s["sent"] > 0)))})
    month = now - 30 * DAY
    hourly, load = stats.rows(stats.hour_of(month))
    names = list(g.UPSTREAMS)
    return {"generated": g.iso(now),
            "settings": {"gpu_hourly": GPU_HOURLY or None, "speed_bar": stats.SPEED_BAR,
                         "prices": {p: c / 100 for p, c in checkout.PRICES.items()}},
            "health": {"model": await g.reachable(g.UPSTREAMS[names[0]]) if names else False,
                       "bitcart": sales is not None},
            "summary": summary(sales, key_rows, hourly, gpu_hours(month), now),
            "sales": sales[:300] if sales is not None else None,
            "wallets": await wallets(raw[0] if raw else None) if raw is not None else None,
            "keys": key_rows, "orders": orders, "hourly": hourly, "load": load}


@router.post("/v1/admin/run")
async def run(request: Request):
    refused = gate(request)
    if refused is not None:
        return refused
    g = checkout.gw()
    try:
        args = (await request.json()).get("args")
        assert isinstance(args, list) and 2 <= len(args) <= 3 and all(isinstance(a, str) for a in args)
        assert args[0] in COMMANDS
    except (ValueError, AttributeError, AssertionError):
        return g.error(400, "bad_command", f"Send {{\"args\": [command, ...]}} with one of: {', '.join(COMMANDS)}.")
    try:
        return {"output": admin.main(args, g.conn())}
    except ValueError as e:
        return g.error(400, "refused", str(e).splitlines()[0])
