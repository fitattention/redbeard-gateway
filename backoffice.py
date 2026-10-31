"""The owner's back office: GET /v1/<path>/overview (everything on one page) and POST /v1/<path>/run (admin.py's own
commands, so the page and the server command can never disagree). <path> is REDBEARD_ADMIN_PATH, a private value
kept only in the server's environment; any other path answers 404 like an address that doesn't exist, and while it
is unset the back office is off.

One admin key, kept only as its SHA-256 in REDBEARD_ADMIN_HASH; while that is unset every admin route answers 404.
It reads what already exists and keeps the halves apart as they are on disk: sales come from Bitcart (the gateway's
own invoice token, nothing new), keys from the key table, usage from stats.py's anonymous hourly totals; a sale is
never joined to a key. Wallet balances are read from the public chains, so this server never holds Bitcart's wallet
access (which could change the address customers pay into). Something it can't read shows as null, never as 0.
Settings: REDBEARD_ADMIN_HASH, REDBEARD_ADMIN_PATH, REDBEARD_GPU_HOURLY (dollars an hour for the GPU box: the estimate
used when the real bill can't be read). The real GPU bill comes from an optional private module, gpu_bill, kept out
of this public code; it holds only the owner's own costs, never anything about customers.
"""
import asyncio
import hashlib
import hmac
import json
import os
import re
import time
from collections import Counter
from math import ceil
from dataclasses import replace
from datetime import datetime, timedelta, timezone
from pathlib import Path
from statistics import median

import httpx
from fastapi import APIRouter, Request
from fastapi.responses import JSONResponse

import admin
import checkout
import meter
import stats
import store

try:
    import gpu_bill  # private: reads the GPU provider's bill with a read-only key (not part of the public code)
except ImportError:
    gpu_bill = None

router = APIRouter()
ADMIN_HASH = os.environ.get("REDBEARD_ADMIN_HASH", "").strip().lower()
ADMIN_PATH = os.environ.get("REDBEARD_ADMIN_PATH", "").strip()  # unset: off, never a guessable default
GPU_HOURLY = float(os.environ.get("REDBEARD_GPU_HOURLY") or 0)  # only for the estimate, when the bill can't be read
COMMANDS = ("new", "extend", "topup", "revoke", "credit")
PLAN_BY_PRICE = {cents / 100: plan for plan, cents in checkout.PRICES.items()}  # sales made before metadata: guessed
RPC = {"bnb": "https://bsc-rpc.publicnode.com", "matic": "https://polygon-bor-rpc.publicnode.com",
       "eth": "https://ethereum-rpc.publicnode.com"}
TRONGRID = "https://api.trongrid.io/v1/accounts/"
DAY = 86400
PRODUCT_BOXES = ("sherlock", "watson")  # boxes that serve customers, by their label at the GPU provider
_wallets = (0.0, [])  # (when read, rows): public chains are asked at most every two minutes


def gate(request, door):
    """None for the holder of the admin key at the private path, else the answer to send."""
    g = checkout.gw()
    if not ADMIN_HASH or not hmac.compare_digest(door.encode(), ADMIN_PATH.encode()):  # unset path never matches
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
        credit = payg or bool(k.balance_cap or k.balance_micros)     # a plan key can hold credit too (extra usage)
        live = k.balance_micros > 0 or (not payg and bool(k.paid_until and now < k.paid_until))
        limits = meter.PLANS.get(k.plan) or {}
        pct = lambda used, cap: round(min(100.0, 100 * used / cap), 1) if cap else None
        out.append({
            "fp": k.key_hash, "plan": k.plan,
            "status": "revoked" if k.revoked else "active" if live else "empty" if payg else "ended",
            "paid_until": checkout.gw().iso(k.paid_until),
            "days_left": None if payg or k.paid_until is None else round((k.paid_until - now) / DAY, 1),
            "five_hour_pct": None if payg else pct(v.fh_used, limits.get("five_hour")),
            "week_pct": None if payg else pct(v.wk_used, limits.get("weekly")),
            "balance_usd": round(k.balance_micros / 1e6, 2) if credit else None,
            "balance_left_pct": pct(k.balance_micros, max(k.balance_cap, k.balance_micros)) if credit else None})
    return out


def gpu_hours(since):
    """Hours the model answered, from the /status recorder's five-minute samples. Only hours a server really answered
    count; a night with no server counts nothing. Runs a little LOW against the bill: the provider charges a server
    from the moment it is rented, setup included."""
    g = checkout.gw()
    day = datetime.fromtimestamp(since, timezone.utc).strftime("%Y-%m-%d")
    names = list(g.UPSTREAMS)
    try:
        up = g.uptime_db().execute("SELECT COALESCE(SUM(up), 0) FROM uptime WHERE day >= ? AND component = ?",
                                   (day, names[0] if names else "")).fetchone()[0]
    except Exception:
        return None
    return round(up * 300 / 3600, 1)


def summary(sales, key_rows, hourly, hours, now, spent=None):
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
    estimate = round(hours * GPU_HOURLY, 2) if hours is not None and GPU_HOURLY else None
    outside = round(sum(r.get("upstream_micros", 0) for r in hourly) / 1e6, 2)  # past outside-AI bills, before 2026-10-28
    gpu = spent if spent is not None else estimate  # the real GPU bill when it can be read
    cost = round(gpu + outside, 2) if gpu is not None else None
    out["gpu"] = {"source": "bill" if spent is not None else "estimate", "hours_30d": hours, "hourly": GPU_HOURLY or None,
                  "estimate_30d": estimate, "billed_30d": spent, "outside_ai_30d": outside, "cost_30d": cost,
                  "margin_30d": round(out["revenue"]["30d"] - cost, 2) if out["revenue"] and cost is not None else None}
    out["offline"] = {name: sum(r.get("offline", 0) for r in hourly if r["hour"] >= now - days * DAY)
                      for name, days in (("24h", 1), ("7d", 7), ("30d", 30))}
    used = Counter()
    for r in hourly:
        used[r["plan"]] += meter.counted(r["prompt_tokens"], r["completion_tokens"], r["cached_tokens"])
    total = sum(used.values())
    out["plans"] = {p: {"active_keys": active[p], "tokens_30d": used[p],
                        "revenue_30d": round(sum(s["usd"] for s in paid if s["plan"] == p and ts(s["paid_at"] or s["created"]) >= month), 2)
                        if sales is not None else None,
                        "cost_30d": round(cost * used[p] / total, 2) if cost is not None and total else None}
                    for p in store.PLAN_IDS}
    return out


def accounts(sales, bill, monthly, key_rows, hourly, cost_30d, now):
    """The owner's books, by UTC month, on a cash basis: a sale counts in the month it was paid. The books start with
    the month of the first paid sale; GPU spending before that is reported apart, not mixed in. Each figure comes from
    one record (Bitcart, the GPU bill, the gateway's own counters); anything a record can't give is None, never 0."""
    month_of = lambda t: datetime.fromtimestamp(t, timezone.utc).strftime("%Y-%m")
    paid = [s for s in sales or [] if s["status"] == "complete"]
    sold, count, received = Counter(), Counter(), Counter()
    for s in paid:
        m = month_of(ts(s["paid_at"] or s["created"]))
        sold[m] += s["usd"]
        count[m] += 1
        received[m] += s["sent"]
    ok = bool(bill and bill.get("ok"))
    spent = bill.get("spent_by_month") if ok else None
    product = bill.get("product_spent_by_month") if ok else None
    deposited = bill.get("deposited_by_month") if ok else None
    first = min(sold) if sold else month_of(now)
    known = sales is not None
    rows = []
    for m in sorted({m for m in [*sold, *(spent or {}), *monthly, month_of(now)] if m >= first}, reverse=True):
        gpu = round(spent.get(m, 0.0), 2) if spent is not None else None
        outside = round((monthly.get(m) or {}).get("upstream_micros", 0) / 1e6, 2)
        income = round(sold.get(m, 0.0), 2) if known else None
        rows.append({"month": m, "sales_usd": income, "sales": count.get(m, 0) if known else None,
                     "gpu_usd": gpu, "gpu_product_usd": round(product.get(m, 0.0), 2) if product is not None else None,
                     "outside_ai_usd": outside,
                     "profit_usd": round(income - gpu - outside, 2) if income is not None and gpu is not None else None,
                     "cash_in_usdt": round(received.get(m, 0.0), 2) if known else None,
                     "cash_out_usd": round(deposited.get(m, 0.0), 2) if deposited is not None else None,
                     "replies": (monthly.get(m) or {}).get("requests", 0), "tokens": (monthly.get(m) or {}).get("tokens", 0)})
    # owed: plan time already paid for but not yet used (each plan is 30 days from its payment), and prepaid balances
    unearned = sum(s["usd"] * max(0.0, 30 - (now - ts(s["paid_at"])) / DAY) / 30
                   for s in paid if s["plan"] != "payg" and s["paid_at"]) if known else None
    replies, tokens = sum(r["requests"] for r in hourly), sum(r["prompt_tokens"] + r["completion_tokens"] for r in hourly)
    busy = len({r["hour"] for r in hourly if r["requests"]})
    box_month = GPU_HOURLY * 24 * 30
    return {"basis": "cash", "first_month": first, "months": rows,
            "gpu_before_books_usd": round(sum(v for m, v in spent.items() if m < first), 2) if spent is not None else None,
            "owed": {"unearned_plans_usd": round(unearned, 2) if unearned is not None else None,
                     "prepaid_balances_usd": round(sum(k["balance_usd"] or 0 for k in key_rows if k["status"] == "active"), 2)},
            # busy hours over the 720 clock hours of 30 days: one unit on both sides (status-check hours are a different
            # measure, and dividing one by the other once gave an impossible 102%)
            "unit_30d": {"replies": replies, "tokens": tokens, "busy_hours": busy, "clock_hours": 30 * 24,
                         "cost_per_reply_usd": round(cost_30d / replies, 4) if cost_30d is not None and replies else None,
                         "cost_per_million_tokens_usd": round(cost_30d / tokens * 1e6, 2) if cost_30d is not None and tokens else None,
                         "sales": sum(1 for s in paid if ts(s["paid_at"] or s["created"]) >= now - 30 * DAY) if known else None},
            "break_even": {"box_month_usd": round(box_month, 2) if GPU_HOURLY else None,
                           "sales_needed": {p: ceil(box_month / (c / 100)) for p, c in checkout.PRICES.items()} if GPU_HOURLY else None}}


def book_settings():
    """The owner's private book settings: books.json beside this file, or REDBEARD_BOOKS (JSON). Read on every look,
    so a change needs no restart. operating_start: "YYYY-MM-DD", the day the service began running on a box rented by
    the month; until it is set, every GPU dollar is research and development. monthly_costs: [{"name", "usd",
    "from": "YYYY-MM-DD"}], fixed costs such as a server, counted for every month from the one they start in.
    other_projects: label prefixes of GPU boxes that belong to the owner's other work, left out of these books.
    books_begin: "YYYY-MM-DD", the day this business's work began; GPU spending before it was other work, left out."""
    try:
        raw = os.environ.get("REDBEARD_BOOKS") or Path(__file__).with_name("books.json").read_text(encoding="utf-8")
        cfg = json.loads(raw)
    except (OSError, ValueError):
        cfg = {}
    day = lambda v: v if isinstance(v, str) and re.fullmatch(r"\d{4}-\d{2}-\d{2}", v) else None
    costs = [{"name": str(c.get("name") or "cost"), "usd": float(c["usd"]), "from": day(c.get("from"))}
             for c in cfg.get("monthly_costs") or [] if isinstance(c, dict) and isinstance(c.get("usd"), (int, float))]
    return {"operating_start": day(cfg.get("operating_start")), "monthly_costs": [c for c in costs if c["from"]],
            "other_projects": tuple(str(p) for p in cfg.get("other_projects") or [] if p),
            "books_begin": day(cfg.get("books_begin"))}


def statement(bill, sales, monthly, now, settings):
    """The business's books, by UTC month, on a cash basis. Every GPU dollar is research and development (R&D) until
    the operating start; from that day, boxes labelled sherlock or watson are the cost of running the service and every
    other box is still R&D. The outside AI used before our own box is R&D too. Fixed monthly costs come from the
    settings; anything no record can see is listed in the page as "not in these books". None means unknown, never 0."""
    month_of = lambda t: datetime.fromtimestamp(t, timezone.utc).strftime("%Y-%m")
    start, other, begin = settings["operating_start"], settings["other_projects"], settings.get("books_begin")
    days = bill.get("spent_by_day_label") if bill and bill.get("ok") else None
    since_30d = datetime.fromtimestamp(now - 30 * DAY, timezone.utc).strftime("%Y-%m-%d")
    rnd, running, left_out, rnd_30d, running_30d = Counter(), Counter(), Counter(), 0.0, 0.0
    for day, labels in (days or {}).items():
        for label, usd in labels.items():
            if (other and label.startswith(other)) or (begin and day < begin):  # other work: not this business
                left_out[day[:7]] += usd
            elif start and day >= start and label.startswith(PRODUCT_BOXES):
                running[day[:7]] += usd
                running_30d += usd if day >= since_30d else 0
            else:
                rnd[day[:7]] += usd
                rnd_30d += usd if day >= since_30d else 0
    for m, row in monthly.items():
        if not begin or m >= begin[:7]:
            rnd[m] += (row.get("upstream_micros") or 0) / 1e6
    paid = [s for s in sales or [] if s["status"] == "complete"]
    sold = Counter()
    for s in paid:
        sold[month_of(ts(s["paid_at"] or s["created"]))] += s["usd"]
    fixed = lambda m: sum(c["usd"] for c in settings["monthly_costs"] if c["from"][:7] <= m)
    gpu_known, sales_known = days is not None, sales is not None
    known = lambda v, ok: round(v, 2) if ok else None
    sub = lambda a, b: round(a - b, 2) if a is not None and b is not None else None
    rows = []
    for m in sorted({*rnd, *running, *sold, month_of(now)} - {""}, reverse=True):
        revenue, cost = known(sold[m], sales_known), known(running[m], gpu_known)
        gross = sub(revenue, cost)
        operating = sub(gross, fixed(m))
        r_and_d = known(rnd[m], gpu_known)
        rows.append({"month": m, "revenue_usd": revenue, "running_usd": cost, "gross_usd": gross,
                     "gross_margin_pct": round(100 * gross / revenue, 1) if gross is not None and revenue else None,
                     "fixed_usd": round(fixed(m), 2), "operating_usd": operating, "rnd_usd": r_and_d,
                     "net_usd": sub(operating, r_and_d)})
    plan_30d = [s for s in paid if s["plan"] != "payg" and ts(s["paid_at"] or s["created"]) >= now - 30 * DAY]
    payg_30d = sum(s["usd"] for s in paid if s["plan"] == "payg" and ts(s["paid_at"] or s["created"]) >= now - 30 * DAY)
    mrr = sum(s["usd"] for s in plan_30d)
    revenue_30d = mrr + payg_30d
    fixed_now = fixed(month_of(now))
    gross_30d = round(revenue_30d - running_30d, 2) if sales_known and gpu_known else None
    profit_30d = round(gross_30d - fixed_now, 2) if gross_30d is not None else None
    rnd_total = round(sum(rnd.values()), 2) if gpu_known else None
    box_month = GPU_HOURLY * 24 * 30
    deposited = bill.get("deposited_30d") if bill and bill.get("ok") else None
    return {
        "stage": "running" if start else "building", "operating_start": start, "basis": "cash",
        "monthly_costs": settings["monthly_costs"], "months": rows,
        "left_out_usd": round(sum(left_out.values()), 2) if gpu_known else None,
        "rnd_total_usd": rnd_total, "rnd_months": sum(1 for v in rnd.values() if v > 0) if gpu_known else None,
        "last_30d": {
            "revenue_usd": round(revenue_30d, 2) if sales_known else None, "mrr_usd": round(mrr, 2) if sales_known else None,
            "arr_usd": round(12 * mrr, 2) if sales_known else None, "payg_usd": round(payg_30d, 2) if sales_known else None,
            "paying_customers": len(plan_30d) if sales_known else None,
            "arpu_usd": round(mrr / len(plan_30d), 2) if sales_known and plan_30d else None,
            "running_usd": round(running_30d, 2) if gpu_known else None, "rnd_usd": round(rnd_30d, 2) if gpu_known else None,
            "gross_usd": gross_30d, "gross_margin_pct": round(100 * gross_30d / revenue_30d, 1) if gross_30d is not None and revenue_30d else None,
            "fixed_usd": round(fixed_now, 2), "operating_usd": profit_30d,
            "cash_flow_usd": round(revenue_30d - deposited - fixed_now, 2) if deposited is not None and sales_known else None},
        "monthly_run_cost_usd": round(box_month + fixed_now, 2) if GPU_HOURLY else None,
        "break_even_customers": {p: ceil((box_month + fixed_now) / (c / 100)) for p, c in checkout.PRICES.items()} if GPU_HOURLY else None,
        "rnd_payback_months": round(rnd_total / profit_30d, 1) if rnd_total is not None and profit_30d and profit_30d > 0 else None,
    }


def download_totals(rows, now):
    """App downloads (stats.downloads rows) per file for the last 24 hours, 7 and 30 days and all kept, and per
    UTC day for the last 30 days. Counts only: the table holds no visitor detail."""
    files = {}
    for r in rows:
        f = files.setdefault(r["file"], {"24h": 0, "7d": 0, "30d": 0, "all": 0})
        f["all"] += r["count"]
        for name, span in (("24h", DAY), ("7d", 7 * DAY), ("30d", 30 * DAY)):
            if now - r["hour"] < span:
                f[name] += r["count"]
    days = {}
    for r in rows:
        if now - r["hour"] < 30 * DAY:
            days[r["hour"] // DAY * DAY] = days.get(r["hour"] // DAY * DAY, 0) + r["count"]
    return {"files": files, "daily": [{"day": d, "count": c} for d, c in sorted(days.items())]}


@router.get("/v1/{door}/overview")
async def overview(door: str, request: Request):
    refused = gate(request, door)
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
    bill = await gpu_bill.read(now) if gpu_bill else {"connected": False}
    hours = gpu_hours(month)
    totals = summary(sales, key_rows, hourly, hours, now, bill.get("spent_30d") if bill.get("ok") else None)
    monthly = stats.monthly(stats.hour_of(now - stats.KEEP_DAYS * DAY))
    return {"generated": g.iso(now),
            "settings": {"gpu_hourly": GPU_HOURLY or None, "speed_bar": stats.SPEED_BAR,
                         "prices": {p: c / 100 for p, c in checkout.PRICES.items()}},
            "health": {"model": await g.reachable(g.UPSTREAMS[names[0]]) if names else False,
                       "bitcart": sales is not None, "gpu_bill": bill.get("ok") if bill["connected"] else None},
            "summary": totals,
            "accounts": accounts(sales, bill, monthly, key_rows, hourly, totals["gpu"]["cost_30d"], now),
            "books": statement(bill, sales, monthly, now, book_settings()),
            "gpu_bill": bill,
            "sales": sales[:300] if sales is not None else None,
            "wallets": await wallets(raw[0] if raw else None) if raw is not None else None,
            "keys": key_rows, "orders": orders, "hourly": hourly, "load": load,
            "downloads": download_totals(stats.downloads(stats.hour_of(now - stats.KEEP_DAYS * DAY)), now)}


@router.post("/v1/{door}/run")
async def run(door: str, request: Request):
    refused = gate(request, door)
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
