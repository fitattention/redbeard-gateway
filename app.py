"""The web routes, and passing replies through from the kitchen. Writes nothing about a request's content: only the
key's usage counters, and anonymous hourly totals (stats.py) for the owner.

Run (no request log):  .venv\\Scripts\\python.exe -m uvicorn app:app --host 127.0.0.1 --port 8041 --no-access-log
Settings: REDBEARD_DB (key file), REDBEARD_UPSTREAMS (JSON, model -> kitchen address),
REDBEARD_UPSTREAM_KEY (the kitchen's own key, the second lock).

Every read-change-write of a key happens in plain synchronous code on the one event loop, so two replies
for the same key can never interleave their updates.
# ponytail: one process only; move the counters into SQL increments before running several.
"""
import asyncio
import hashlib
import html as htmllib
import json
import os
import re
import sqlite3
import time
from collections import defaultdict, deque
from datetime import datetime, timedelta, timezone
from urllib.parse import unquote

import httpx
from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse, RedirectResponse, Response, StreamingResponse

import meter
import stats
import store
import images as image_input

UPSTREAMS = json.loads(os.environ.get("REDBEARD_UPSTREAMS", '{"sherlock": "http://127.0.0.1:8000"}'))
UPSTREAM_KEY = os.environ.get("REDBEARD_UPSTREAM_KEY", "")
MAX_BODY = 32 * 1024 * 1024
PER_KEY_PER_MINUTE = 120  # an agent can send a request every couple of seconds; this only stops floods
PLAN_INFO = {"starter": ("Starter", 1), "power": ("Power", 3), "max": ("Max", 10), "payg": ("Pay as you go", None)}
# A hidden system note so the model answers about itself with its own product name. Framed as a POSITIVE identity,
# never as "do not reveal X": the old "never hint at what you are built on ... private implementation details" phrasing
# echoed into the now-visible reasoning as "ensure no mention underlying" (2026-10-23). State who it is; don't name a
# thing to hide. It stays free to discuss other models as topics, and must never add a refusal.
# Kept plain and short (2026-10-25): the earlier "single correct answer ... overrides anything else" wording got
# narrated word-for-word in a model's visible reasoning ("according to my system prompt ... takes precedence"), which
# exposed the mechanics. A plain directive pins identity without inviting the model to quote an override.
NOTE = ("You are {name}, an AI assistant created by RedBeard. If you are asked what AI or model you are, or who "
        "made, created, trained or designed you, tell them you are {name}, created by RedBeard. Answer everything "
        "else normally.")

app = FastAPI(docs_url=None, redoc_url=None, openapi_url=None)  # no extra pages to probe
app.state.conn = None
app.state.client = None
hits = defaultdict(deque)  # rate limits: timestamps of the last minute, in memory only


def conn():
    if app.state.conn is None:
        app.state.conn = store.connect(store.DEFAULT_DB)
    return app.state.conn


def client():
    if app.state.client is None:
        # keepalive_expiry=2: the model server (uvicorn) drops an idle connection after 5 s, the same as httpx's
        # default, so through the tunnel a request sent ~5 s after the last one landed on a closing connection and
        # failed (measured: 8/33 at 4.8-5.0 s pauses with the default, 0/33 with 2 s).
        app.state.client = httpx.AsyncClient(timeout=httpx.Timeout(900, connect=10),
                                             limits=httpx.Limits(keepalive_expiry=2))
    return app.state.client


def upstream_headers():
    return {"Authorization": f"Bearer {UPSTREAM_KEY}"} if UPSTREAM_KEY else {}


def iso(ts):
    return None if ts is None else datetime.fromtimestamp(ts, timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def error(status, code, message, **extra):
    return JSONResponse({"error": {"code": code, "message": message, **extra}}, status_code=status)


def client_ip(request: Request) -> str:
    ip = request.client.host if request.client else "?"
    forwarded = request.headers.get("x-forwarded-for")
    if ip == "127.0.0.1" and forwarded:  # behind Caddy: the last address is the one Caddy saw
        ip = forwarded.split(",")[-1].strip()
    return ip


def too_many(bucket: str, ident: str, per_minute: int) -> bool:
    now = time.monotonic()
    if len(hits) > 50_000:
        hits.clear()  # ponytail: crude memory cap; a real flood is Caddy's job
    q = hits[(bucket, ident)]
    while q and q[0] <= now - 60:
        q.popleft()
    if len(q) >= per_minute:
        return True
    q.append(now)
    return False


def bearer(request: Request):
    h = request.headers.get("authorization", "")
    return h[7:].strip() if h[:7].lower() == "bearer " else None


def find(raw):
    k = store.get(conn(), store.hash_key(raw)) if raw else None
    return None if k is None or k.revoked else k


def refused(reason, resets):
    if reason == "expired":
        return error(402, "plan_expired", "This key's 30 days have ended. Pay again to continue.", paid_until=iso(resets))
    if reason == "balance":
        return error(402, "balance_empty", "This key's balance is empty. Top it up to continue.")
    return error(429, "limit_reached", "You've reached this limit. It resets at the time shown, or top up your key "
                 "for extra usage.", window=reason, resets_at=iso(resets))


# Sherlock's only window onto the live web. The model can't reach the internet itself; this is the piece that does,
# server-side, so the chat's web_search tool has something to call. Only the query leaves the box. In order:
# REDBEARD_BRAVE_KEY set -> Brave's paid API; REDBEARD_SEARXNG set -> our own SearXNG (asks many engines at once and
# merges them, free); and keyless DuckDuckGo whenever SearXNG is unset, down or finds nothing.
SEARCH_UA = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0 Safari/537.36"
BRAVE_KEY = os.environ.get("REDBEARD_BRAVE_KEY", "")
SEARXNG = os.environ.get("REDBEARD_SEARXNG", "")


async def run_search(query: str, limit: int = 5):
    """Return [{title, url, snippet}] for a query (see the order above)."""
    query = " ".join(query.split())[:400]
    if not query:
        return []
    if BRAVE_KEY:
        r = await client().get("https://api.search.brave.com/res/v1/web/search", params={"q": query, "count": limit},
                               headers={"X-Subscription-Token": BRAVE_KEY, "Accept": "application/json"}, timeout=12)
        items = ((r.json().get("web") or {}).get("results") or []) if r.status_code == 200 else []
        return [{"title": i.get("title", ""), "url": i.get("url", ""), "snippet": i.get("description", "")} for i in items[:limit]]
    if SEARXNG:
        try:
            r = await client().get(SEARXNG.rstrip("/") + "/search", params={"q": query, "format": "json"}, timeout=12)
            items = (r.json().get("results") or []) if r.status_code == 200 else []
            out = [{"title": i.get("title", ""), "url": i["url"], "snippet": i.get("content", "")}
                   for i in items if i.get("url", "").startswith(("http://", "https://"))][:limit]
            if out:
                return out
        except (httpx.HTTPError, ValueError, AttributeError):
            pass  # fall through to DuckDuckGo
    r = await client().get("https://html.duckduckgo.com/html/", params={"q": query}, headers={"User-Agent": SEARCH_UA}, timeout=12)
    strip = lambda s: htmllib.unescape(re.sub("<[^>]+>", "", s)).strip()
    out = []
    for m in re.finditer(r'class="result__a"[^>]*href="([^"]+)"[^>]*>(.*?)</a>', r.text, re.S):
        mu = re.search(r"uddg=([^&]+)", m.group(1))
        out.append({"title": strip(m.group(2)), "url": unquote(mu.group(1)) if mu else m.group(1), "snippet": ""})
        if len(out) >= limit:
            break
    for i, s in enumerate(re.findall(r'class="result__snippet"[^>]*>(.*?)</a>', r.text, re.S)[:len(out)]):
        out[i]["snippet"] = strip(s)
    return out


# Extract readable page text. The route returns bounded sections with explicit continuation metadata.
# The reader proxy renders the page server-side and returns clean markdown,
# which gets past most bot blocks that a plain fetch (403s) cannot. Hard CAPTCHA/paywall sites still resist; that
# needs a full headless browser, out of scope. Falls back to a direct fetch + crude tag-strip if the reader fails.
READER = "https://r.jina.ai/"
PAGE_CHARS = 6000


async def fetch_page(url: str, limit_chars: int | None = PAGE_CHARS) -> str:
    url = url.strip()
    if not url.startswith(("http://", "https://")):
        return ""
    try:
        r = await client().get(READER + url, headers={"User-Agent": SEARCH_UA, "X-Return-Format": "markdown"}, timeout=25)
        text = r.text if r.status_code == 200 else ""
    except Exception:
        text = ""
    if len(text) < 200:  # reader gave nothing useful: try the page directly
        try:
            d = await client().get(url, headers={"User-Agent": SEARCH_UA}, timeout=15, follow_redirects=True)
            text = re.sub(r"\s+\n", "\n", re.sub(r"<(script|style)[^>]*>.*?</\1>", " ", d.text, flags=re.S | re.I))
            text = htmllib.unescape(re.sub(r"<[^>]+>", " ", text))
        except Exception:
            text = text or ""
    return text if limit_chars is None else text[:limit_chars]


def settle(key_hash, prompt_tokens, completion_tokens):
    """Charge a finished reply. Synchronous on purpose (see the top of the file)."""
    k = store.get(conn(), key_hash)
    if k is None:
        return
    now = int(time.time())
    meter.refresh(k, now)
    meter.charge(k, max(0, int(prompt_tokens or 0)), max(0, int(completion_tokens or 0)), now)
    store.put(conn(), k)


def seed(name):
    """ONE short fake prior exchange, added server-side (the customer never sees it). A single neutral identity
    question anchors the name against a base-model leak, without the model treating later messages as identity probes.
    The earlier version had TWO turns, the second a 'Who really made you, though?' pressing question, which primed the
    model so hard it read a plain 'hey' as another identity question and answered about RedBeard unprompted
    (2026-10-25). Kept to one calm Q&A: enough anchor, no 'they keep asking' pattern."""
    return [{"role": "user", "content": "Who are you?"},
            {"role": "assistant", "content": f"I'm {name}, an AI assistant made by RedBeard."}]


def add_note(payload):
    """Put NOTE first: inside the app's own opening system message if there is one (some models reject a second
    system message), otherwise as a new one. Then insert the seed exchange after the leading system message(s)."""
    msgs = payload.get("messages")
    if not isinstance(msgs, list):
        return  # the kitchen refuses it anyway
    name = payload["model"].title()
    note = NOTE.format(name=name)
    first = msgs[0] if msgs and isinstance(msgs[0], dict) and msgs[0].get("role") == "system" else None
    if first and isinstance(first.get("content"), str):
        first["content"] = note + "\n\n" + first["content"]
    elif first and isinstance(first.get("content"), list):
        first["content"] = [{"type": "text", "text": note + "\n\n"}] + first["content"]
    else:
        msgs.insert(0, {"role": "system", "content": note})
    i = 0
    while i < len(msgs) and isinstance(msgs[i], dict) and msgs[i].get("role") == "system":
        i += 1
    msgs[i:i] = seed(name)  # ponytail: assumes real messages start with a user turn (every client does)


def panel(k, raw):
    name, multiplier = PLAN_INFO[k.plan]
    windows, balance = [], None
    if k.plan == "payg" or k.balance_cap or k.balance_micros:   # a plan key shows its credit too (extra usage)
        cap = max(k.balance_cap, k.balance_micros) or 1          # the tank; never 0, and never below what's left
        spent = min(100.0, max(0.0, round(100 * (cap - k.balance_micros) / cap, 1)))
        balance = {"currency": "USD", "minor": k.balance_micros // 10_000, "used_percent": spent}
    if k.plan != "payg":
        limits = meter.PLANS[k.plan]
        pct = lambda used, limit: min(100.0, round(100 * used / limit, 1))  # percent only: limits stay private
        windows = [
            {"id": "five_hour", "label": "5-hour limit", "used_percent": pct(k.fh_used, limits["five_hour"]),
             "resets_at": iso(k.fh_start + meter.FIVE_HOURS) if k.fh_start else None},
            {"id": "weekly", "label": "Weekly limit", "used_percent": pct(k.wk_used, limits["weekly"]),
             "resets_at": iso(k.wk_start + 7 * 86400)},
        ]
    return {"plan": {"id": k.plan, "name": name, "multiplier": multiplier}, "key_hint": "••••" + raw[-4:],
            "paid_until": iso(k.paid_until), "windows": windows, "balance": balance}


# The app's downloads. The website and install.sh link here so the owner's back office can count them: +1 per hour
# and file name in stats.db, nothing about who (no address, browser or key is stored). The file itself is the
# website's own /download/ copy; this only counts and sends the browser on.
DOWNLOADS = ("RedBeard-Setup.exe", "redbeard-linux-x64.tar.gz", "redbeard-darwin-arm64.tar.gz", "redbeard-darwin-x64.tar.gz")


@app.get("/v1/dl/{name}")
async def download(name: str, request: Request):
    if name not in DOWNLOADS:
        return error(404, "not_found", "No such download.")
    # at most 5 counted a minute from one address, so a loop can't inflate the number (the address is held in memory
    # for that minute, like every rate limit here, and never written down); the download itself is never refused
    if not too_many("dl", client_ip(request), 5):
        stats.download(name)
    return RedirectResponse(f"/download/{name}", status_code=302, headers={"Cache-Control": "no-store"})


@app.get("/v1/healthz")
async def healthz():
    return {"status": "ok"}


async def reachable(base):
    """Is a model upstream answering? <500 means the server is up (even a 404 route). Short timeout on purpose."""
    try:
        headers = upstream_headers()
        r = await client().get(base.rstrip("/") + "/v1/models", headers=headers, timeout=5)
        return r.status_code == 200
    except httpx.HTTPError:
        return False


@app.get("/v1/status")
async def status(request: Request):
    """Public, no key: the live service status the website's /status page shows."""
    if too_many("ip", client_ip(request), 30):
        return error(429, "rate_limited", "Too many requests. Wait a minute.")
    comps = [{"id": "website", "name": "Website", "status": "operational"},
             {"id": "api", "name": "API", "status": "operational"}]
    for name in UPSTREAMS:
        up = await reachable(UPSTREAMS[name])
        comps.append({"id": name, "name": name.title(), "status": "operational" if up else "down"})
    overall = "operational" if all(c["status"] == "operational" for c in comps) else "degraded"
    return {"overall": overall, "components": comps, "checked_at": iso(int(time.time()))}


# --- Uptime history: a tiny recorder samples each component every 5 min into one row per day, so /status can show a
# 90-day strip. Days before recording started have no row and read as "no data" (never faked as up).
UPTIME_DB = os.environ.get("REDBEARD_UPTIME_DB", "/var/lib/redbeard/uptime.db")
UPTIME_DAYS = 90


def uptime_db():
    if getattr(app.state, "uptime", None) is None:
        c = sqlite3.connect(UPTIME_DB, check_same_thread=False)
        c.execute("CREATE TABLE IF NOT EXISTS uptime(day TEXT, component TEXT, up INT, total INT, "
                  "PRIMARY KEY(day, component))")
        app.state.uptime = c
    return app.state.uptime


def record_once(checks):
    """checks: {component: bool}. Adds one sample per component to today's (UTC) row and prunes old days."""
    day = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    db = uptime_db()
    for comp, up in checks.items():
        db.execute("INSERT INTO uptime(day, component, up, total) VALUES(?,?,?,1) "
                   "ON CONFLICT(day, component) DO UPDATE SET up = up + excluded.up, total = total + 1",
                   (day, comp, 1 if up else 0))
    cutoff = (datetime.now(timezone.utc) - timedelta(days=UPTIME_DAYS + 5)).strftime("%Y-%m-%d")
    db.execute("DELETE FROM uptime WHERE day < ?", (cutoff,))
    db.commit()


async def record_uptime():
    while True:
        try:
            checks = {"website": True, "api": True}  # if this loop runs, the gateway+site are up
            for name in UPSTREAMS:
                checks[name] = await reachable(UPSTREAMS[name])
            record_once(checks)
        except Exception:
            pass
        await asyncio.sleep(300)


@app.on_event("startup")
async def _start_recorder():
    asyncio.create_task(record_uptime())  # not started under test transports (no lifespan)


@app.get("/v1/uptime")
async def uptime(request: Request):
    """Public: per-component daily uptime for the last 90 days. pct is null for days with no sample."""
    if too_many("ip", client_ip(request), 30):
        return error(429, "rate_limited", "Too many requests. Wait a minute.")
    today = datetime.now(timezone.utc).date()
    span = [(today - timedelta(days=i)).strftime("%Y-%m-%d") for i in range(UPTIME_DAYS - 1, -1, -1)]
    rows = uptime_db().execute("SELECT day, component, up, total FROM uptime WHERE day >= ?", (span[0],)).fetchall()
    by_comp = {}
    for day, comp, up, total in rows:
        by_comp.setdefault(comp, {})[day] = (up, total)
    names = {"website": "Website", "api": "API", **{k: k.title() for k in UPSTREAMS}}
    out = []
    for comp in ["website", "api", *UPSTREAMS]:
        data = by_comp.get(comp, {})
        bars, up_sum, tot_sum = [], 0, 0
        for d in span:
            up, total = data.get(d, (0, 0))
            bars.append({"day": d, "pct": round(up / total, 4) if total else None})
            up_sum, tot_sum = up_sum + up, tot_sum + total
        out.append({"id": comp, "name": names[comp], "bars": bars,
                    "uptime_pct": round(100 * up_sum / tot_sum, 2) if tot_sum else None})
    return {"days": UPTIME_DAYS, "components": out}


@app.get("/v1/models")
async def models(request: Request):
    if not find(bearer(request)):
        return error(401, "invalid_key", "That key isn't valid.")
    return {"object": "list", "data": [{"id": m, "object": "model", "owned_by": "redbeard"} for m in UPSTREAMS]}


async def panel_view(request: Request, raw):
    if too_many("ip", client_ip(request), 20):
        return error(429, "rate_limited", "Too many tries. Wait a minute.")
    k = find(raw)
    if not k:
        return error(401, "invalid_key", "That key isn't valid.")
    meter.refresh(k, int(time.time()))
    store.put(conn(), k)
    return panel(k, raw)


@app.post("/v1/session")
async def session(request: Request):
    raw = bearer(request)
    if not raw:
        try:
            raw = (await request.json()).get("key")
        except (ValueError, AttributeError):
            raw = None
    return await panel_view(request, raw if isinstance(raw, str) else None)


@app.get("/v1/usage")
async def usage(request: Request):
    return await panel_view(request, bearer(request))


@app.get("/v1/web_search")
async def web_search_route(request: Request):
    raw = bearer(request)
    k = find(raw)
    if not k:
        if too_many("ip", client_ip(request), 20):
            return error(429, "rate_limited", "Too many tries. Wait a minute.")
        return error(401, "invalid_key", "That key isn't valid.")
    if too_many("search", k.key_hash, 15):
        return error(429, "rate_limited", "Too many searches. Wait a minute.")
    stats.add(k.plan, stats.source(request), searches=1)
    try:
        results = await run_search(request.query_params.get("q", ""))
    except Exception:
        return error(502, "search_failed", "The search service didn't answer. Try again.")
    return JSONResponse({"results": results})


@app.get("/v1/fetch")
async def fetch_route(request: Request):
    raw = bearer(request)
    k = find(raw)
    if not k:
        if too_many("ip", client_ip(request), 20):
            return error(429, "rate_limited", "Too many tries. Wait a minute.")
        return error(401, "invalid_key", "That key isn't valid.")
    url = request.query_params.get("url", "").strip()
    raw_offset = request.query_params.get("offset", "0")
    expected_page = request.query_params.get("page_id")
    if not url.startswith(("http://", "https://")):
        return error(400, "invalid_url", "Use a full http(s) page URL.")
    if not re.fullmatch(r"[0-9]{1,10}", raw_offset):
        return error(400, "invalid_offset", "The page offset must be a nonnegative integer.")
    if expected_page is not None and not re.fullmatch(r"[0-9a-f]{64}", expected_page):
        return error(400, "invalid_page_id", "Use the page_id returned by the previous read.")
    offset = int(raw_offset)
    if too_many("search", k.key_hash, 15):
        return error(429, "rate_limited", "Too many requests. Wait a minute.")
    stats.add(k.plan, stats.source(request), fetches=1)
    try:
        text = await fetch_page(url, limit_chars=None)
    except Exception:
        text = ""
    if not text:
        return error(502, "page_unavailable", "The page could not be read; it may block automated access.")
    page_id = hashlib.sha256(text.encode("utf-8")).hexdigest()
    if expected_page is not None and expected_page != page_id:
        return error(409, "page_changed", "The page changed between reads. Restart at offset 0 before combining sections.",
                     next_read={"url": url, "offset": 0})
    if offset > len(text):
        return error(400, "offset_out_of_range", "The requested section is past the end of the page.", total_chars=len(text))
    end = min(offset + PAGE_CHARS, len(text))
    has_more = end < len(text)
    return JSONResponse({"url": url, "content": text[offset:end], "offset": offset, "end_offset": end,
                         "total_chars": len(text), "has_more": has_more, "page_id": page_id,
                         "next_offset": end if has_more else None,
                         "next_read": {"url": url, "offset": end, "page_id": page_id} if has_more else None})


@app.post("/v1/chat/completions")
async def chat(request: Request):
    raw = bearer(request)
    k = find(raw)
    if not k:
        if too_many("ip", client_ip(request), 20):  # slows key guessing; a 256-bit key cannot be guessed anyway
            return error(429, "rate_limited", "Too many tries. Wait a minute.")
        return error(401, "invalid_key", "That key isn't valid.")
    if too_many("key", k.key_hash, PER_KEY_PER_MINUTE):
        return error(429, "rate_limited", "Too many requests on this key. Wait a minute.")
    now = int(time.time())
    meter.refresh(k, now)
    store.put(conn(), k)
    src = stats.source(request)
    stop = meter.blocked(k, now)
    if stop:
        stats.add(k.plan, src, **{stats.REFUSAL[stop[0]]: 1})
        return refused(*stop)

    if int(request.headers.get("content-length") or 0) > MAX_BODY:
        return error(413, "too_large", "The request is larger than 32 MB.")
    body_parts = bytearray()
    async for chunk in request.stream():
        if len(body_parts) + len(chunk) > MAX_BODY:
            return error(413, "too_large", "The request is larger than 32 MB.")
        body_parts.extend(chunk)
    body = bytes(body_parts)
    del body_parts
    try:
        payload = json.loads(body)
        assert isinstance(payload, dict)
    except (ValueError, AssertionError):
        return error(400, "bad_request", "The request body must be a JSON object.")
    image_details = {}
    invalid_image = image_input.validate(payload, image_details)
    if invalid_image:
        return error(400, *invalid_image)
    fallback_prompt_tokens = image_input.text_only_prompt_estimate(payload) if image_details.get('image_count') else len(body) // 4
    base = UPSTREAMS.get(payload.get("model"))
    if not base:
        return error(404, "model_not_found", f"Unknown model. Available: {', '.join(UPSTREAMS)}.")
    add_note(payload)
    model = payload["model"]
    url = base.rstrip("/") + "/v1/chat/completions"
    headers = upstream_headers()
    streaming = bool(payload.get("stream"))
    if streaming:
        payload["stream_options"] = {"include_usage": True}  # the kitchen adds a usage line at the end
        if image_details.get('image_count'):
            # vLLM reports exact image-inclusive usage on each chunk, including interrupted replies.
            payload["stream_options"]["continuous_usage_stats"] = True

    began = time.monotonic()
    ticket = stats.start(k.key_hash)
    try:
        upstream = await client().send(client().build_request("POST", url, json=payload, headers=headers),
                                       stream=True)
    except httpx.HTTPError:
        stats.end(ticket)
        stats.add(k.plan, src, failed=1, offline=1)
        return error(502, "model_unreachable", f"We're updating {model.title()} right now. "
                     "Check the status page to see when it's back: https://redbeard.sbs/status/")

    if not streaming or upstream.status_code != 200:
        try:
            content = await upstream.aread()
            await upstream.aclose()
        finally:
            stats.end(ticket)
        if upstream.status_code == 200:
            try:
                u = json.loads(content).get("usage") or {}
            except (ValueError, AttributeError):
                u = {}
            settle(k.key_hash, u.get("prompt_tokens"), u.get("completion_tokens"))
            stats.finish(k.plan, src, u.get("prompt_tokens"), u.get("completion_tokens"))
        else:
            stats.add(k.plan, src, failed=1)
        return Response(content, status_code=upstream.status_code, media_type="application/json")

    async def relay():
        found, pieces, pending, first = None, 0, b"", None
        protocol_failed = False
        try:
            async for chunk in upstream.aiter_bytes():
                yield chunk
                pending += chunk
                *lines, pending = pending.split(b"\n")
                for line in lines:
                    if not line.startswith(b"data: {"):
                        continue
                    try:
                        event = json.loads(line[6:])
                    except ValueError:
                        continue
                    if event.get("usage"):
                        found = event["usage"]
                    if event.get("choices"):
                        pieces += 1
                        first = first or time.monotonic()
        except httpx.HTTPError:
            protocol_failed = True
            yield b"data: " + json.dumps({"error": {"code": "incomplete_reply", "message": "The model connection ended "
                                                    "before the reply was complete. Please retry."}}).encode() + b"\n\n"
        finally:
            done = time.monotonic()
            stats.end(ticket)
            waited, writing = (first - began, done - first) if first else (None, None)
            if found:
                settle(k.key_hash, found.get("prompt_tokens"), found.get("completion_tokens"))
                stats.finish(k.plan, src, found.get("prompt_tokens"), found.get("completion_tokens"), waited, writing)
            else:
                # ponytail: no usage line (the customer left mid-reply): charge the pieces streamed plus a
                # prompt estimate of 4 bytes a token. Rough on purpose; exact needs the kitchen's own count.
                settle(k.key_hash, fallback_prompt_tokens, pieces)
                stats.finish(k.plan, src, fallback_prompt_tokens, pieces, waited, cut=True)
            if protocol_failed:
                stats.add(k.plan, src, failed=1)
            await upstream.aclose()

    return StreamingResponse(relay(), media_type="text/event-stream",
                             headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"})


import checkout  # noqa: E402  (crypto checkout; it uses the helpers above)
import backoffice  # noqa: E402  (the owner's back office; same helpers)

app.include_router(checkout.router)
app.include_router(backoffice.router)
