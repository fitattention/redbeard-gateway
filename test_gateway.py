"""The runnable check. Run: .venv\\Scripts\\python.exe test_gateway.py  (prints PASS lines, exits 1 on a failure)."""
import calendar
import sqlite3

import meter
import store

SUN = calendar.timegm((2026, 10, 18, 0, 0, 0))  # a Sunday, 00:00 UTC
HOUR, DAY = 3600, 86400


def plan_key(plan="starter", paid_days=30, now=SUN):
    k = store.Key(key_hash="x", plan=plan, paid_until=now + paid_days * DAY)
    meter.refresh(k, now)
    return k


def test_week_start():
    assert meter.week_start(SUN) == SUN
    assert meter.week_start(SUN + 4 * DAY + 12 * HOUR) == SUN          # Thursday noon
    assert meter.week_start(SUN - 1) == SUN - 7 * DAY                   # Saturday 23:59:59


def test_blocked_and_windows():
    now = SUN + 10 * HOUR
    assert meter.blocked(plan_key(paid_days=0), now)[0] == "expired"     # paid_until == now: ran out
    k = store.Key(key_hash="x", plan="starter")                          # never paid
    meter.refresh(k, now)
    assert meter.blocked(k, now) == ("expired", None)
    k = plan_key()
    assert meter.blocked(k, now) is None
    meter.charge(k, prompt_tokens=8_000, completion_tokens=299_000, now=now)
    assert (k.fh_start, k.fh_used, k.wk_used) == (now, 300_000, 300_000)  # 299,000 + 8,000 / 8
    assert meter.blocked(k, now + HOUR) == ("five_hour", now + 5 * HOUR)
    meter.refresh(k, now + 5 * HOUR)                                       # window over
    assert (k.fh_start, k.fh_used, k.wk_used) == (None, 0, 300_000)
    assert meter.blocked(k, now + 5 * HOUR) is None
    k.wk_used = 1_300_000
    assert meter.blocked(k, now + 5 * HOUR) == ("weekly", SUN + 7 * DAY)
    meter.refresh(k, SUN + 7 * DAY)                                        # new week
    assert k.wk_used == 0 and meter.blocked(k, SUN + 7 * DAY) is None


def test_payg():
    k = store.Key(key_hash="x", plan="payg", balance_micros=5_000_000)    # $5
    assert meter.blocked(k, SUN) is None
    meter.charge(k, prompt_tokens=1_000, completion_tokens=100, now=SUN)
    assert k.balance_micros == 5_000_000 - 100 * 10 - 1_000 * 1
    assert (k.fh_used, k.wk_used) == (0, 0)                                # pay-as-you-go has no windows
    k.balance_micros = 0
    assert meter.blocked(k, SUN) == ("balance", None)


def test_extend():
    k = store.Key(key_hash="x", plan="power")
    meter.extend(k, SUN)
    assert k.paid_until == SUN + 30 * DAY
    meter.extend(k, SUN + DAY)                                             # paid again early: stacks
    assert k.paid_until == SUN + 60 * DAY
    meter.extend(k, SUN + 90 * DAY)                                        # paid after running out: from today
    assert k.paid_until == SUN + 120 * DAY


def test_store():
    raw = store.new_raw_key()
    assert raw.startswith("rb_") and len(raw) == 46 and raw != store.new_raw_key()
    conn = store.connect(":memory:")
    k = store.Key(key_hash=store.hash_key(raw), plan="max", paid_until=SUN)
    store.put(conn, k)
    assert store.get(conn, store.hash_key(raw)) == k
    assert store.get(conn, store.hash_key(raw + "x")) is None
    k.fh_used = 7
    store.put(conn, k)                                                     # writing again replaces, not duplicates
    assert store.get(conn, k.key_hash).fh_used == 7
    assert conn.execute("SELECT count(*) FROM keys").fetchone()[0] == 1
    assert raw not in str(conn.execute("SELECT * FROM keys").fetchall())  # the raw key is never stored
    try:
        store.put(conn, store.Key(key_hash="y", plan="free"))
        raise AssertionError("an unknown plan was accepted")
    except sqlite3.IntegrityError:
        pass


# ---------- end to end: a fake kitchen behind the real gateway ----------
import asyncio
import json
import threading
import time

import httpx
from fastapi import FastAPI, Request
from fastapi.responses import StreamingResponse

import app as gateway
import stats

kitchen = FastAPI()
seen = []                       # what the kitchen received
kitchen_mode = {"usage": True, "slow": 0.0}


@kitchen.post("/v1/chat/completions")
async def fake_kitchen(request: Request):
    body = await request.json()
    seen.append(body)
    if not body.get("stream"):
        return {"choices": [{"message": {"content": "Hello"}}], "usage": {"prompt_tokens": 80, "completion_tokens": 5}}

    async def pieces():
        for word in ["Hel", "lo"] + ["."] * (40 if kitchen_mode["slow"] else 0):
            yield "data: " + json.dumps({"choices": [{"delta": {"content": word}}]}) + "\n\n"
            await asyncio.sleep(kitchen_mode["slow"])
        if kitchen_mode["usage"] and body.get("stream_options", {}).get("include_usage"):
            written = 1000 if kitchen_mode["slow"] else 2  # big when slow, so a full charge is easy to tell apart
            yield 'data: {"choices":[],"usage":{"prompt_tokens":80,"completion_tokens":%d}}\n\n' % written
        yield "data: [DONE]\n\n"
    return StreamingResponse(pieces(), media_type="text/event-stream")


def fresh_gateway():
    """In-memory keys, the fake kitchen as the only model, rate limits cleared."""
    gateway.app.state.conn = store.connect(":memory:")
    gateway.UPSTREAMS = {"sherlock": "http://kitchen"}
    gateway.hits.clear()
    seen.clear()
    kitchen_mode.update(usage=True, slow=0.0)
    stats._db = stats.connect(":memory:")
    stats._hour, stats._keys, stats._open = 0, set(), {}


def add_key(plan="starter", **fields):
    raw = store.new_raw_key()
    k = store.Key(key_hash=store.hash_key(raw), plan=plan, **fields)
    if plan != "payg" and "paid_until" not in fields:
        meter.extend(k, int(time.time()))
    store.put(gateway.app.state.conn, k)
    return raw, k.key_hash


def used(key_hash):
    return store.get(gateway.app.state.conn, key_hash)


async def run_e2e():
    fresh_gateway()
    gateway.app.state.client = httpx.AsyncClient(transport=httpx.ASGITransport(app=kitchen))
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=gateway.app), base_url="http://gw") as gw:
        raw, h = add_key()
        auth = {"Authorization": f"Bearer {raw}"}
        ask = {"model": "sherlock", "messages": [{"role": "user", "content": "hi"}]}

        assert (await gw.get("/v1/healthz")).json() == {"status": "ok"}
        assert (await gw.get("/v1/models")).status_code == 401                       # no key
        assert (await gw.get("/v1/models", headers=auth)).json()["data"][0]["id"] == "sherlock"

        s = (await gw.get("/v1/status")).json()                                       # public status page feed
        assert [c["id"] for c in s["components"]][:2] == ["website", "api"]
        assert any(c["id"] == "sherlock" for c in s["components"]) and s["overall"] in ("operational", "degraded")
        for bad in ({}, {"Authorization": "Bearer rb_wrong"}, {"Authorization": raw}):
            r = await gw.post("/v1/chat/completions", json=ask, headers=bad)
            assert r.status_code == 401 and r.json()["error"]["code"] == "invalid_key"
        assert seen == []                                                             # nothing reached the kitchen

        r = await gw.post("/v1/chat/completions", json=ask, headers=auth)             # plain reply
        assert r.status_code == 200 and r.json()["choices"][0]["message"]["content"] == "Hello"
        assert used(h).fh_used == 5 + 80 // 8
        note = gateway.NOTE.format(name="Sherlock")                                   # the hidden note + seed
        m = seen[-1]["messages"]
        assert m[0] == {"role": "system", "content": note}                            # note first
        assert m[-1] == {"role": "user", "content": "hi"}                             # real message last, untouched
        assert sum(1 for x in m if x["role"] == "system") == 1                        # exactly one system message
        assert [x["role"] for x in m] == ["system", "user", "assistant", "user", "assistant", "user"]  # seed between
        assert any("RedBeard" in x["content"] for x in m if x["role"] == "assistant")
        app_sys = {"role": "system", "content": "You are opencode."}
        await gw.post("/v1/chat/completions", json={**ask, "messages": [app_sys, ask["messages"][0]]}, headers=auth)
        m = seen[-1]["messages"]
        assert m[0]["content"] == note + "\n\nYou are opencode."                      # merged into the app's system
        assert sum(1 for x in m if x["role"] == "system") == 1                        # merged, not a second one
        assert m[-1] == {"role": "user", "content": "hi"}
        parts = {"role": "system", "content": [{"type": "text", "text": "You are opencode."}]}
        await gw.post("/v1/chat/completions", json={**ask, "messages": [parts, ask["messages"][0]]}, headers=auth)
        assert seen[-1]["messages"][0]["content"] == [{"type": "text", "text": note + "\n\n"}, parts["content"][0]]
        assert "Sherlock" in note and "RedBeard" in note

        r = await gw.post("/v1/chat/completions", json={**ask, "stream": True}, headers=auth)
        assert r.status_code == 200 and "Hel" in r.text and '"usage"' in r.text
        assert seen[-1]["stream_options"] == {"include_usage": True}                  # we asked for usage
        assert used(h).fh_used == 3 * 15 + 2 + 80 // 8                                # 3 plain replies so far

        kitchen_mode["usage"] = False                                                 # kitchen forgets usage
        before = used(h).fh_used
        r = await gw.post("/v1/chat/completions", json={**ask, "stream": True}, headers=auth)
        assert r.status_code == 200 and used(h).fh_used > before                      # estimate still charged
        kitchen_mode["usage"] = True

        r = await gw.post("/v1/chat/completions", json={**ask, "model": "gpt-5"}, headers=auth)
        assert r.status_code == 404
        for junk in (b"not json", b"[1, 2]", b""):
            r = await gw.post("/v1/chat/completions", content=junk, headers=auth)
            assert r.status_code == 400, junk

        n = len(seen)
        k = used(h); k.fh_used = meter.PLANS["starter"]["five_hour"]; store.put(gateway.app.state.conn, k)  # 5-hour full
        r = await gw.post("/v1/chat/completions", json=ask, headers=auth)
        assert r.status_code == 429 and r.json()["error"]["window"] == "five_hour"
        raw2, _ = add_key(paid_until=int(time.time()) - 1)                           # 30 days over
        r = await gw.post("/v1/chat/completions", json=ask, headers={"Authorization": f"Bearer {raw2}"})
        assert r.status_code == 402 and r.json()["error"]["code"] == "plan_expired"
        raw3, _ = add_key("payg", balance_micros=0)                                    # empty balance
        r = await gw.post("/v1/chat/completions", json=ask, headers={"Authorization": f"Bearer {raw3}"})
        assert r.status_code == 402 and r.json()["error"]["code"] == "balance_empty"
        raw4, h4 = add_key(revoked=1)                                                  # revoked
        r = await gw.post("/v1/chat/completions", json=ask, headers={"Authorization": f"Bearer {raw4}"})
        assert r.status_code == 401
        assert len(seen) == n                                                          # none reached the kitchen

        raw5, h5 = add_key("payg", balance_micros=5_000_000)                           # pay-as-you-go
        r = await gw.post("/v1/chat/completions", json=ask, headers={"Authorization": f"Bearer {raw5}"})
        assert r.status_code == 200 and used(h5).balance_micros == 5_000_000 - 5 * 10 - 80

        r = await gw.post("/v1/session", json={"key": raw5})                         # the panel
        assert r.status_code == 200 and r.json()["balance"] == {"currency": "USD", "minor": 499, "used_percent": 0.0}
        assert r.json()["key_hint"] == "••••" + raw5[-4:]
        raw6, _ = add_key("payg", balance_micros=2_000_000, balance_cap=5_000_000)    # $2 left of a $5 tank
        r = await gw.post("/v1/session", json={"key": raw6})                          # bar reads 60% spent
        assert r.json()["balance"] == {"currency": "USD", "minor": 200, "used_percent": 60.0}
        r = await gw.get("/v1/usage", headers={"Authorization": f"Bearer {raw}"})
        assert [w["used_percent"] for w in r.json()["windows"]] == [100.0, round(100 * used(h).wk_used / meter.PLANS["starter"]["weekly"], 1)]
        assert '"limit"' not in r.text and '"used"' not in r.text                    # token numbers stay private
        assert (await gw.post("/v1/session", json={"key": "rb_nope"})).status_code == 401
        assert (await gw.post("/v1/session", content=b"junk")).status_code == 401

        gateway.hits.clear()
        codes = [(await gw.post("/v1/session", json={"key": "rb_nope"})).status_code for _ in range(21)]
        assert codes[:20] == [401] * 20 and codes[20] == 429                           # guessing slowed

        gateway.hits.clear()
        raw6, _ = add_key("max")
        n = gateway.PER_KEY_PER_MINUTE
        codes = [(await gw.post("/v1/chat/completions", json=ask,
                                headers={"Authorization": f"Bearer {raw6}"})).status_code for _ in range(n + 1)]
        assert codes[:n] == [200] * n and codes[n] == 429                              # flood on one key

        gateway.hits.clear()                                                          # kitchen down
        gateway.app.state.client = httpx.AsyncClient()
        gateway.UPSTREAMS = {"sherlock": "http://127.0.0.1:9"}
        r = await gw.post("/v1/chat/completions", json=ask, headers={"Authorization": f"Bearer {raw6}"})
        assert r.status_code == 502 and r.json()["error"]["code"] == "model_unreachable"

    tables = [t for (t,) in gateway.app.state.conn.execute("SELECT name FROM sqlite_master WHERE type='table'")]
    assert tables == ["keys", "orders"]                                                # nothing else is kept
    assert "hi" not in str(gateway.app.state.conn.execute("SELECT * FROM keys").fetchall())


def test_end_to_end():
    asyncio.run(run_e2e())


def test_stats():
    """The owner's anonymous hourly totals: split by plan and source, refusals by type, speed against the bar, no key
    and no content stored, and a broken stats file never costs a customer a reply."""
    async def run():
        fresh_gateway()
        gateway.app.state.client = httpx.AsyncClient(transport=httpx.ASGITransport(app=kitchen))
        real_search = gateway.run_search
        async def fake_search(q, limit=5):
            return [{"title": "t", "url": "https://example.com", "snippet": ""}]
        gateway.run_search = fake_search
        try:
            async with httpx.AsyncClient(transport=httpx.ASGITransport(app=gateway.app), base_url="http://gw") as gw:
                raw, h = add_key("power")
                auth = {"Authorization": f"Bearer {raw}"}
                browser = {**auth, "Origin": "https://redbeard.sbs"}
                ask = {"model": "sherlock", "messages": [{"role": "user", "content": "secret words"}]}
                assert (await gw.post("/v1/chat/completions", json=ask, headers=auth)).status_code == 200
                assert (await gw.post("/v1/chat/completions", json={**ask, "stream": True}, headers=browser)).status_code == 200
                assert (await gw.get("/v1/web_search?q=secret", headers=browser)).status_code == 200
                k = used(h); k.fh_used = meter.PLANS["power"]["five_hour"]; store.put(gateway.app.state.conn, k)
                assert (await gw.post("/v1/chat/completions", json=ask, headers=auth)).status_code == 429
                raw2, _ = add_key("max", paid_until=int(time.time()) - 1)
                await gw.post("/v1/chat/completions", json=ask, headers={"Authorization": f"Bearer {raw2}"})

                hourly, load = stats.rows(0)
                by = {(r["plan"], r["source"]): r for r in hourly}
                api, chat = by[("power", "api")], by[("power", "chat")]
                assert (api["requests"], api["prompt_tokens"], api["completion_tokens"], api["limit_5h"]) == (1, 80, 5, 1)
                assert (chat["requests"], chat["completion_tokens"], chat["waited"], chat["cut"]) == (1, 2, 1, 0)
                assert chat["searches"] == 1 and chat["gen_tokens"] == 0          # 2 tokens: too short for a speed
                assert by[("max", "api")]["expired"] == 1 and by[("max", "api")]["requests"] == 0
                assert len(load) == 1 and load[0]["at_once"] == 1 and load[0]["keys"] == 1
                dump = str(stats.db().execute("SELECT * FROM hourly").fetchall() + load)
                assert h not in dump and raw not in dump and "secret" not in dump   # no key, no content
                assert [r[1] for r in stats.db().execute("PRAGMA table_info(hourly)")] == ["hour", "plan", "source", *stats.COUNTS]

                stats.finish("max", "api", 10, 100, waited=0.5, writing=5.0)     # 20 tokens a second: slow
                stats.finish("max", "api", 10, 400, waited=0.5, writing=5.0)     # 80 a second: fast
                r = {(x["plan"], x["source"]): x for x in stats.rows(0)[0]}[("max", "api")]
                assert (r["gen_tokens"], r["gen_ms"], r["timed"], r["slow"], r["wait_ms"], r["waited"]) == (500, 10_000, 2, 1, 1000, 2)

                t1, t2 = stats.start("a"), stats.start("b")                      # two replies at once
                stats.end(t1); stats.end(t2); stats.end(t2)                      # ending twice is harmless
                assert stats.rows(0)[1][0]["at_once"] == 2 and stats.rows(0)[1][0]["keys"] == 3
                stats.start("c", now=time.time() - 2 * stats.STALE)              # a reply that never reported back...
                stats.start("d")
                assert len(stats._open) == 1                                     # ...is dropped, not counted forever

                stats._db = sqlite3.connect(":memory:")                          # broken stats file: no tables
                raw3, h3 = add_key("power")
                for stream in (True, False):
                    r = await gw.post("/v1/chat/completions", json={**ask, "stream": stream},
                                      headers={"Authorization": f"Bearer {raw3}", "Origin": "x"})
                    assert r.status_code == 200 and "Hel" in r.text              # the customer still gets the reply
                assert used(h3).fh_used > 0                                      # and is still charged
        finally:
            gateway.run_search = real_search
    asyncio.run(run())


def test_uptime():
    """The /v1/uptime feed: 90 slots, today reflects the samples, days with no sample read as null (never faked up)."""
    import sqlite3
    from datetime import datetime, timezone
    gateway.app.state.uptime = sqlite3.connect(":memory:")
    gateway.app.state.uptime.execute("CREATE TABLE uptime(day TEXT, component TEXT, up INT, total INT, "
                                      "PRIMARY KEY(day, component))")
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    gateway.app.state.uptime.execute("INSERT INTO uptime VALUES(?,?,?,?)", (today, "sherlock", 280, 288))
    gateway.app.state.uptime.commit()
    gateway.hits.clear()

    async def run():
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=gateway.app), base_url="http://gw") as gw:
            j = (await gw.get("/v1/uptime")).json()
            assert j["days"] == 90
            sh = next(c for c in j["components"] if c["id"] == "sherlock")
            assert len(sh["bars"]) == 90
            assert sh["bars"][-1]["pct"] == round(280 / 288, 4)      # today, from the sample
            assert sh["bars"][0]["pct"] is None                      # 90 days ago, no sample -> blank
            assert sh["uptime_pct"] == round(100 * 280 / 288, 2)
    asyncio.run(run())
    gateway.app.state.uptime.close()
    gateway.app.state.uptime = None


def serve(asgi, port):
    import uvicorn
    server = uvicorn.Server(uvicorn.Config(asgi, host="127.0.0.1", port=port, log_level="error", access_log=False))
    threading.Thread(target=server.run, daemon=True).start()
    while not server.started:
        time.sleep(0.05)
    return server


def test_hang_up_mid_reply():
    """Real sockets: the reply streams through piece by piece, and a customer who leaves is still charged."""
    fresh_gateway()
    kitchen_mode["slow"] = 0.05
    gateway.app.state.client = None                                  # a real client, made inside the server
    gateway.UPSTREAMS = {"sherlock": "http://127.0.0.1:18952"}
    servers = [serve(kitchen, 18952), serve(gateway.app, 18951)]
    try:
        raw, h = add_key()
        ask = {"model": "sherlock", "messages": [{"role": "user", "content": "hi"}], "stream": True}
        started = time.time()
        with httpx.stream("POST", "http://127.0.0.1:18951/v1/chat/completions", json=ask,
                          headers={"Authorization": f"Bearer {raw}"}, timeout=10) as r:
            first = next(r.iter_raw())
        assert b"Hel" in first and time.time() - started < 1.5      # arrived before the kitchen finished
        for _ in range(60):                                          # the gateway notices the hang-up
            if used(h).fh_used > 0:
                break
            time.sleep(0.05)
        assert 0 < used(h).fh_used < 100                             # an estimate; the full reply would be 1010
    finally:
        for s in servers:
            s.should_exit = True


fake_bitcart = FastAPI()
invoices, bitcart_seen = {}, []


bitcart_mode = {"no_wallet": False}


@fake_bitcart.post("/invoices")
async def fake_create_invoice(request: Request):
    from fastapi.responses import JSONResponse
    if bitcart_mode["no_wallet"]:
        return JSONResponse({"detail": "No wallet linked"}, status_code=422)   # real Bitcart's answer, 2026-10-22
    body = await request.json()
    bitcart_seen.append((request.headers.get("authorization"), body))
    iid = f"inv{len(invoices) + 1}"
    invoices[iid] = {"id": iid, "status": "pending", "exception_status": "none", "time_left": 1200,
                     "price": body["price"], "payments": [
                         {"id": f"{iid}-btc", "name": "BTC", "symbol": "btc", "currency": "btc", "amount": "0.0012",
                          "payment_address": "bc1qtest", "payment_url": "bitcoin:bc1qtest?amount=0.0012", "contract": "",
                          "user_address": None},
                         {"id": f"{iid}-trx", "name": "USDT", "symbol": "usdt", "currency": "trx", "amount": "100",
                          "payment_address": "TTest", "payment_url": "tron:TTest", "contract": "TR7NHqje",
                          "user_address": None}]}
    return invoices[iid]


@fake_bitcart.patch("/invoices/{iid}/details")
async def fake_set_sender(iid: str, request: Request):          # mirrors Bitcart's update_payment_details
    from fastapi.responses import JSONResponse
    body = await request.json()
    p = next((p for p in invoices[iid]["payments"] if p["id"] == body["id"]), None)
    if p is None:
        return JSONResponse({"detail": "No such payment method found"}, status_code=404)
    if p["user_address"] is not None:
        return JSONResponse({"detail": "Can't update payment address once set"}, status_code=422)
    p["user_address"] = body["address"]
    return True


@fake_bitcart.get("/invoices/{iid}")
async def fake_get_invoice(iid: str):
    return invoices[iid]


@fake_bitcart.get("/invoices")
async def fake_list_invoices(offset: int = 0, limit: int = 100):
    items = sorted(invoices.values(), key=lambda i: i.get("created", ""), reverse=True)
    return {"count": len(items), "result": items[offset:offset + limit]}


async def run_checkout():
    import checkout
    fresh_gateway()
    invoices.clear(); bitcart_seen.clear()
    checkout.STORE_ID, checkout.PUBLIC_BASE = "store1", "https://rb.example"
    checkout._client = httpx.AsyncClient(transport=httpx.ASGITransport(app=fake_bitcart), base_url="http://bitcart",
                                         headers={"Authorization": "Bearer gw-token"})
    gateway.app.state.client = httpx.AsyncClient(transport=httpx.ASGITransport(app=kitchen))
    conn = gateway.app.state.conn
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=gateway.app), base_url="http://gw") as gw:
        raw = store.new_raw_key()                       # made by the customer's browser; the server sees only its hash
        h = store.hash_key(raw)
        r = await gw.post("/v1/checkout", json={"key_hash": h, "plan": "power"})
        assert r.status_code == 200, r.text
        o = r.json()
        assert o["status"] == "waiting" and o["price_usd"] == 100 and [m["address"] for m in o["methods"]] == ["bc1qtest", "TTest"]
        assert [m["name"] for m in o["methods"]] == ["BTC", "USDT · TRON"] and o["methods"][1]["network"] == "TRON"

        sender = "TStub1Stub1Stub1Stub1Stub1Stub1Stu"                                    # dummy "pay-from" address
        url = f"/v1/checkout/{o['order_id']}/sender"
        assert (await gw.post(url, json={"chain": "trx", "address": "not-an-address"})).json()["error"]["code"] == "bad_address"
        r = await gw.post(url, json={"chain": "trx", "address": sender})
        assert r.status_code == 200 and r.json()["methods"][1]["sender"] == sender
        assert invoices["inv1"]["payments"][1]["user_address"] == sender                 # Bitcart was told
        assert (await gw.post(url, json={"chain": "trx", "address": sender})).status_code == 409   # set once
        assert (await gw.post(url, json={"chain": "eth", "address": "0x" + "a" * 40})).json()["error"]["code"] == "bad_network"
        assert (await gw.post("/v1/checkout/nope/sender", json={"chain": "trx", "address": sender})).status_code == 404
        assert raw not in r.text and h not in r.text                                      # never echoed back
        auth, sent = bitcart_seen[-1]
        assert auth == "Bearer gw-token" and sent["price"] == "100.00" and sent["store_id"] == "store1"
        assert sent["notification_url"] == f"https://rb.example/v1/checkout/{o['order_id']}/notify"
        assert "buyer_email" not in sent                                                  # no email, ever
        assert sent["metadata"] == {"kind": "plan", "plan": "power", "renewal": False}    # the sale says what it was
        assert h not in json.dumps(sent)                                                  # never which key
        inv = invoices[conn.execute("SELECT invoice_id FROM orders").fetchone()[0]]

        assert (await gw.get(f"/v1/checkout/{o['order_id']}")).json()["status"] == "waiting"
        assert store.get(conn, h) is None                                                 # nothing before payment
        inv["status"] = "paid"                                                            # seen, not yet confirmed
        assert (await gw.get(f"/v1/checkout/{o['order_id']}")).json()["status"] == "confirming"
        assert store.get(conn, h) is None
        assert (await gw.post(f"/v1/checkout/{o['order_id']}/notify", json={"status": "complete"})).json() == {"ok": True}
        assert store.get(conn, h) is None                                                 # a ping alone credits nothing
        inv["status"] = "complete"
        r = await gw.get(f"/v1/checkout/{o['order_id']}")
        assert r.json()["status"] == "paid" and r.json()["methods"] == []
        k = store.get(conn, h)
        first = k.paid_until
        assert k.plan == "power" and abs(first - (time.time() + 30 * DAY)) < 60
        for _ in range(3):                                                                # polls + pings: still once
            await gw.get(f"/v1/checkout/{o['order_id']}")
            await gw.post(f"/v1/checkout/{o['order_id']}/notify")
        assert store.get(conn, h).paid_until == first

        r = await gw.post("/v1/chat/completions", headers={"Authorization": f"Bearer {raw}"},   # the bought key works
                          json={"model": "sherlock", "messages": [{"role": "user", "content": "hi"}]})
        assert r.status_code == 200

        o2 = (await gw.post("/v1/checkout", json={"key_hash": h, "plan": "power"})).json()   # renew early: stacks
        assert bitcart_seen[-1][1]["metadata"]["renewal"] is True
        invoices[f"inv{len(invoices)}"]["status"] = "complete"
        assert (await gw.get(f"/v1/checkout/{o2['order_id']}")).json()["status"] == "paid"
        assert store.get(conn, h).paid_until == first + 30 * DAY

        hp = store.hash_key(store.new_raw_key())                                          # pay-as-you-go top-up
        o3 = (await gw.post("/v1/checkout", json={"key_hash": hp, "plan": "payg", "amount_usd": 20})).json()
        assert o3["price_usd"] == 20 and bitcart_seen[-1][1]["price"] == "20.00"
        assert bitcart_seen[-1][1]["metadata"] == {"kind": "topup", "plan": "payg", "renewal": False}
        invoices[f"inv{len(invoices)}"]["status"] = "complete"
        await gw.get(f"/v1/checkout/{o3['order_id']}")
        assert store.get(conn, hp).plan == "payg" and store.get(conn, hp).balance_micros == 20_000_000
        assert store.get(conn, hp).balance_cap == 20_000_000              # the top-up filled the tank to full

        o4 = (await gw.post("/v1/checkout", json={"key_hash": store.hash_key("rb_x"), "plan": "starter"})).json()
        invoices[f"inv{len(invoices)}"].update(status="expired")                          # never paid
        assert (await gw.get(f"/v1/checkout/{o4['order_id']}")).json()["status"] == "expired"
        assert store.get(conn, store.hash_key("rb_x")) is None
        o5 = (await gw.post("/v1/checkout", json={"key_hash": store.hash_key("rb_y"), "plan": "max"})).json()
        invoices[f"inv{len(invoices)}"].update(status="pending", exception_status="paid_partial")
        r = (await gw.get(f"/v1/checkout/{o5['order_id']}")).json()
        assert r["underpaid"] is True and store.get(conn, store.hash_key("rb_y")) is None

        gateway.hits.clear()
        bad = [{"key_hash": "abc", "plan": "power"}, {"key_hash": h, "plan": "gold"},
               {"key_hash": hp, "plan": "power"}, {"key_hash": h, "plan": "payg", "amount_usd": 20},
               {"key_hash": hp, "plan": "payg", "amount_usd": 1}, {"key_hash": hp, "plan": "payg", "amount_usd": "x"},
               {"key_hash": hp, "plan": "payg", "amount_usd": 5000}]
        codes = [(await gw.post("/v1/checkout", json=b)).status_code for b in bad]
        assert codes == [400, 400, 409, 409, 400, 400, 400], codes
        assert (await gw.post("/v1/checkout", content=b"junk")).status_code == 400
        assert (await gw.get("/v1/checkout/no-such-order")).status_code == 404

        import admin                                                                      # support fixes a short payment
        assert "NOT credited" in admin.main(["order", o5["order_id"]], conn)
        assert ", credited" in admin.main(["credit", o5["order_id"]], conn)
        assert store.get(conn, store.hash_key("rb_y")).plan == "max"
        try:
            admin.main(["credit", o5["order_id"]], conn)
            raise AssertionError("an order was credited twice")
        except ValueError:
            pass

        count = lambda done: conn.execute("SELECT count(*) FROM orders WHERE done = ?", (done,)).fetchone()[0]
        unfinished = count(0)                                                             # the expired one (o4)
        gateway.hits.clear()
        conn.execute("UPDATE orders SET created = created - ?", (2 * DAY,))                # 2 days later:
        await gw.post("/v1/checkout", json={"key_hash": store.hash_key("rb_z"), "plan": "starter"})
        assert count(1) == 0 and count(0) == unfinished + 1                               # finished links gone, unfinished kept
        conn.execute("UPDATE orders SET created = created - ?", (8 * DAY,))                # 8 more days: all past a week
        await gw.post("/v1/checkout", json={"key_hash": store.hash_key("rb_w"), "plan": "starter"})
        assert count(1) + count(0) == 1                                                   # only today's order is left
        assert store.get(conn, h).paid_until == first + 30 * DAY                           # the keys stay

        gateway.hits.clear()
        codes = [(await gw.post("/v1/checkout", json={"key_hash": store.hash_key(f"rb_{i}"), "plan": "starter"})).status_code
                 for i in range(11)]
        assert codes[:10] == [200] * 10 and codes[10] == 429                              # invoice spam slowed

        gateway.hits.clear()
        bitcart_mode["no_wallet"] = True                                                  # no wallet connected yet
        r = await gw.post("/v1/checkout", json={"key_hash": store.hash_key("rb_n"), "plan": "starter"})
        bitcart_mode["no_wallet"] = False
        assert r.status_code == 503 and r.json()["error"]["code"] == "payments_not_open"

        checkout._client = httpx.AsyncClient(base_url="http://127.0.0.1:9")               # Bitcart down
        gateway.hits.clear()
        r = await gw.post("/v1/checkout", json={"key_hash": store.hash_key("rb_d"), "plan": "starter"})
        assert r.status_code == 502 and r.json()["error"]["code"] == "payments_unavailable"
    checkout._client = None


def test_checkout():
    asyncio.run(run_checkout())


def test_backoffice():
    """The owner's page: locked without the admin key; money read from Bitcart (complete only, unknown never shown as
    0); keys, open orders and usage; the GPU estimate; and admin.py's commands through /v1/admin/run."""
    import hashlib
    from datetime import datetime, timezone
    import backoffice
    import checkout

    def bitcart_time(t):
        return datetime.fromtimestamp(t, timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%fZ")

    def inv(iid, status, price, created, paid=None, chain="bnb", meta=None, exc="none", sent=0.0):
        invoices[iid] = {"id": iid, "order_id": "o" + iid, "status": status, "exception_status": exc,
                         "price": f"{price:.2f}", "created": bitcart_time(created),
                         "paid_date": bitcart_time(paid) if paid else None, "metadata": meta or {}, "sent_amount": sent,
                         "tx_hashes": ["0xabc"] if paid else [], "payments": [
                             {"currency": c, "payment_address": a, "contract": "c-" + c, "is_used": c == chain and bool(paid)}
                             for c, a in (("trx", "TAddr"), ("bnb", "0xaddr"), ("eth", "0xaddr"))]}

    async def fake_balance(chain, address, contract):
        if chain == "eth":
            raise httpx.ConnectError("reader down")
        return 12.5

    async def run():
        fresh_gateway()
        invoices.clear()
        conn = gateway.app.state.conn
        checkout._client = httpx.AsyncClient(transport=httpx.ASGITransport(app=fake_bitcart), base_url="http://bitcart")
        gateway.app.state.client = httpx.AsyncClient(transport=httpx.ASGITransport(app=kitchen))
        gateway.app.state.uptime = sqlite3.connect(":memory:")
        gateway.app.state.uptime.execute("CREATE TABLE uptime(day TEXT, component TEXT, up INT, total INT, "
                                         "PRIMARY KEY(day, component))")
        today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        gateway.app.state.uptime.execute("INSERT INTO uptime VALUES (?, 'sherlock', 12, 288)", (today,))  # 1 hour
        backoffice.balance, backoffice._wallets = fake_balance, (0.0, [])
        backoffice.ADMIN_HASH, backoffice.GPU_HOURLY = "", 2.0
        now = time.time()
        inv("a", "complete", 100, now - 2 * DAY, now - 2 * DAY + 120, meta={"kind": "plan", "plan": "power", "renewal": False})
        inv("b", "complete", 5, now - 40 * DAY, now - 40 * DAY + 60)                    # old, no metadata: guessed
        inv("c", "expired", 50, now - 3 * DAY)
        inv("d", "pending", 200, now - HOUR, exc="paid_partial", sent=150.0)            # underpaid: stuck
        inv("e", "complete", 50, now - 2 * HOUR, now - 2 * HOUR + 600, chain="trx")
        _, h_ok = add_key("power")
        add_key("max", paid_until=int(now) + 3 * DAY)                                    # ends within a week
        add_key("starter", paid_until=int(now) - DAY)                                    # ended
        add_key("payg", balance_micros=4_990_000, balance_cap=5_000_000)
        add_key("power", revoked=1)
        for ref, iid, kind, plan, cents in (("od", "d", "plan", "max", 20000), ("oc", "c", "plan", "starter", 5000)):
            conn.execute("INSERT INTO orders (order_id, key_hash, kind, plan, cents, invoice_id, created) VALUES"
                         " (?, ?, ?, ?, ?, ?, ?)", (ref, "f" * 64, kind, plan, cents, iid, int(now)))
        stats.add("power", "api", prompt_tokens=800, completion_tokens=900)              # counts 1,000
        stats.add("max", "chat", completion_tokens=3000)                                 # counts 3,000

        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=gateway.app), base_url="http://gw") as gw:
            assert (await gw.get("/v1/admin/overview")).status_code == 404               # no admin key set: not there
            backoffice.ADMIN_HASH = hashlib.sha256(b"rb_admin_test").hexdigest()
            for bad in ({}, {"Authorization": "Bearer rb_admin_wrong"}, {"Authorization": "rb_admin_test"}):
                assert (await gw.get("/v1/admin/overview", headers=bad)).status_code == 401
            raw_customer, _ = add_key("max")                                             # a customer key is not admin
            assert (await gw.get("/v1/admin/overview", headers={"Authorization": f"Bearer {raw_customer}"})).status_code == 401
            ok = {"Authorization": "Bearer rb_admin_test"}
            r = await gw.get("/v1/admin/overview", headers=ok)
            assert r.status_code == 200, r.text
            o = r.json()
            s = o["summary"]
            assert s["revenue"] == {"24h": 50.0, "7d": 150.0, "30d": 150.0, "all": 155.0}, s["revenue"]
            assert s["funnel"] == {"started": 4, "paid": 2, "expired": 1, "underpaid": 1, "minutes_to_pay": 6.0}, s["funnel"]
            assert s["keys"]["active"] == {"power": 1, "max": 2, "payg": 1} and s["keys"]["ending_7d"] == 1
            assert s["keys"]["ended"] == 1 and s["keys"]["payg_owed_usd"] == 4.99
            assert s["gpu"] == {"hours_30d": 1.0, "hourly": 2.0, "cost_30d": 2.0, "margin_30d": 148.0}, s["gpu"]
            assert s["plans"]["power"] == {"active_keys": 1, "tokens_30d": 1000, "revenue_30d": 100.0, "cost_30d": 0.5}
            assert s["plans"]["max"]["cost_30d"] == 1.5 and s["plans"]["starter"]["cost_30d"] == 0.0
            by = {x["id"]: x for x in o["sales"]}
            assert [x["id"] for x in o["sales"]] == ["d", "e", "a", "c", "b"]                # newest first
            assert by["a"]["plan"] == "power" and not by["a"]["guessed"] and by["a"]["network"] == "BNB Smart Chain"
            assert by["a"]["tx_url"] == "https://bscscan.com/tx/0xabc"
            assert by["e"]["tx_url"] == "https://tronscan.org/#/transaction/0xabc"
            assert by["b"]["plan"] == "payg" and by["b"]["guessed"] and by["d"]["underpaid"]
            assert [(w["chain"], w["usdt"]) for w in o["wallets"]] == [("trx", 12.5), ("bnb", 12.5), ("eth", None)]
            orders = {x["ref"]: x for x in o["orders"]}
            assert orders["od"]["stuck"] and orders["od"]["sent"] == 150.0 and not orders["oc"]["stuck"]
            assert "f" * 64 not in json.dumps(o["orders"]) and "f" * 64 not in json.dumps(o["sales"])  # a sale never names a key
            assert {k["status"] for k in o["keys"]} == {"active", "ended", "revoked"} and len(o["keys"]) == 6
            assert any(k["fp"] == h_ok for k in o["keys"])
            assert {(x["plan"], x["source"]) for x in o["hourly"]} == {("power", "api"), ("max", "chat")}
            assert o["health"]["bitcart"] is True

            checkout._client = httpx.AsyncClient(base_url="http://127.0.0.1:9")          # Bitcart down
            o = (await gw.get("/v1/admin/overview", headers=ok)).json()
            assert o["sales"] is None and o["summary"]["revenue"] is None and o["health"]["bitcart"] is False
            assert o["summary"]["gpu"]["margin_30d"] is None                              # unknown, never "$0"
            assert len(o["keys"]) == 6                                                    # the rest still shows

            run_ = lambda args: gw.post("/v1/admin/run", json={"args": args}, headers=ok)
            r = await run_(["new", "power"])
            raw = r.json()["output"].splitlines()[0]
            assert r.status_code == 200 and raw.startswith("rb_") and store.get(conn, store.hash_key(raw)).plan == "power"
            first = store.get(conn, store.hash_key(raw)).paid_until
            assert (await run_(["extend", store.hash_key(raw)])).status_code == 200
            assert store.get(conn, store.hash_key(raw)).paid_until == first + 30 * DAY
            assert (await run_(["revoke", store.hash_key(raw)])).status_code == 200
            assert store.get(conn, store.hash_key(raw)).revoked == 1
            assert "credited" in (await run_(["credit", "od"])).json()["output"]
            assert store.get(conn, "f" * 64).plan == "max"
            assert (await run_(["credit", "od"])).json()["error"]["code"] == "refused"   # never twice
            for bad in (["show", raw], ["delete", raw], ["new"], ["new", "free"], ["new", "payg", "-5"], [1, 2], "new"):
                assert (await gw.post("/v1/admin/run", json={"args": bad}, headers=ok)).status_code == 400, bad
            assert (await gw.post("/v1/admin/run", content=b"junk", headers=ok)).status_code == 400
            assert (await gw.post("/v1/admin/run", json={"args": ["new", "max"]})).status_code == 401  # no key

            gateway.hits.clear()
            codes = [(await gw.get("/v1/admin/overview", headers={"Authorization": "Bearer x"})).status_code
                     for _ in range(31)]
            assert codes[:30] == [401] * 30 and codes[30] == 429                          # guessing slowed

    real_balance = backoffice.balance
    try:
        asyncio.run(run())
    finally:
        backoffice.balance, backoffice.ADMIN_HASH, backoffice._wallets = real_balance, "", (0.0, [])
        checkout._client = None
        gateway.app.state.uptime = None


def test_admin():
    import admin
    conn = store.connect(":memory:")
    out = admin.main(["new", "power"], conn)
    raw = out.splitlines()[0]
    k = store.get(conn, store.hash_key(raw))
    assert k.plan == "power" and k.paid_until > time.time() + 29 * DAY
    first = k.paid_until
    admin.main(["extend", raw], conn)
    assert store.get(conn, k.key_hash).paid_until == first + 30 * DAY                 # stacks
    admin.main(["revoke", k.key_hash], conn)                                          # by fingerprint too
    assert store.get(conn, k.key_hash).revoked == 1
    raw_p = admin.main(["new", "payg", "20"], conn).splitlines()[0]
    admin.main(["topup", raw_p, "5.50"], conn)
    assert store.get(conn, store.hash_key(raw_p)).balance_micros == 25_500_000
    for bad in (["new", "free"], ["new", "payg"], ["new", "payg", "-5"], ["new", "payg", "abc"],
                ["topup", raw, "5"], ["extend", raw_p], ["show", "rb_nope"], ["delete", raw], []):
        try:
            admin.main(bad, conn)
            raise AssertionError(f"accepted {bad}")
        except ValueError:
            pass


if __name__ == "__main__":
    import sys
    failed = 0
    for name, fn in list(globals().items()):
        if name.startswith("test_"):
            try:
                fn()
                print("PASS", name)
            except Exception as e:  # report every test, not just the first failure
                failed += 1
                print("FAIL", name, type(e).__name__, e)
    sys.exit(1 if failed else 0)
