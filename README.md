# RedBeard gateway

The gateway behind RedBeard: a small, auditable proxy that sits in front of OpenAI-compatible model servers. It is
open so anyone can verify the one promise that matters most: **it never stores your conversations.**

## What it does

- Authenticates a request by a key's SHA-256 fingerprint and applies the plan's usage limits.
- Forwards the request to a model server and streams the reply straight back to you.
- Writes **only counters**: the key's usage, and anonymous hourly totals for the whole service. Your prompt and the
  model's reply are never written to disk.
- Handles crypto checkout (through a Bitcart instance) and issues or extends keys.
- Runs web search and page reading for the website chat (`/v1/web_search`, `/v1/fetch`); only the query or the URL
  leaves the server.
- Serves the owner's back office (`/v1/admin/*`, `backoffice.py`), locked by one admin key kept only as its SHA-256.

## What it stores, in full

Two SQLite tables in the keys file (created in `store.py`):

- `keys` — a key's SHA-256 hash (never the raw key), plan, usage counters, paid-until, prepaid balance.
- `orders` — a payment reference linking an invoice to a key fingerprint. Cleared at the next checkout once it is a
  day old and finished, or a week old if unfinished (so a late payment can still be credited).

And one separate file of hourly totals (`stats.py`, `REDBEARD_STATS_DB`): one row per hour, per plan, per route
(website chat or API), holding only counts and sums (replies, tokens, seconds, limits reached, searches), plus the
most replies at once and how many keys were active in that hour, as numbers. No key, no fingerprint, no text; rows
older than 400 days are deleted.

There is no table, and no column, for message content. `test_gateway.py` asserts exactly this: it sends a
conversation through, then checks the text appears in no table, and that neither the text nor the key's fingerprint
appears in the hourly totals.

## Run it

    pip install -r requirements.txt
    cp plans.example.json plans.json      # then set your real per-plan limits (plans.json is gitignored)
    python -m uvicorn app:app --host 127.0.0.1 --port 8041 --no-access-log
    python test_gateway.py                # 13 checks, fully offline
    python -m unittest test_reader -v      # page sections, continuation and read failures

## Page reading

`GET /v1/fetch?url=...` returns up to 6,000 characters of extracted page text in `content`, plus `offset`,
`end_offset`, `total_chars`, `has_more`, `next_offset`, `page_id` and `next_read`. When `has_more` is true,
pass the returned `next_read` fields as query parameters to retrieve the next section. Callers can stop
as soon as they have the text they need. A result only describes the reader's extracted text.

The page is fetched again for each section; no page cache or content-storage table is added. Send the
previous `page_id` to detect a changed extraction: a mismatch returns `409 page_changed` with restart
arguments. Invalid offsets and failed reads return explicit errors. Existing URL-only calls still
receive their first section in the `content` field.

## Configuration

Everything secret or environment-specific is read from the environment; **there are no credentials in this code.**

- `REDBEARD_UPSTREAMS` — JSON mapping a model name to its server base URL.
- `REDBEARD_UPSTREAM_KEY` — bearer token for the model server, if it needs one.
- `REDBEARD_INFERENCE_MODE=api` — include aggregate API expense estimates in the back office. Only an additional numeric
  hourly total is recorded. Historical GPU samples are retained under `REDBEARD_RETIRED_COMPONENT`.
- `REDBEARD_PLANS` (JSON) or a local `plans.json` — the per-plan token windows.
- `REDBEARD_DB` — path to the SQLite keys file.
- `BITCART_API`, `BITCART_HOST_HEADER`, `BITCART_TOKEN`, `BITCART_STORE_ID`, `REDBEARD_PUBLIC_BASE` — crypto checkout.
- `REDBEARD_STATS_DB` — path to the hourly-totals file.
- `REDBEARD_ADMIN_HASH` — SHA-256 of the admin key (unset: the back office answers 404); `REDBEARD_GPU_HOURLY` —
  the GPU box's price an hour, for the back office's cost estimate.
- `REDBEARD_SEARXNG` — optional: a SearXNG instance (self-hosted metasearch) to search with; DuckDuckGo's HTML page
  is the fallback when it is unset, down or finds nothing.
- `REDBEARD_BRAVE_KEY` — optional: web search through Brave's paid API instead (takes precedence when set).

## License

MIT. See `LICENSE`.
