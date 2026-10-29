"""RedBeard's own page reader: a web address in, readable text out, with no outside service. The owner, 2026-10-29: no
Jina or any other reading company ("that could control the speed, and quality, and doesnt hold our privacy").

  plain fetch, as a normal browser asks  ->  PDF: pypdf  |  JSON / plain text: as it is
                                         ->  web page: the article (trafilatura) or the whole page minus menus
                                             (html2text), whichever keeps the answer
  the plain page empty or a bot check    ->  a hidden Chromium renders it, the same choice on what it shows
  a wall even the browser can't pass     ->  "" (the caller answers "blocked, try another source")
  a PyPI package page                    ->  PyPI's own JSON door instead (the page is behind a bot check; PyPI's
                                             rules: reading through it "is not scraping"), as a short page

The choice, measured on 8 pages (.verification/reader-test/eval_modes.py, 2026-10-29): the article when it keeps at
least half of the whole page's text (real articles: 20-45% smaller, answer kept); otherwise the whole page (GitHub
releases, NVD, forums, npm), where the article extractor drops what matters.
"""
import asyncio
import io
import json
import re

import html2text
import lxml.html
import trafilatura
from pypdf import PdfReader

UA = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0 Safari/537.36"
HEADERS = {"User-Agent": UA, "Accept-Language": "en-US,en;q=0.9",
           "Accept": "text/html,application/xhtml+xml,application/pdf;q=0.9,application/json;q=0.8,*/*;q=0.7"}
ENOUGH = 500                  # characters: less than this from the plain page and the browser gets a turn
SCRAPS = 400                  # characters: even after the browser, less than this is a wall or an empty shell
MENUS = ("//nav|//header|//footer|//aside|//script|//style|//noscript|//template"
         "|//*[@role='navigation' or @role='banner' or @role='contentinfo']")
# a bot check or a firewall error in place of the page. The second line came from the own-reader sweep (2026-10-29):
# PyPI "Client Challenge", Reddit "Prove your humanity", Exploit-DB's Sucuri firewall error, DuckDuckGo's notice.
WALL = re.compile(r"<title>\s*(Just a moment|Attention Required|Access denied|Please Wait|Verifying)|challenge-platform"
                  r"|cf-chl-|Enable JavaScript and cookies to continue|Please enable JS and disable any ad blocker"
                  r"|Client Challenge|JavaScript is disabled in your browser|Prove your humanity"
                  r"|Sucuri Website Firewall|If this persists, please email us", re.I)


# pypi.org/project/NAME[/VERSION]/ and its JSON twin pypi.org/pypi/NAME[/VERSION]/json
PYPI = re.compile(r"https?://pypi\.org/(?:project|pypi)/([^/?#]+)(?:/([^/?#]+))?(?:/json)?/?(?:[?#].*)?$", re.I)


def walled(status: int, html: str) -> bool:
    return status in (401, 403, 429, 503) or bool(WALL.search(html[:30000]))


def article(html: str, url: str) -> str:
    return trafilatura.extract(html, url=url, output_format="markdown", include_links=True, include_tables=True,
                               include_formatting=True, favor_recall=True) or ""


def whole(html: str, url: str) -> tuple[str, str]:
    """(title, the page minus menus as markdown, links kept and made absolute)."""
    try:
        doc = lxml.html.fromstring(html, base_url=url)
    except Exception:
        return "", ""
    title = " ".join((doc.findtext(".//title") or "").split())
    for el in doc.xpath(MENUS):
        el.drop_tree()
    doc.make_links_absolute(url, resolve_base_href=True)
    h = html2text.HTML2Text()
    h.ignore_images, h.body_width = True, 0
    return title, re.sub(r"\n{3,}", "\n\n", h.handle(lxml.html.tostring(doc, encoding="unicode"))).strip()


def from_html(html: str, url: str) -> str:
    title, page = whole(html, url)
    art = article(html, url).strip()
    text = art if art and len(art) >= len(page) / 2 else page
    return f"Title: {title}\n\n{text}" if title and text else text


def pdf_text(data: bytes) -> str:
    try:
        return "\n\n".join((p.extract_text() or "").strip() for p in PdfReader(io.BytesIO(data)).pages).strip()
    except Exception:
        return ""


def pypi_text(data: bytes) -> str:
    """PyPI's JSON answer as a short page: the facts first, then the versions, the files with their links and the
    README last. The raw answer lists every file of every version (numpy: 3.9 million characters)."""
    try:
        d = json.loads(data)
        i = d["info"]
        name, ver = i["name"], i["version"]
        out = [f"Title: {name} {ver} on PyPI", "", i.get("summary") or "", "",
               f"Version: {ver}" + (f" (YANKED: {i.get('yanked_reason') or 'no reason given'})" if i.get("yanked") else ""),
               f"Install: pip install {name}=={ver}", f"Page: {i.get('release_url') or i.get('package_url') or ''}"]
        if i.get("requires_python"):
            out.append(f"Requires Python: {i['requires_python']}")
        licence = (i.get("license_expression") or i.get("license") or "").strip().split("\n")[0][:100]
        if licence:
            out.append(f"License: {licence}")
        out += [f"{label}: {link}" for label, link in (i.get("project_urls") or {}).items()]
        if i.get("requires_dist"):
            out.append("Depends on: " + "; ".join(i["requires_dist"]))
        vulns = d.get("vulnerabilities") or []
        out += ["", f"Known vulnerabilities in this version (as listed by PyPI): {len(vulns) or 'none'}"]
        for v in vulns:
            details = " ".join((v.get("details") or "").split())[:300]
            out.append(f"- {v.get('id')} ({', '.join(v.get('aliases') or [])}), fixed in "
                       f"{', '.join(v.get('fixed_in') or []) or 'no fix listed'}: {v.get('link') or ''} | {details}")
        if releases := d.get("releases"):  # only on the project's answer, not a single version's
            newest = sorted(releases, key=lambda v: max((f.get("upload_time_iso_8601") or "" for f in releases[v]),
                                                        default=""), reverse=True)
            out += ["", f"All versions, newest first ({len(newest)}): " + ", ".join(
                v + (" (yanked)" if releases[v] and all(f.get("yanked") for f in releases[v]) else "") for v in newest)]
        out += ["", f"Files of {ver} ({len(d.get('urls') or [])}):"]
        out += [f"- {f['filename']} ({(f.get('size') or 0) // 1024:,} KB, {(f.get('upload_time_iso_8601') or '')[:10]}"
                f"{', yanked' if f.get('yanked') else ''}): {f['url']}" for f in d.get("urls") or []]
        if i.get("description"):
            out += ["", "## Description", "", i["description"].strip()]
        return "\n".join(out).strip()
    except Exception:
        return ""  # not PyPI's usual answer (a wall, a changed format): say "can't read" rather than guess


async def pypi(name: str, version: str | None, client) -> str:
    version = None if (version or "").lower() == "json" else version  # pypi.org/pypi/NAME/json has no version
    door = f"https://pypi.org/pypi/{name}/{version + '/' if version else ''}json"
    try:
        r = await client.get(door, headers=HEADERS, timeout=15, follow_redirects=True)
    except Exception:
        return ""
    return await asyncio.to_thread(pypi_text, r.content) if r.status_code == 200 else ""


async def read(url: str, client, render=None) -> str:
    """The page as text, or "" when it can't be read (blocked, not found, an unreadable file). `client` is an
    httpx.AsyncClient; `render(url) -> html` is the hidden browser (None: plain fetch only)."""
    if m := PYPI.match(url):
        return await pypi(m[1], m[2], client)
    try:
        r = await client.get(url, headers=HEADERS, timeout=15, follow_redirects=True)
        status, kind, body = r.status_code, r.headers.get("content-type", "").lower(), r.content
        page = r.text if kind.startswith(("text/", "application/xhtml", "application/json")) else ""  # its own charset
    except Exception:
        status, kind, body, page = 0, "", b"", ""
    text = ""
    if status == 200 and ("pdf" in kind or body[:5] == b"%PDF-"):
        return await asyncio.to_thread(pdf_text, body)  # heavy work off the main loop: replies keep streaming
    if status == 200 and kind.startswith(("application/json", "text/plain", "text/markdown")):
        return page.strip()
    if status == 200 and kind and not kind.startswith(("text/", "application/xhtml")):
        return ""  # a file we can't read (zip, installer, video): no use starting the browser
    html = page if kind.startswith(("text/html", "application/xhtml")) else ""
    if status == 200 and html and not walled(status, html):
        text = await asyncio.to_thread(from_html, html, url)
    if len(text) >= ENOUGH or render is None:
        return text
    try:
        shown = await render(url)
    except Exception:
        shown = ""
    if shown and not walled(200, shown):
        text = max(text, await asyncio.to_thread(from_html, shown, url), key=len)
    return text if len(text) >= SCRAPS else ""  # scraps are a wall or an empty shell: say "can't read" instead


class Browser:
    """One shared hidden Chromium, started on first use, at most `pages` pages at a time (today's shared server has
    ~1.5 GB free; RedBeard's own server will allow more). Images, video and fonts are never loaded."""

    def __init__(self, pages: int = 2):
        self.slots = asyncio.Semaphore(pages)
        self.playwright = self.browser = None

    async def render(self, url: str) -> str:
        async with self.slots:
            if self.browser is None or not self.browser.is_connected():
                from playwright.async_api import async_playwright  # only where the browser is installed
                self.playwright = self.playwright or await async_playwright().start()
                self.browser = await self.playwright.chromium.launch(headless=True, args=["--disable-dev-shm-usage"])
            context = await self.browser.new_context(user_agent=UA, locale="en-US")
            try:
                page = await context.new_page()
                await page.route("**/*", lambda r: r.abort() if r.request.resource_type in ("image", "media", "font")
                                 else r.continue_())
                await page.goto(url, wait_until="domcontentloaded", timeout=25000)
                try:
                    await page.wait_for_load_state("networkidle", timeout=8000)
                except Exception:
                    pass  # a page that never goes quiet: read what is there
                return await page.content()
            finally:
                await context.close()

    async def close(self):
        if self.browser:
            await self.browser.close()
        if self.playwright:
            await self.playwright.stop()
        self.playwright = self.browser = None
