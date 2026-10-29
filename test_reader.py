"""Reader checks. ReaderRouteTests: the /v1/fetch route (sections, page ids, the per-key snapshot, errors) with the
page text swapped in. OwnReaderTests: RedBeard's own reader (reader.py) on synthetic pages, with a stand-in browser."""
import hashlib
import json
import unittest

import httpx

import app as gateway
import reader
import stats
import store


class ReaderRouteTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.text = ("A readable line with an emoji 🙂 and an accent é.\n" * 400)
        self.reads = []
        gateway.hits.clear()
        gateway.snapshots.clear()
        gateway.app.state.conn = store.connect(":memory:")
        stats._db = stats.connect(":memory:")
        self.key, self.key2 = "synthetic-reader-test-key", "second-synthetic-reader-test-key"
        for k in (self.key, self.key2):
            store.put(gateway.app.state.conn, store.Key(key_hash=store.hash_key(k), plan="max"))
        self.real_read_page = gateway.read_page

        async def fake(url):
            self.reads.append(url)
            return self.text

        gateway.read_page = fake
        self.api = httpx.AsyncClient(transport=httpx.ASGITransport(app=gateway.app), base_url="http://local")

    async def asyncTearDown(self):
        gateway.read_page = self.real_read_page
        await self.api.aclose()
        gateway.app.state.conn.close()
        gateway.app.state.conn = None
        stats._db.close()
        stats._db = None
        gateway.hits.clear()
        gateway.snapshots.clear()

    async def read(self, key=None, **params):
        return await self.api.get("/v1/fetch", params={"url": "https://fixture.test/page", **params},
                                  headers={"Authorization": "Bearer " + (key or self.key)})

    async def test_sections_reconstruct_unicode_page_exactly(self):
        content, offset, page_id = [], 0, None
        while True:
            response = await self.read(offset=offset, **({"page_id": page_id} if page_id else {}))
            self.assertEqual(response.status_code, 200)
            page = response.json()
            self.assertEqual(page["offset"], offset)
            self.assertLessEqual(len(page["content"]), 6000)
            self.assertEqual(page["end_offset"], offset + len(page["content"]))
            self.assertEqual(page["total_chars"], len(self.text))
            self.assertEqual(page["page_id"], hashlib.sha256(self.text.encode()).hexdigest())
            content.append(page["content"])
            if not page["has_more"]:
                self.assertIsNone(page["next_offset"])
                self.assertIsNone(page["next_read"])
                break
            self.assertEqual(page["next_read"], {"url": "https://fixture.test/page", "offset": page["end_offset"], "page_id": page["page_id"]})
            offset, page_id = page["next_offset"], page["page_id"]
        self.assertEqual("".join(content), self.text)
        self.assertEqual(len(self.reads), 1)  # every later part came from the snapshot

    async def test_short_page_and_exact_end(self):
        self.text = "Short readable source. " * 20
        page = (await self.read()).json()
        self.assertEqual(page["content"], self.text)
        self.assertFalse(page["has_more"])
        end = (await self.read(offset=len(self.text))).json()
        self.assertEqual(end["content"], "")
        self.assertFalse(end["has_more"])

    async def test_invalid_offsets_rejected_before_fetch(self):
        for offset in ["-1", "1.5", "NaN", "", "1e3", "9" * 50]:
            response = await self.read(offset=offset)
            self.assertEqual(response.status_code, 400, offset)
            self.assertEqual(response.json()["error"]["code"], "invalid_offset")
        self.assertEqual(self.reads, [])

    async def test_invalid_url_and_page_id_rejected_before_fetch(self):
        for params in [{"url": "file:///etc/passwd"}, {"url": ""}, {"page_id": "wrong"}]:
            self.assertEqual((await self.read(**params)).status_code, 400)
        self.assertEqual(self.reads, [])

    async def test_offset_past_end_is_explicit_error(self):
        response = await self.read(offset=len(self.text) + 1)
        self.assertEqual(response.status_code, 400)
        self.assertEqual(response.json()["error"]["code"], "offset_out_of_range")

    async def test_continuation_reads_the_snapshot_even_if_the_page_changes(self):
        # GitHub's release list changed between parts on 2026-10-29 and the model had to start over
        original = self.text
        first = (await self.read()).json()
        self.text = "Changed introduction.\n" + self.text
        response = await self.read(offset=first["next_offset"], page_id=first["page_id"])
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json()["content"], original[6000:12000])
        self.assertEqual(len(self.reads), 1)

    async def test_changed_page_after_the_snapshot_expired_requires_restart(self):
        first = (await self.read()).json()
        self.text = "Changed introduction.\n" + self.text
        gateway.snapshots.clear()  # as after 10 minutes
        response = await self.read(offset=first["next_offset"], page_id=first["page_id"])
        self.assertEqual(response.status_code, 409)
        self.assertEqual(response.json()["error"]["code"], "page_changed")
        self.assertEqual(response.json()["error"]["next_read"]["offset"], 0)

    async def test_snapshots_are_never_shared_between_keys(self):
        await self.read()
        await self.read(key=self.key2, offset=6000)
        self.assertEqual(len(self.reads), 2)  # the second key fetched its own copy

    async def test_a_fresh_first_read_fetches_again(self):
        await self.read()
        await self.read()
        self.assertEqual(len(self.reads), 2)

    async def test_auth_required_without_reader_request(self):
        response = await self.api.get("/v1/fetch", params={"url": "https://fixture.test/page"})
        self.assertEqual(response.status_code, 401)
        self.assertEqual(self.reads, [])

    async def test_old_url_only_call_keeps_content_field(self):
        page = (await self.read()).json()
        self.assertEqual(page["content"], self.text[:6000])
        self.assertTrue(page["has_more"])
        self.assertEqual(await gateway.fetch_page("https://fixture.test/page"), self.text[:6000])

    async def test_unreadable_page_reports_failure(self):
        self.text = ""
        response = await self.read()
        self.assertEqual(response.status_code, 502)
        self.assertEqual(response.json()["error"]["code"], "page_unavailable")


def tiny_pdf(text: str) -> bytes:
    """A real one-page PDF holding `text` (built by hand: no PDF writer needed)."""
    stream = f"BT /F1 12 Tf 10 50 Td ({text}) Tj ET".encode()
    objects = [b"<< /Type /Catalog /Pages 2 0 R >>", b"<< /Type /Pages /Kids [3 0 R] /Count 1 >>",
               b"<< /Type /Page /Parent 2 0 R /MediaBox [0 0 400 100] /Contents 4 0 R "
               b"/Resources << /Font << /F1 5 0 R >> >> >>",
               b"<< /Length " + str(len(stream)).encode() + b" >>\nstream\n" + stream + b"\nendstream",
               b"<< /Type /Font /Subtype /Type1 /BaseFont /Helvetica >>"]
    out, offsets = bytearray(b"%PDF-1.4\n"), []
    for i, body in enumerate(objects, 1):
        offsets.append(len(out))
        out += f"{i} 0 obj\n".encode() + body + b"\nendobj\n"
    xref = len(out)
    out += f"xref\n0 {len(objects) + 1}\n0000000000 65535 f \n".encode()
    for off in offsets:
        out += f"{off:010d} 00000 n \n".encode()
    out += f"trailer << /Size {len(objects) + 1} /Root 1 0 R >>\nstartxref\n{xref}\n%%EOF".encode()
    return bytes(out)


MENU = "<nav>" + "<a href='/home'>Home</a> <a href='/login'>Login</a> <a href='/pricing'>Pricing</a> " * 30 + "</nav>"
ARTICLE = ("<html><head><title>Wireshark history</title></head><body>" + MENU + "<article><h1>Wireshark</h1>"
           + "<p>Wireshark began in 1998 under the name Ethereal and was renamed Wireshark in May 2006 after a "
             "trademark dispute. It lets you inspect the packets on a network in great detail.</p>" * 25
           + "</article><footer>Copyright footer text</footer></body></html>")
ASSETS = ("<html><head><title>Releases · frida/frida</title></head><body>" + MENU + "<main><h2>Frida 17.24.3</h2>"
          "<p>Latest</p><h3>Assets</h3><ul>"
          + "".join(f"<li><a href='/frida/frida/releases/download/17.24.3/frida-server-17.24.3-{arch}.xz'>"
                    f"frida-server-17.24.3-{arch}.xz</a> 12.4 MB</li>"
                    for arch in ["linux-x86_64", "linux-arm64", "android-arm64", "android-x86", "macos-arm64", "ios-arm64"] * 6)
          + "</ul></main></body></html>")
WALL = ("<html><head><title>Just a moment...</title></head><body><div id='challenge-platform'>Verify you are human"
        "</div></body></html>")
# PyPI's JSON answers, cut down to the fields the reader uses (the real ones were read 2026-10-29: requests, numpy)
SDIST = {"filename": "frida_tools-14.4.5.tar.gz", "url": "https://files.test/frida_tools-14.4.5.tar.gz",
         "size": 2048000, "upload_time_iso_8601": "2026-10-01T10:00:00Z", "yanked": False}
INFO = {"name": "frida-tools", "version": "14.4.5", "summary": "Frida CLI tools", "requires_python": ">=3.7",
        "license": None, "license_expression": "wxWindows", "project_urls": {"Homepage": "https://frida.test"},
        "requires_dist": ["frida<18,>=17", "prompt-toolkit"], "release_url": "https://pypi.org/project/frida-tools/14.4.5/",
        "yanked": False, "yanked_reason": None, "description": "Frida CLI tools README."}
VULN = {"id": "GHSA-test-0001", "aliases": ["CVE-2099-0001"], "fixed_in": ["14.4.6"], "link": "https://osv.test/1",
        "details": "A test hole."}
PYPI_VERSION = {"info": INFO, "urls": [SDIST], "vulnerabilities": [VULN]}
PYPI_PROJECT = {**PYPI_VERSION, "releases": {
    "14.0.0": [{**SDIST, "upload_time_iso_8601": "2025-01-01T00:00:00Z"}],
    "14.4.5": [SDIST],
    "14.4.4": [{**SDIST, "upload_time_iso_8601": "2026-09-01T00:00:00Z", "yanked": True}]}}


class OwnReaderTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.pages = {}       # url -> (status, content-type, body)
        self.shown = {}       # url -> what the stand-in browser shows (html, or an exception to raise)
        self.rendered = []

        async def handler(request):
            status, kind, body = self.pages.get(str(request.url), (404, "text/html", b"<html>Not found</html>"))
            return httpx.Response(status, content=body, headers={"content-type": kind})

        self.client = httpx.AsyncClient(transport=httpx.MockTransport(handler))

    async def asyncTearDown(self):
        await self.client.aclose()

    async def render(self, url):
        self.rendered.append(url)
        shown = self.shown.get(url, "")
        if isinstance(shown, Exception):
            raise shown
        return shown

    async def read(self, url, browser=True):
        return await reader.read(url, self.client, self.render if browser else None)

    def page(self, url, html, status=200, kind="text/html; charset=utf-8"):
        self.pages[url] = (status, kind, html.encode("utf-8") if isinstance(html, str) else html)

    async def test_article_page_keeps_the_article_not_the_menus(self):
        self.page("https://site.test/wireshark", ARTICLE)
        text = await self.read("https://site.test/wireshark")
        self.assertTrue(text.startswith("Title: Wireshark history"))
        self.assertIn("Ethereal", text)
        self.assertNotIn("Login", text)
        self.assertNotIn("Copyright footer", text)
        self.assertEqual(self.rendered, [])  # an ordinary page never needs the browser

    async def test_app_like_page_keeps_the_download_links(self):
        url = "https://github.test/frida/frida/releases"
        self.page(url, ASSETS)
        text = await self.read(url)
        self.assertIn("https://github.test/frida/frida/releases/download/17.24.3/frida-server-17.24.3-linux-x86_64.xz", text)
        self.assertNotIn("Login", text)

    async def test_pdf_becomes_text(self):
        self.page("https://arxiv.test/paper.pdf", tiny_pdf("Attention heads: 8"), kind="application/pdf")
        self.assertIn("Attention heads: 8", await self.read("https://arxiv.test/paper.pdf"))
        self.assertEqual(self.rendered, [])

    async def test_json_api_comes_back_as_it_is(self):
        self.page("https://api.test/react/latest", '{"name": "react", "version": "19.3.1"}', kind="application/json")
        self.assertEqual(await self.read("https://api.test/react/latest"), '{"name": "react", "version": "19.3.1"}')

    async def test_empty_javascript_page_uses_the_browser(self):
        url = "https://nvd.test/vuln/CVE-2024-3094"
        self.page(url, "<html><head><title>NVD</title></head><body><div id='root'></div><script>app()</script></body></html>")
        self.shown[url] = ("<html><head><title>NVD - CVE-2024-3094</title></head><body><main>"
                           + "<p>NVD Base Score: 10 CRITICAL. Affects xz liblzma 5.6.0 and 5.6.1.</p>" * 20 + "</main></body></html>")
        text = await self.read(url)
        self.assertIn("Base Score: 10 CRITICAL", text)
        self.assertEqual(self.rendered, [url])

    async def test_bot_wall_the_browser_cannot_pass_is_blocked(self):
        url = "https://npm.test/package/react"
        self.page(url, WALL, status=403)
        self.shown[url] = WALL
        self.assertEqual(await self.read(url), "")

    async def test_a_403_page_the_browser_can_read(self):
        url = "https://forum.test/questions/23064"
        self.page(url, WALL, status=403)
        self.shown[url] = ARTICLE
        self.assertIn("Ethereal", await self.read(url))

    async def test_no_browser_installed_gives_the_plain_result(self):
        url = "https://app.test/"
        self.page(url, "<html><body><div id='root'></div></body></html>")
        self.shown[url] = ImportError("No module named 'playwright'")
        self.assertEqual(await self.read(url), "")

    async def test_error_page_is_not_read(self):
        url = "https://site.test/missing"
        self.page(url, "<html><body>" + "<p>Page not found on this site.</p>" * 50 + "</body></html>", status=404)
        self.assertEqual(await self.read(url), "")

    async def test_unreadable_file_is_not_read_and_skips_the_browser(self):
        self.page("https://dl.test/app.zip", b"PK\x03\x04" + b"\x00\x01" * 500, kind="application/zip")
        self.assertEqual(await self.read("https://dl.test/app.zip"), "")
        self.assertEqual(self.rendered, [])

    # the own-reader sweep (2026-10-29) found these passed on as if they were the page
    async def test_walls_seen_in_the_sweep_are_blocked(self):
        walls = {"https://reddit.test/r/netsec/": "<html><head><title>Reddit - Prove your humanity</title></head><body>"
                 "<p>Prove your humanity. We're committed to safety and security.</p>" * 3 + "</body></html>",
                 "https://pypi.test/project/frida-tools/": "<html><head><title>Client Challenge</title></head><body>"
                 "<p>JavaScript is disabled in your browser. Please enable JavaScript to proceed.</p></body></html>",
                 "https://exploit.test/": "<html><head><title>Sucuri WebSite Firewall - Access Denied</title></head>"
                 "<body><h1>502 Hosting Server Connect Timeout</h1><p>Sucuri Website Firewall</p>" * 5 + "</body></html>"}
        for url, html in walls.items():
            self.page(url, html)
            self.shown[url] = html
            self.assertEqual(await self.read(url), "", url)

    async def test_scraps_after_the_browser_are_not_the_page(self):
        url = "https://ddg.test/html/?q=frida"
        self.page(url, "<html><body><div id='app'></div></body></html>")
        self.shown[url] = "<html><body><p>Something went wrong. Our support email address includes an error code.</p></body></html>"
        self.assertEqual(await self.read(url), "")

    async def test_plain_fetch_only_when_no_browser_is_given(self):
        self.page("https://site.test/wireshark", ARTICLE)
        self.assertIn("Ethereal", await self.read("https://site.test/wireshark", browser=False))

    # PyPI's pages sit behind a bot check; its JSON door is the sanctioned way in (the owner's choice, 2026-10-29)
    async def test_pypi_page_reads_through_pypis_own_door(self):
        self.page("https://pypi.org/pypi/frida-tools/json", json.dumps(PYPI_PROJECT), kind="application/json")
        text = await self.read("https://pypi.org/project/frida-tools/")
        self.assertTrue(text.startswith("Title: frida-tools 14.4.5 on PyPI"))
        self.assertIn("pip install frida-tools==14.4.5", text)
        self.assertIn("https://files.test/frida_tools-14.4.5.tar.gz", text)
        self.assertIn("GHSA-test-0001 (CVE-2099-0001), fixed in 14.4.6", text)
        self.assertIn("newest first (3): 14.4.5, 14.4.4 (yanked), 14.0.0", text)
        self.assertTrue(text.endswith("Frida CLI tools README."))
        self.assertEqual(self.rendered, [])  # never the browser: the web page itself is a bot check

    async def test_pypi_version_page_and_json_link_use_the_same_door(self):
        self.page("https://pypi.org/pypi/frida-tools/14.4.5/json", json.dumps(PYPI_VERSION), kind="application/json")
        page = await self.read("https://pypi.org/project/frida-tools/14.4.5/#files")
        self.assertIn("Files of 14.4.5 (1):", page)
        self.assertNotIn("All versions", page)  # a single version's answer has no version list
        self.assertEqual(page, await self.read("https://pypi.org/pypi/frida-tools/14.4.5/json"))
        self.page("https://pypi.org/pypi/frida-tools/json", json.dumps(PYPI_PROJECT), kind="application/json")
        self.assertIn("All versions", await self.read("https://pypi.org/pypi/frida-tools/json"))

    async def test_pypi_missing_package_or_odd_answer_is_not_read(self):
        self.assertEqual(await self.read("https://pypi.org/project/no-such-package/"), "")  # PyPI's 404
        self.page("https://pypi.org/pypi/walled/json", WALL)  # a bot check in place of the JSON
        self.assertEqual(await self.read("https://pypi.org/project/walled/"), "")
        self.page("https://pypi.org/pypi/empty/json", "{}", kind="application/json")
        self.assertEqual(await self.read("https://pypi.org/project/empty/"), "")
        self.page("https://pypi.org/search/?q=frida", ARTICLE)  # any other PyPI page: the normal way
        self.assertIn("Ethereal", await self.read("https://pypi.org/search/?q=frida"))


if __name__ == "__main__":
    unittest.main()
