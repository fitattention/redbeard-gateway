"""Reader route checks with in-memory keys/stats and a synthetic HTTP reader."""
import hashlib
import unittest

import httpx

import app as gateway
import stats
import store


class ReaderTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.text = ("A readable line with an emoji 🙂 and an accent é.\n" * 400)
        self.reader_status = 200
        self.html = ""
        self.direct_status, self.direct_type = 200, None  # the page itself, when fetched directly
        self.requests = []
        gateway.hits.clear()
        gateway.app.state.conn = store.connect(":memory:")
        stats._db = stats.connect(":memory:")
        self.key = "synthetic-reader-test-key"
        store.put(gateway.app.state.conn, store.Key(key_hash=store.hash_key(self.key), plan="max"))

        async def reader(request):
            self.requests.append(str(request.url))
            if str(request.url).startswith(gateway.READER):
                return httpx.Response(self.reader_status, text=self.text)
            return httpx.Response(self.direct_status, text=self.html,
                                  headers={"content-type": self.direct_type} if self.direct_type else None)

        gateway.app.state.client = httpx.AsyncClient(transport=httpx.MockTransport(reader))
        self.api = httpx.AsyncClient(transport=httpx.ASGITransport(app=gateway.app), base_url="http://local")

    async def asyncTearDown(self):
        await self.api.aclose()
        await gateway.app.state.client.aclose()
        gateway.app.state.client = None
        gateway.app.state.conn.close()
        gateway.app.state.conn = None
        stats._db.close()
        stats._db = None
        gateway.hits.clear()

    async def read(self, **params):
        return await self.api.get("/v1/fetch", params={"url": "https://fixture.test/page", **params},
                                  headers={"Authorization": "Bearer " + self.key})

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
        self.assertEqual(self.requests, [])

    async def test_invalid_url_and_page_id_rejected_before_fetch(self):
        for params in [{"url": "file:///etc/passwd"}, {"url": ""}, {"page_id": "wrong"}]:
            self.assertEqual((await self.read(**params)).status_code, 400)
        self.assertEqual(self.requests, [])

    async def test_offset_past_end_is_explicit_error(self):
        response = await self.read(offset=len(self.text) + 1)
        self.assertEqual(response.status_code, 400)
        self.assertEqual(response.json()["error"]["code"], "offset_out_of_range")

    async def test_changed_page_requires_restart(self):
        first = (await self.read()).json()
        self.text = "Changed introduction.\n" + self.text
        response = await self.read(offset=first["next_offset"], page_id=first["page_id"])
        self.assertEqual(response.status_code, 409)
        self.assertEqual(response.json()["error"]["code"], "page_changed")
        self.assertEqual(response.json()["error"]["next_read"]["offset"], 0)

    async def test_auth_required_without_reader_request(self):
        response = await self.api.get("/v1/fetch", params={"url": "https://fixture.test/page"})
        self.assertEqual(response.status_code, 401)
        self.assertEqual(self.requests, [])

    async def test_old_url_only_call_keeps_content_field(self):
        page = (await self.read()).json()
        self.assertEqual(page["content"], self.text[:6000])
        self.assertTrue(page["has_more"])
        self.assertEqual(await gateway.fetch_page("https://fixture.test/page"), self.text[:6000])

    async def test_unavailable_page_reports_failure(self):
        self.reader_status, self.html = 503, ""
        response = await self.read()
        self.assertEqual(response.status_code, 502)
        self.assertEqual(response.json()["error"]["code"], "page_unavailable")

    async def test_direct_html_fallback_is_paginated(self):
        self.reader_status = 503
        self.html = "<html><script>discard this script</script><body>" + "Actual page words. " * 700 + "</body></html>"
        first = (await self.read()).json()
        self.assertNotIn("discard this script", first["content"])
        self.assertTrue(first["has_more"])
        self.assertEqual(len(first["content"]), 6000)
        second = (await self.read(**first["next_read"])).json()
        self.assertNotEqual(first["content"], second["content"])
        self.assertEqual(second["offset"], 6000)

    # 2026-10-29 sweep: bot walls and files were handed on as if they were the page
    WALL = ("Title: Just a moment...\n\nURL Source: https://fixture.test/page\n\nWarning: Target URL returned error 403: "
            "Forbidden\n\nMarkdown Content:\nVerify you are human by completing the action below.\n" + "x" * 250)

    async def test_bot_wall_is_not_handed_on(self):
        self.text, self.direct_status, self.html = self.WALL, 403, "<html>Forbidden</html>" * 50
        response = await self.read()
        self.assertEqual(response.status_code, 502)
        self.assertEqual(response.json()["error"]["code"], "page_unavailable")

    async def test_bot_wall_then_the_page_itself(self):
        self.text, self.direct_type = self.WALL, "text/html; charset=utf-8"
        self.html = "<html><body>" + "The real article text. " * 100 + "</body></html>"
        page = (await self.read()).json()
        self.assertIn("The real article text.", page["content"])
        self.assertNotIn("Just a moment", page["content"])

    async def test_js_wall_is_not_handed_on(self):
        self.text, self.direct_status = "reuters.com\n\nPlease enable JS and disable any ad blocker" + " " * 200, 401
        self.assertEqual((await self.read()).status_code, 502)

    async def test_pdf_is_not_read_as_text(self):
        self.reader_status, self.direct_type = 403, "application/pdf"
        self.html = "%PDF-1.5\n%\xe2\xe3\xcf\xd3\n137 0 obj stream " + "\x00\x01binary" * 500
        self.assertEqual((await self.read()).status_code, 502)

    async def test_direct_error_page_is_not_handed_on(self):
        self.reader_status, self.direct_status = 503, 404
        self.html = "<html><body>" + "Page not found on this site. " * 50 + "</body></html>"
        self.assertEqual((await self.read()).status_code, 502)

    async def test_real_pages_mentioning_walls_still_read(self):
        long_js = "Some articles say: please enable JS for the video. " + "Real paragraph text. " * 200
        late_warning = "x" * 900 + " Target URL returned error 403 is a phrase this article explains."
        for text in (long_js, late_warning):
            self.text = text
            page = (await self.read()).json()
            self.assertEqual(page["total_chars"], len(text))


if __name__ == "__main__":
    unittest.main()
