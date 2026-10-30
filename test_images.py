import base64
import struct
import unittest

import images


def png(width=192, height=192):
    return b'\x89PNG\r\n\x1a\n' + b'\0' * 8 + struct.pack('>II', width, height)


def payload(data=None, mime='image/png', count=1):
    url = 'data:' + mime + ';base64,' + base64.b64encode(data or png()).decode()
    return {'messages': [{'role': 'user', 'content': [{'type': 'image_url', 'image_url': {'url': url}}] * count}]}


class ImageValidationTests(unittest.TestCase):
    def test_text_is_untouched(self):
        self.assertIsNone(images.validate({'messages': [{'role': 'user', 'content': 'text'}]}))

    def test_attached_png_and_limit(self):
        self.assertIsNone(images.validate(payload(count=images.MAX_IMAGES)))
        self.assertEqual(images.validate(payload(count=images.MAX_IMAGES + 1))[0], 'too_many_images')

    def test_remote_sources_are_not_fetched(self):
        value = payload(); value['messages'][0]['content'][0]['image_url']['url'] = 'http://169.254.169.254/image'
        self.assertEqual(images.validate(value)[0], 'invalid_image')

    def test_invalid_and_mismatched_data(self):
        self.assertEqual(images.validate(payload(b'not an image'))[0], 'invalid_image')
        self.assertEqual(images.validate(payload(mime='image/jpeg'))[0], 'invalid_image')

    def test_pixel_bounds(self):
        self.assertEqual(images.validate(payload(png(100000, 100000)))[0], 'image_too_large')
        self.assertEqual(images.validate(payload(png(1, 1000)))[0], 'image_too_large')

    def test_other_media_are_explicit(self):
        self.assertEqual(images.validate({'messages': [{'content': [{'type': 'video_url'}]}]})[0], 'unsupported_media')

    def test_request_cannot_raise_processor_limits(self):
        value = payload(); value['mm_processor_kwargs'] = {'max_pixels': 1000000000}
        self.assertEqual(images.validate(value)[0], 'invalid_image_options')

    def test_all_supplied_real_raster_formats(self):
        from pathlib import Path
        root = Path(__file__).resolve().parents[2] / '.verification/chat-files/fixtures'
        for name, mime in [('scan.png', 'image/png'), ('scan.jpg', 'image/jpeg'), ('scan.webp', 'image/webp')]:
            if (root / name).exists():
                self.assertIsNone(images.validate(payload((root / name).read_bytes(), mime)), name)

    def test_byte_limits(self):
        self.assertEqual(images.validate(payload(png() + b'x' * images.MAX_IMAGE_BYTES))[0], 'image_too_large')
        self.assertEqual(images.validate(payload(png() + b'x' * (6 * 1024 * 1024), count=3))[0], 'image_too_large')

    def test_encoded_bytes_do_not_inflate_fallback_billing(self):
        small = payload(png())
        padded = payload(png() + b'x' * 200000)
        self.assertEqual(images.text_only_prompt_estimate(small), images.text_only_prompt_estimate(padded))
        self.assertLess(images.text_only_prompt_estimate(padded), 1000)


class ImageRouteTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        import httpx
        import app as gateway
        import test_gateway
        test_gateway.fresh_gateway()
        self.gateway = gateway
        self.requests = []
        async def upstream(request):
            import json
            self.requests.append(json.loads(request.content))
            if self.requests[-1].get('stream'):
                return httpx.Response(200, text='data: {"choices":[{"delta":{"content":"ready"}}],"usage":{"prompt_tokens":128,"completion_tokens":1}}\n\ndata: [DONE]\n\n')
            return httpx.Response(200, json={'choices':[{'message':{'content':'ready'}}], 'usage':{'prompt_tokens':128,'completion_tokens':1}})
        gateway.app.state.client = httpx.AsyncClient(transport=httpx.MockTransport(upstream))
        self.api = httpx.AsyncClient(transport=httpx.ASGITransport(app=gateway.app), base_url='http://gateway')
        raw, _ = test_gateway.add_key('max')
        self.auth = {'Authorization':'Bearer ' + raw}

    async def asyncTearDown(self):
        import stats
        await self.api.aclose()
        await self.gateway.app.state.client.aclose()
        self.gateway.app.state.client = None
        self.gateway.app.state.conn.close()
        self.gateway.app.state.conn = None
        stats._db.close(); stats._db = None

    async def test_invalid_image_never_reaches_inference(self):
        value = payload(b'bad image'); value['model'] = 'sherlock'
        response = await self.api.post('/v1/chat/completions',json=value,headers=self.auth)
        self.assertEqual(response.status_code,400)
        self.assertEqual(self.requests,[])

    async def test_native_content_is_preserved_and_usage_is_continuous(self):
        value = payload(); value.update(model='sherlock',stream=True)
        response = await self.api.post('/v1/chat/completions',json=value,headers=self.auth)
        self.assertEqual(response.status_code,200)
        self.assertEqual(self.requests[0]['messages'][-1]['content'],value['messages'][0]['content'])
        self.assertEqual(self.requests[0]['stream_options'],{'include_usage':True,'continuous_usage_stats':True})
        import stats
        self.assertEqual(stats._db.execute("SELECT waited FROM hourly WHERE plan='max'").fetchone()[0],1)

    async def test_chunked_body_limit_cannot_be_bypassed(self):
        async def chunks():
            for _ in range(33): yield b'x' * (1024 * 1024)
        response = await self.api.post('/v1/chat/completions',content=chunks(),headers=self.auth)
        self.assertEqual(response.status_code,413)
        self.assertEqual(self.requests,[])


if __name__ == '__main__':
    unittest.main()
