"""Validate attached image data in memory. No downloads, pixel decoding, files, or logs."""
import base64
import binascii
import re
import struct
import json

MAX_IMAGES = 5
MAX_IMAGE_BYTES = 10 * 1024 * 1024
MAX_TOTAL_BYTES = 16 * 1024 * 1024
MAX_PIXELS = 12_000_000


def dimensions(data):
    if data.startswith(b'\x89PNG\r\n\x1a\n') and len(data) >= 24:
        return 'image/png', *struct.unpack('>II', data[16:24])
    if data.startswith(b'\xff\xd8'):
        offset = 2
        while offset + 8 < len(data):
            if data[offset] != 255:
                offset += 1
                continue
            marker = data[offset + 1]
            if marker in [0xc0, 0xc1, 0xc2, 0xc3, 0xc5, 0xc6, 0xc7, 0xc9, 0xca, 0xcb, 0xcd, 0xce, 0xcf]:
                height, width = struct.unpack('>HH', data[offset + 5:offset + 9])
                return 'image/jpeg', width, height
            if marker in [0xd8, 0xd9, 0x01, *range(0xd0, 0xd8)]:
                offset += 2
                continue
            size = struct.unpack('>H', data[offset + 2:offset + 4])[0]
            if size < 2:
                break
            offset += size + 2
    if len(data) >= 30 and data[:4] == b'RIFF' and data[8:12] == b'WEBP':
        kind = data[12:16]
        if kind == b'VP8X':
            return 'image/webp', 1 + int.from_bytes(data[24:27], 'little'), 1 + int.from_bytes(data[27:30], 'little')
        if kind == b'VP8L' and data[20] == 0x2f:
            bits = struct.unpack('<I', data[21:25])[0]
            return 'image/webp', 1 + (bits & 0x3fff), 1 + ((bits >> 14) & 0x3fff)
        if kind == b'VP8 ' and data[23:26] == b'\x9d\x01\x2a':
            width, height = struct.unpack('<HH', data[26:30])
            return 'image/webp', width & 0x3fff, height & 0x3fff
    raise ValueError('Unsupported or damaged image header.')


def text_only_prompt_estimate(payload):
    """Conservative fallback when usage is lost: never bill base64 bytes as text tokens."""
    safe = dict(payload)
    safe['messages'] = [
        {**message, 'content': [part for part in message['content']
          if not isinstance(part, dict) or part.get('type') != 'image_url']}
        if isinstance(message, dict) and isinstance(message.get('content'), list) else message
        for message in payload.get('messages', [])
    ]
    return len(json.dumps(safe, ensure_ascii=False).encode('utf-8', errors='replace')) // 4


def validate(payload, details=None):
    """Return a public (code, message) error, or None. Never echo input or credentials."""
    count = total = 0
    messages = payload.get('messages', [])
    if not isinstance(messages, list):
        return None  # The inference protocol handles non-image schema errors.
    for message in messages:
        if not isinstance(message, dict) or not isinstance(message.get('content'), list):
            continue
        for part in message['content']:
            if not isinstance(part, dict):
                continue
            if part.get('type') in ['video_url', 'input_audio', 'audio_url', 'image_embeds', 'image', 'input_image']:
                return 'unsupported_media', 'Use attached PNG, JPEG or WebP images; this media input is not supported.'
            if part.get('type') != 'image_url':
                continue
            count += 1
            if count > MAX_IMAGES:
                return 'too_many_images', 'Use at most 5 images in one conversation request.'
            image = part.get('image_url')
            url = image.get('url') if isinstance(image, dict) else None
            if not isinstance(url, str):
                return 'invalid_image', 'An attached image must contain a base64 data URL.'
            match = re.fullmatch(r'data:(image/(?:png|jpeg|webp));base64,([A-Za-z0-9+/]*={0,2})', url)
            if not match:
                return 'invalid_image', 'Attach a PNG, JPEG or WebP image as a base64 data URL. Remote image URLs are not fetched.'
            if len(match[2]) > ((MAX_IMAGE_BYTES + 2) // 3) * 4:
                return 'image_too_large', 'Each image must be 10 MB or smaller.'
            try:
                data = base64.b64decode(match[2], validate=True)
                mime, width, height = dimensions(data)
            except (ValueError, binascii.Error, struct.error):
                return 'invalid_image', 'The attached image is damaged or has an invalid encoding.'
            total += len(data)
            if len(data) > MAX_IMAGE_BYTES or total > MAX_TOTAL_BYTES:
                return 'image_too_large', 'Images must be 10 MB or smaller each and 16 MB or smaller together.'
            if mime != match[1] or not width or not height:
                return 'invalid_image', 'The image type or dimensions do not match its file data.'
            if width * height > MAX_PIXELS or max(width, height) / min(width, height) > 200:
                return 'image_too_large', 'Use images of up to 12 megapixels and split extremely tall or narrow images.'
    if count and (payload.get('mm_processor_kwargs') or payload.get('media_io_kwargs')):
        return 'invalid_image_options', 'Attached images use the service’s configured image-processing limits.'
    if details is not None:
        details['image_count'] = count
    return None
