# m2mr/media/cache_rg_image.py
import asyncio
import hashlib
import random
from io import BytesIO
from urllib.parse import urlparse, urlunparse

import psycopg
import requests
from PIL import Image
from vercel.blob import AsyncBlobClient


EXT_BY_CONTENT_TYPE = {
    "image/jpeg": "jpg",
    "image/jpg": "jpg",
    "image/png": "png",
    "image/webp": "webp",
    "image/gif": "gif",
}

RETRYABLE_HTTP_STATUS = {429, 500, 502, 503, 504}


def _canonicalize_url(url: str) -> str:
    p = urlparse(url)
    return urlunparse((p.scheme, p.netloc, p.path, "", "", ""))


def _normalize_content_type(ct: str | None) -> str | None:
    if not ct:
        return None
    return ct.split(";")[0].strip().lower() or None


def _infer_ext(content_type: str | None, data: bytes) -> str:
    ct = _normalize_content_type(content_type)
    if ct in EXT_BY_CONTENT_TYPE:
        return EXT_BY_CONTENT_TYPE[ct]

    try:
        with Image.open(BytesIO(data)) as img:
            fmt = (img.format or "").lower()
            if fmt == "jpeg":
                return "jpg"
            if fmt in ("png", "webp", "gif"):
                return fmt
    except Exception:
        pass

    return "bin"


def _get_dimensions(data: bytes) -> tuple[int | None, int | None]:
    try:
        with Image.open(BytesIO(data)) as img:
            w, h = img.size
            return int(w), int(h)
    except Exception:
        return None, None


async def _sleep_backoff(attempt: int) -> None:
    # Exponential backoff with jitter, capped
    base = min(2**attempt, 15)
    jitter = random.random() * 0.35
    await asyncio.sleep(base + jitter)


def _db_get_public_url(dsn: str, original_url: str) -> str | None:
    with psycopg.connect(dsn) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                select public_url
                from m2mr.media_assets
                where source='RG' and original_url=%s
                """,
                (original_url,),
            )
            row = cur.fetchone()
    return row[0] if row and row[0] else None


def _db_upsert_mapping(
    dsn: str,
    *,
    rg_animal_id: str,
    rg_media_id: str | None,
    original_url: str,
    public_url: str,
    sha: str,
    content_type: str | None,
    size_bytes: int,
    width: int | None,
    height: int | None,
    ext: str,
) -> None:
    with psycopg.connect(dsn) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                insert into m2mr.media_assets (
                    source, source_entity, source_entity_id, source_media_id,
                    original_url, public_url,
                    sha256, content_type, bytes, width, height,
                    file_ext, data
                )
                values (
                    'RG', 'animal', %s, %s,
                    %s, %s,
                    %s, %s, %s, %s, %s,
                    %s, null
                )
                on conflict (source, original_url)
                do update set
                    public_url = excluded.public_url,
                    sha256 = excluded.sha256,
                    content_type = excluded.content_type,
                    bytes = excluded.bytes,
                    width = excluded.width,
                    height = excluded.height,
                    file_ext = excluded.file_ext,
                    data = null,
                    updated_at = now(),
                    fetched_at = now()
                """,
                (
                    rg_animal_id,
                    rg_media_id,
                    original_url,
                    public_url,
                    sha,
                    content_type,
                    size_bytes,
                    width,
                    height,
                    ext,
                ),
            )
        conn.commit()


def _download_once(url: str) -> tuple[bytes, str | None]:
    # Tuple timeout = (connect, read)
    r = requests.get(url, timeout=(10, 90), headers={"User-Agent": "SnoutHubImporter/1.0"})
    if r.status_code == 404:
        raise RuntimeError(f"404_not_found:{url}")
    if r.status_code in RETRYABLE_HTTP_STATUS:
        raise RuntimeError(f"retryable_http:{r.status_code}")
    r.raise_for_status()
    return r.content, _normalize_content_type(r.headers.get("content-type"))


async def _download_with_retries(url: str, attempts: int = 5) -> tuple[bytes, str | None]:
    last = None
    for attempt in range(attempts):
        try:
            return await asyncio.to_thread(_download_once, url)
        except Exception as e:
            last = e
            # 404 is permanent
            if str(e).startswith("404_not_found:"):
                raise
            if attempt == attempts - 1:
                raise
            await _sleep_backoff(attempt)
    raise last or RuntimeError("download_failed")


async def _blob_put_with_retries(
    client: AsyncBlobClient,
    *,
    pathname: str,
    data: bytes,
    content_type: str | None,
    attempts: int = 4,
) -> str:
    last = None
    for attempt in range(attempts):
        try:
            uploaded = await client.put(
                pathname,
                data,
                access="public",
                content_type=content_type or "application/octet-stream",
                # Vercel Blob blocks overwrites by default; enable overwrite explicitly. :contentReference[oaicite:2]{index=2}
                overwrite=True,
                add_random_suffix=False,
            )
            return uploaded.url
        except Exception as e:
            last = e

            # If the SDK/API still reports "already exists", fetch the existing URL via head()
            msg = str(e).lower()
            if "already exists" in msg:
                try:
                    meta = await client.head(pathname)
                    url = getattr(meta, "url", None) or (meta.get("url") if isinstance(meta, dict) else None)
                    if url:
                        return url
                except Exception:
                    pass

            if attempt == attempts - 1:
                raise
            await _sleep_backoff(attempt)
    raise last or RuntimeError("blob_put_failed")


async def cache_rg_image_to_blob(
    *,
    dsn: str,
    original_url: str,
    rg_animal_id: str,
    rg_media_id: str | None = None,
) -> str:
    original_url = _canonicalize_url(original_url)

    # 1) DB cache (non-fatal if DB is flaky)
    for attempt in range(3):
        try:
            mapped = await asyncio.to_thread(_db_get_public_url, dsn, original_url)
            if mapped:
                return mapped
            break
        except Exception:
            if attempt == 2:
                break
            await _sleep_backoff(attempt)

    # 2) Download with retries
    data, content_type = await _download_with_retries(original_url)

    sha = hashlib.sha256(data).hexdigest()
    ext = _infer_ext(content_type, data)
    width, height = _get_dimensions(data)

    # 3) Upload to Blob with retries
    pathname = f"pets/{sha}.{ext}"
    client = AsyncBlobClient()
    public_url = await _blob_put_with_retries(
        client,
        pathname=pathname,
        data=data,
        content_type=content_type,
    )

    # 4) Upsert mapping (retry, but never fail the URL rewrite if Neon hiccups)
    for attempt in range(3):
        try:
            await asyncio.to_thread(
                _db_upsert_mapping,
                dsn,
                rg_animal_id=rg_animal_id,
                rg_media_id=rg_media_id,
                original_url=original_url,
                public_url=public_url,
                sha=sha,
                content_type=content_type,
                size_bytes=len(data),
                width=width,
                height=height,
                ext=ext,
            )
            break
        except Exception:
            if attempt == 2:
                break
            await _sleep_backoff(attempt)

    return public_url
