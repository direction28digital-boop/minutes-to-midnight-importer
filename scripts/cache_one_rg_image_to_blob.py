# scripts/cache_one_rg_image_to_blob.py
# Usage:
#   python scripts/cache_one_rg_image_to_blob.py "<RG_IMAGE_URL>" "<RG_ANIMAL_ID>" "<RG_MEDIA_ID(optional)>"

import asyncio
import hashlib
import os
import sys
from io import BytesIO

import psycopg
import requests
from dotenv import load_dotenv
from PIL import Image
from vercel.blob import AsyncBlobClient  # uses BLOB_READ_WRITE_TOKEN from env by default :contentReference[oaicite:1]{index=1}


EXT_BY_CONTENT_TYPE = {
    "image/jpeg": "jpg",
    "image/jpg": "jpg",
    "image/png": "png",
    "image/webp": "webp",
    "image/gif": "gif",
}


def _normalize_content_type(ct: str | None) -> str | None:
    if not ct:
        return None
    return ct.split(";")[0].strip().lower() or None


def _infer_ext(content_type: str | None, data: bytes) -> str:
    ct = _normalize_content_type(content_type)
    if ct in EXT_BY_CONTENT_TYPE:
        return EXT_BY_CONTENT_TYPE[ct]

    # fallback: inspect the bytes
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


async def main() -> None:
    load_dotenv(".env.local")

    if len(sys.argv) < 3:
        raise SystemExit(
            "Usage:\n"
            '  python scripts/cache_one_rg_image_to_blob.py "<RG_IMAGE_URL>" "<RG_ANIMAL_ID>" "<RG_MEDIA_ID(optional)>"\n'
        )

    original_url = sys.argv[1].strip()
    rg_animal_id = sys.argv[2].strip()
    rg_media_id = sys.argv[3].strip() if len(sys.argv) >= 4 else None

    dsn = os.getenv("DATABASE_URL")
    if not dsn:
        raise SystemExit("DATABASE_URL not set. Ensure it exists in .env.local (export DATABASE_URL=...)")

    if not os.getenv("BLOB_READ_WRITE_TOKEN"):
        raise SystemExit("BLOB_READ_WRITE_TOKEN not set. Ensure it exists in .env.local (export BLOB_READ_WRITE_TOKEN=...)")

    # 1) If already cached, reuse
    with psycopg.connect(dsn) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                select id, public_url, sha256
                from m2mr.media_assets
                where source='RG' and original_url=%s
                """,
                (original_url,),
            )
            row = cur.fetchone()

    if row and row[1]:
        print("Already cached:")
        print("id:", row[0])
        print("public_url:", row[1])
        print("sha256:", row[2])
        return

    # 2) Download image
    r = requests.get(original_url, timeout=30, headers={"User-Agent": "SnoutHubImporter/1.0"})
    r.raise_for_status()
    data = r.content

    sha = hashlib.sha256(data).hexdigest()
    content_type = _normalize_content_type(r.headers.get("content-type"))
    ext = _infer_ext(content_type, data)
    width, height = _get_dimensions(data)

    # Put into a neutral path in Blob (no RG in the URL/path)
    pathname = f"pets/{sha}.{ext}"

    # 3) Upload to Vercel Blob (token pulled from env by SDK) :contentReference[oaicite:2]{index=2}
    client = AsyncBlobClient()
    uploaded = await client.put(
        pathname,
        data,
        access="public",
        content_type=content_type or "application/octet-stream",
        overwrite=True,
        add_random_suffix=False,
    )

    public_url = uploaded.url

    # 4) Upsert mapping into Neon
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
                returning id, public_url, sha256, bytes
                """,
                (
                    rg_animal_id,
                    rg_media_id,
                    original_url,
                    public_url,
                    sha,
                    content_type,
                    len(data),
                    width,
                    height,
                    ext,
                ),
            )
            saved = cur.fetchone()
        conn.commit()

    print("Saved:")
    print("id:", saved[0])
    print("public_url:", saved[1])
    print("sha256:", saved[2])
    print("bytes:", saved[3])


if __name__ == "__main__":
    asyncio.run(main())
