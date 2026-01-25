# scripts/rehost_animals_photos_jsonl.py
# Rewrites RescueGroups photo URLs in animals.normalized.jsonl to Vercel Blob URLs.
#
# Run (small test):
#   source .env.local
#   python scripts/rehost_animals_photos_jsonl.py --in data/animals.normalized.jsonl --out data/animals.normalized.rehosted.jsonl --limit 50 --concurrency 3
#
# Resume (if interrupted):
#   source .env.local
#   python scripts/rehost_animals_photos_jsonl.py --in data/animals.normalized.jsonl --out data/animals.normalized.rehosted.jsonl --concurrency 5 --resume

import os
import sys

# Ensure repo root is importable so `import m2mr...` works
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

import argparse
import asyncio
import json
from urllib.parse import urlparse, urlunparse

from m2mr.media.cache_rg_image import cache_rg_image_to_blob


def canonicalize_url(url: str) -> str:
    """Strip query/fragment so RG resize params (e.g. ?width=100) don't create duplicates."""
    p = urlparse(url)
    return urlunparse((p.scheme, p.netloc, p.path, "", "", ""))


def is_already_rehosted(url: str) -> bool:
    return bool(url) and ("blob.vercel-storage.com" in url)


def is_rescuegroups_url(url: str) -> bool:
    try:
        host = (urlparse(url).netloc or "").lower()
    except Exception:
        return False
    return host.endswith("rescuegroups.org")


def count_lines(path: str) -> int:
    n = 0
    with open(path, "r", encoding="utf-8") as f:
        for _ in f:
            n += 1
    return n


async def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--in", dest="in_path", required=True)
    ap.add_argument("--out", dest="out_path", required=True)
    ap.add_argument("--limit", type=int, default=0, help="0 = no limit")
    ap.add_argument("--concurrency", type=int, default=3)
    ap.add_argument("--resume", action="store_true")
    ap.add_argument("--progress-every", type=int, default=200)
    args = ap.parse_args()

    dsn = os.getenv("DATABASE_URL")
    if not dsn:
        raise SystemExit("DATABASE_URL not set. Run: source .env.local")

    if not os.getenv("BLOB_READ_WRITE_TOKEN"):
        raise SystemExit("BLOB_READ_WRITE_TOKEN not set. Run: source .env.local")

    sem = asyncio.Semaphore(max(1, args.concurrency))

    processed = 0
    changed = 0
    failed = 0

    # Resume support: skip N lines already written
    skip = 0
    mode = "w"
    if args.resume and os.path.exists(args.out_path):
        skip = count_lines(args.out_path)
        mode = "a"

    with open(args.in_path, "r", encoding="utf-8") as fin:
        # Skip already-processed records
        for _ in range(skip):
            next(fin, None)

        with open(args.out_path, mode, encoding="utf-8") as fout:
            for line in fin:
                if args.limit and processed >= args.limit:
                    break

                obj = json.loads(line)
                photos = obj.get("photos") or []
                rg_animal_id = str(obj.get("sourceAnimalId") or obj.get("m2mId") or "unknown")

                async def handle_photo(photo: dict) -> None:
                    nonlocal changed, failed
                    url = (photo.get("url") or "").strip()
                    if not url:
                        return
                    if is_already_rehosted(url):
                        return
                    if not is_rescuegroups_url(url):
                        return

                    original = canonicalize_url(url)

                    try:
                        async with sem:
                            blob_url = await cache_rg_image_to_blob(
                                dsn=dsn,
                                original_url=original,
                                rg_animal_id=rg_animal_id,
                                rg_media_id=None,
                            )
                        photo["url"] = blob_url
                        changed += 1
                    except Exception as e:
                        failed += 1
                        photo["rehostError"] = str(e)[:200]

                await asyncio.gather(*(handle_photo(p) for p in photos if isinstance(p, dict)))

                obj["photos"] = photos
                fout.write(json.dumps(obj, ensure_ascii=False) + "\n")

                processed += 1

                if args.progress_every and (processed % args.progress_every == 0):
                    print(
                        f"progress records={processed} photos_rehosted={changed} photos_failed={failed}",
                        flush=True,
                    )

    print("done")
    print("records_processed:", processed)
    print("photos_rehosted:", changed)
    print("photos_failed:", failed)


if __name__ == "__main__":
    asyncio.run(main())
