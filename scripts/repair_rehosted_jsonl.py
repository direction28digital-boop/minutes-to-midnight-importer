# scripts/repair_rehosted_jsonl.py
# Repairs an already-written rehosted JSONL by:
# - Rehosting any remaining rescuegroups.org photo URLs to Vercel Blob
# - Retrying photos that previously got "rehostError"
# - Removing rehostError when a rehost succeeds
#
# Run:
#   source .env.local
#   python scripts/repair_rehosted_jsonl.py \
#     --in data/animals.normalized.rehosted.jsonl \
#     --out data/animals.normalized.rehosted.repaired.jsonl \
#     --concurrency 6 \
#     --progress-every 200

import os
import sys

# Ensure repo root is importable so `import m2mr...` works
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

import argparse
import asyncio
import json
from urllib.parse import urlparse

from m2mr.media.cache_rg_image import cache_rg_image_to_blob


def is_blob_url(url: str) -> bool:
    return bool(url) and ("blob.vercel-storage.com" in url)


def is_rescuegroups_url(url: str) -> bool:
    try:
        host = (urlparse(url).netloc or "").lower()
    except Exception:
        return False
    return host.endswith("rescuegroups.org")


async def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--in", dest="in_path", required=True)
    ap.add_argument("--out", dest="out_path", required=True)
    ap.add_argument("--limit", type=int, default=0, help="0 = no limit")
    ap.add_argument("--concurrency", type=int, default=6)
    ap.add_argument("--progress-every", type=int, default=200)
    args = ap.parse_args()

    dsn = os.getenv("DATABASE_URL")
    if not dsn:
        raise SystemExit("DATABASE_URL not set. Run: source .env.local")

    if not os.getenv("BLOB_READ_WRITE_TOKEN"):
        raise SystemExit("BLOB_READ_WRITE_TOKEN not set. Run: source .env.local")

    sem = asyncio.Semaphore(max(1, args.concurrency))

    processed = 0
    repaired = 0
    marked_missing = 0
    still_failed = 0

    with open(args.in_path, "r", encoding="utf-8") as fin, open(args.out_path, "w", encoding="utf-8") as fout:
        for line in fin:
            if args.limit and processed >= args.limit:
                break

            obj = json.loads(line)
            photos = obj.get("photos") or []
            rg_animal_id = str(obj.get("sourceAnimalId") or obj.get("m2mId") or "unknown")

            async def handle_photo(photo: dict) -> None:
                nonlocal repaired, marked_missing, still_failed

                url = (photo.get("url") or "").strip()
                err = photo.get("rehostError")

                # If already a Blob URL and no error, nothing to do
                if url and is_blob_url(url) and not err:
                    return

                # Only attempt repair when:
                # - It's an RG URL, or
                # - It previously failed (has rehostError)
                if not (is_rescuegroups_url(url) or err):
                    return

                try:
                    async with sem:
                        blob_url = await cache_rg_image_to_blob(
                            dsn=dsn,
                            original_url=url,
                            rg_animal_id=rg_animal_id,
                            rg_media_id=None,
                        )
                    photo["url"] = blob_url
                    photo.pop("rehostError", None)
                    repaired += 1
                except Exception as e:
                    msg = str(e)

                    # If RG no longer has the image, don’t keep an RG URL around
                    if msg.startswith("404_not_found:"):
                        photo["url"] = ""
                        photo["missing"] = True
                        photo.pop("rehostError", None)
                        marked_missing += 1
                        repaired += 1
                        return

                    still_failed += 1
                    photo["rehostError"] = msg[:200]

            await asyncio.gather(*(handle_photo(p) for p in photos if isinstance(p, dict)))

            obj["photos"] = photos
            fout.write(json.dumps(obj, ensure_ascii=False) + "\n")

            processed += 1
            if args.progress_every and (processed % args.progress_every == 0):
                print(
                    f"progress records={processed} repaired={repaired} missing={marked_missing} still_failed={still_failed}",
                    flush=True,
                )

    print("done")
    print("records_processed:", processed)
    print("photos_repaired:", repaired)
    print("photos_marked_missing:", marked_missing)
    print("photos_still_failed:", still_failed)


if __name__ == "__main__":
    asyncio.run(main())
