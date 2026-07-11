# scripts/rehost_media_assets.py
#
# Rehost RescueGroups photo URLs into Vercel Blob + m2mr.media_assets
# (source='RG', canonicalized original_url -> public_url).
#
# Unlike rehost_animals_photos_jsonl.py (which rewrites a jsonl file), this
# script's unit of work is the media_assets table itself, so it serves both:
#   - the one-time backfill:  --from-api   (pull the animal list live from M2M)
#   - the per-import top-up:  --in data/animals.normalized.jsonl
#     (run in import.yml right after the importer writes a fresh snapshot)
#
# Idempotent + resume-safe by construction: existing original_urls (including
# dead ones recorded with public_url='') are prefetched in one query and
# skipped, so re-running only attempts what's still missing.
#
# Dead RG URLs (hard 404s) are recorded in media_assets with public_url=''
# so they're never retried; the site treats an empty mapping as "no photo"
# and falls through to the breed placeholder. They're also appended to
# --dead-report (jsonl) for visibility.
#
# Run (backfill):
#   source .env.local
#   python scripts/rehost_media_assets.py --from-api --concurrency 4
#
# Run (pipeline):
#   python scripts/rehost_media_assets.py --in data/animals.normalized.jsonl

import os
import sys

# Ensure repo root is importable so `import m2mr...` works
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

import argparse
import asyncio
import json
import time
from urllib.parse import urlparse, urlunparse

import psycopg
import requests

from m2mr.media.cache_rg_image import cache_rg_image_to_blob


def canonicalize_url(url: str) -> str:
    """Strip query/fragment so RG resize params (?width=100) don't duplicate."""
    p = urlparse(url)
    return urlunparse((p.scheme, p.netloc, p.path, "", "", ""))


def is_rescuegroups_url(url: str) -> bool:
    try:
        host = (urlparse(url).netloc or "").lower()
    except Exception:
        return False
    return host.endswith("rescuegroups.org")


def collect_from_animals(animals: list[dict]) -> dict[str, str]:
    """canonical_url -> rg_animal_id (first animal seen wins; the mapping is
    keyed by URL, so which animal 'owns' a shared URL doesn't matter)."""
    out: dict[str, str] = {}
    for a in animals:
        rg_animal_id = str(a.get("sourceAnimalId") or a.get("m2mId") or "unknown")
        for p in a.get("photos") or []:
            if not isinstance(p, dict):
                continue
            url = (p.get("url") or "").strip()
            if not url or not is_rescuegroups_url(url):
                continue
            out.setdefault(canonicalize_url(url), rg_animal_id)
    return out


def load_animals_from_jsonl(path: str) -> list[dict]:
    animals = []
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if line:
                animals.append(json.loads(line))
    return animals


def load_animals_from_api(api_base: str) -> list[dict]:
    url = f"{api_base.rstrip('/')}/wp-json/m2mr/v1/animals?page=1&per_page=100000"
    r = requests.get(
        url,
        timeout=(15, 600),
        headers={"Accept": "application/json", "User-Agent": "SnoutHubImporter/1.0"},
    )
    r.raise_for_status()
    return (r.json() or {}).get("items") or []


def fetch_existing_urls(dsn: str) -> set[str]:
    with psycopg.connect(dsn) as conn, conn.cursor() as cur:
        cur.execute("select original_url from m2mr.media_assets where source='RG'")
        return {row[0] for row in cur.fetchall()}


def bump_last_seen(dsn: str, urls: list[str], chunk: int = 5000) -> int:
    """Mark every URL present in the current feed as seen-now. The stale-blob
    cleanup (scripts/cleanup_stale_blobs.py) deletes rows/blobs whose
    last_seen_at is older than its cutoff — this bump is what protects
    still-listed dogs, so it MUST run against the full current feed."""
    bumped = 0
    with psycopg.connect(dsn) as conn, conn.cursor() as cur:
        for i in range(0, len(urls), chunk):
            cur.execute(
                "update m2mr.media_assets set last_seen_at = now() "
                "where source='RG' and original_url = any(%s)",
                (urls[i : i + chunk],),
            )
            bumped += cur.rowcount
        conn.commit()
    return bumped


def record_dead_url(dsn: str, original_url: str, rg_animal_id: str) -> None:
    """Tombstone a hard-404 URL so it's never retried. public_url='' reads as
    "no photo" on the site (falls through to the breed placeholder)."""
    with psycopg.connect(dsn) as conn, conn.cursor() as cur:
        cur.execute(
            """
            insert into m2mr.media_assets (
                source, source_entity, source_entity_id, source_media_id,
                original_url, public_url
            )
            values ('RG', 'animal', %s, null, %s, '')
            on conflict (source, original_url) do nothing
            """,
            (rg_animal_id, original_url),
        )
        conn.commit()


async def main() -> None:
    ap = argparse.ArgumentParser()
    src = ap.add_mutually_exclusive_group(required=True)
    src.add_argument("--in", dest="in_path", help="normalized animals jsonl (importer output)")
    src.add_argument("--from-api", action="store_true", help="pull the animal list live from the M2M API")
    ap.add_argument("--api-base", default=os.getenv("M2M_API_BASE") or os.getenv("WP_BASE_URL") or "",
                    help="M2M WP base URL for --from-api (default: $M2M_API_BASE or $WP_BASE_URL)")
    ap.add_argument("--limit", type=int, default=0, help="max URLs to attempt this run (0 = all)")
    ap.add_argument("--concurrency", type=int, default=4)
    ap.add_argument("--progress-every", type=int, default=100)
    ap.add_argument("--dead-report", default="data/dead_rg_urls.jsonl")
    args = ap.parse_args()

    dsn = os.getenv("DATABASE_URL")
    if not dsn:
        raise SystemExit("DATABASE_URL not set. Run: source .env.local")
    if not os.getenv("BLOB_READ_WRITE_TOKEN"):
        raise SystemExit("BLOB_READ_WRITE_TOKEN not set. Run: source .env.local")

    if args.in_path:
        animals = load_animals_from_jsonl(args.in_path)
        print(f"loaded {len(animals)} animals from {args.in_path}")
    else:
        if not args.api_base:
            raise SystemExit("--from-api needs --api-base (or $M2M_API_BASE / $WP_BASE_URL)")
        animals = load_animals_from_api(args.api_base)
        print(f"loaded {len(animals)} animals from {args.api_base}")

    wanted = collect_from_animals(animals)
    existing = fetch_existing_urls(dsn)

    # Every URL in the current feed counts as "seen" whether or not it needs
    # uploading — keeps the stale-blob cleanup from touching live dogs.
    seen_bumped = bump_last_seen(dsn, list(wanted.keys()))
    print(f"last_seen_at bumped for {seen_bumped} existing mappings")
    todo = [(u, aid) for u, aid in wanted.items() if u not in existing]
    skipped = len(wanted) - len(todo)
    if args.limit:
        todo = todo[: args.limit]

    print(f"distinct RG urls: {len(wanted)} | already mapped/tombstoned: {skipped} | attempting: {len(todo)}")

    os.makedirs(os.path.dirname(args.dead_report) or ".", exist_ok=True)

    sem = asyncio.Semaphore(max(1, args.concurrency))
    uploaded = 0
    dead = 0
    failed = 0
    done = 0
    started = time.time()
    dead_lock = asyncio.Lock()

    async def handle(original_url: str, rg_animal_id: str) -> None:
        nonlocal uploaded, dead, failed, done
        try:
            async with sem:
                await cache_rg_image_to_blob(
                    dsn=dsn,
                    original_url=original_url,
                    rg_animal_id=rg_animal_id,
                    rg_media_id=None,
                )
            uploaded += 1
        except Exception as e:
            msg = str(e)
            if msg.startswith("404_not_found:"):
                try:
                    await asyncio.to_thread(record_dead_url, dsn, original_url, rg_animal_id)
                except Exception as db_err:
                    print(f"WARN: failed to tombstone dead url {original_url}: {db_err}", file=sys.stderr)
                async with dead_lock:
                    with open(args.dead_report, "a", encoding="utf-8") as f:
                        f.write(json.dumps({"originalUrl": original_url, "rgAnimalId": rg_animal_id}) + "\n")
                dead += 1
            else:
                failed += 1
                print(f"FAILED {original_url}: {msg[:160]}", file=sys.stderr)
        finally:
            done += 1
            if args.progress_every and done % args.progress_every == 0:
                rate = done / max(1.0, time.time() - started)
                eta_min = (len(todo) - done) / max(0.1, rate) / 60
                print(
                    f"progress {done}/{len(todo)} uploaded={uploaded} dead={dead} failed={failed} "
                    f"({rate:.1f}/s, ~{eta_min:.0f}m left)",
                    flush=True,
                )

    await asyncio.gather(*(handle(u, aid) for u, aid in todo))

    print("done")
    print("urls_total:", len(wanted))
    print("urls_skipped_existing:", skipped)
    print("urls_attempted:", len(todo))
    print("uploaded:", uploaded)
    print("dead_404:", dead)
    print("failed_transient:", failed)

    # Fail the run (and trip CI alerting) only on systemic failure — a few
    # transient stragglers self-heal on the next run since they stay unmapped.
    if len(todo) > 20 and failed > len(todo) * 0.1:
        raise SystemExit(f"too many failures: {failed}/{len(todo)}")


if __name__ == "__main__":
    asyncio.run(main())
