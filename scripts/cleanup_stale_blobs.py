# scripts/cleanup_stale_blobs.py
#
# Delete Vercel Blob images (and their m2mr.media_assets rows) for dogs that
# left the RescueGroups feed more than --days ago. Caps blob storage growth:
# without this, images of adopted dogs accumulate forever.
#
# Staleness signal: media_assets.last_seen_at, bumped for every URL in the
# current feed by scripts/rehost_media_assets.py (which runs on every import).
# IMPORTANT: only run this after a rehost pass has bumped the current feed,
# which the import.yml step ordering guarantees.
#
# Shared-blob safety: blobs are stored content-addressed (pets/{sha}.{ext}),
# so multiple original_urls (e.g. littermates sharing a photo) can point at
# ONE blob object. A blob is only deleted when every row referencing its
# public_url is stale; stale rows are deleted regardless (the mapping is no
# longer needed — if the dog ever returns, the importer re-rehosts it).
#
# Tombstones (public_url='') are never touched: they're byte-free and prevent
# re-downloading known-dead RG URLs.
#
# Run:
#   source .env.local
#   python scripts/cleanup_stale_blobs.py --dry-run          # report only
#   python scripts/cleanup_stale_blobs.py --days 90

import os
import sys

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

import argparse
import asyncio

import psycopg
from vercel.blob import AsyncBlobClient


def fetch_candidates(dsn: str, days: int) -> tuple[list[tuple[int, str]], set[str]]:
    """Returns (stale rows as (id, public_url), deletable blob public_urls)."""
    with psycopg.connect(dsn) as conn, conn.cursor() as cur:
        cur.execute(
            """
            select id, public_url
            from m2mr.media_assets
            where source='RG'
              and public_url <> ''
              and last_seen_at < now() - make_interval(days => %s)
            """,
            (days,),
        )
        stale_rows = [(r[0], r[1]) for r in cur.fetchall()]

        if not stale_rows:
            return [], set()

        # A blob object is deletable only if NO fresh row shares its public_url.
        stale_urls = list({u for _, u in stale_rows})
        shared_with_live: set[str] = set()
        chunk = 5000
        for i in range(0, len(stale_urls), chunk):
            cur.execute(
                """
                select distinct public_url
                from m2mr.media_assets
                where source='RG'
                  and public_url = any(%s)
                  and last_seen_at >= now() - make_interval(days => %s)
                """,
                (stale_urls[i : i + chunk], days),
            )
            shared_with_live.update(r[0] for r in cur.fetchall())

        deletable_blobs = {u for u in stale_urls if u not in shared_with_live}
        return stale_rows, deletable_blobs


def delete_rows(dsn: str, row_ids: list[int], chunk: int = 5000) -> int:
    deleted = 0
    with psycopg.connect(dsn) as conn, conn.cursor() as cur:
        for i in range(0, len(row_ids), chunk):
            cur.execute(
                "delete from m2mr.media_assets where id = any(%s)",
                (row_ids[i : i + chunk],),
            )
            deleted += cur.rowcount
        conn.commit()
    return deleted


async def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--days", type=int, default=90)
    ap.add_argument("--limit", type=int, default=0, help="max blobs to delete this run (0 = all)")
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()

    dsn = os.getenv("DATABASE_URL")
    if not dsn:
        raise SystemExit("DATABASE_URL not set. Run: source .env.local")
    if not args.dry_run and not os.getenv("BLOB_READ_WRITE_TOKEN"):
        raise SystemExit("BLOB_READ_WRITE_TOKEN not set. Run: source .env.local")

    stale_rows, deletable_blobs = fetch_candidates(dsn, args.days)
    blobs = sorted(deletable_blobs)
    if args.limit:
        blobs = blobs[: args.limit]
    blob_set = set(blobs)

    print(f"stale rows (> {args.days}d unseen): {len(stale_rows)}")
    print(f"deletable blob objects: {len(deletable_blobs)} (this run: {len(blobs)})")

    if args.dry_run:
        rows_would = [
            rid for rid, url in stale_rows if url in blob_set or url not in deletable_blobs
        ]
        print(f"rows that would be deleted: {len(rows_would)}")
        print("[dry-run] nothing deleted")
        return

    # Delete blob objects first; a row is only dropped once its blob is
    # actually gone (or was never deletable because a live row shares it) —
    # otherwise a failed batch would orphan blobs with no row pointing at them.
    blob_failed = 0
    deleted_blob_urls: set[str] = set()
    if blobs:
        client = AsyncBlobClient()
        chunk = 100
        for i in range(0, len(blobs), chunk):
            batch = blobs[i : i + chunk]
            try:
                await client.delete(batch)
                deleted_blob_urls.update(batch)
            except Exception as e:
                blob_failed += len(batch)
                print(f"WARN: blob delete batch failed: {str(e)[:160]}", file=sys.stderr)

    rows_to_delete = [
        rid
        for rid, url in stale_rows
        if url in deleted_blob_urls or url not in deletable_blobs
    ]
    deleted_rows = delete_rows(dsn, rows_to_delete)

    print("done")
    print("blobs_deleted:", len(deleted_blob_urls))
    print("blob_delete_failures:", blob_failed)
    print("rows_deleted:", deleted_rows)

    if blobs and blob_failed > len(blobs) * 0.5:
        raise SystemExit("majority of blob deletions failed")


if __name__ == "__main__":
    asyncio.run(main())
