#!/usr/bin/env python3
"""
Upsert normalized SerpAPI events into WordPress.

Reads data_events/events.normalized.jsonl (output of normalize_serpapi_google_events.py)
and upserts each event to the WordPress m2mr_event custom post type.

Lookup is by slug (derived from the stable event id) so re-runs are idempotent.

Requires env vars:
  WP_BASE_URL  e.g. https://example.com
  WP_USER
  WP_PASSWORD

Optional:
  EVENTS_NORMALIZED_PATH  (default: data_events/events.normalized.jsonl)
  WP_MAX_UPSERT           (default: 0 = unlimited)
  WP_DRY_RUN              (set to 1 to print without posting)
"""

from __future__ import annotations

import json
import os
import re
import sys
from pathlib import Path

import requests


def _env_int(name: str, default: int) -> int:
    raw = os.environ.get(name, "").strip()
    try:
        return int(raw) if raw else default
    except ValueError:
        return default


WP_BASE_URL = os.environ.get("WP_BASE_URL", "").rstrip("/")
WP_USER = os.environ.get("WP_USER", "")
WP_PASSWORD = os.environ.get("WP_PASSWORD", "")
WP_MAX_UPSERT = _env_int("WP_MAX_UPSERT", 0)
WP_DRY_RUN = os.environ.get("WP_DRY_RUN", "0").strip() == "1"
EVENTS_PATH = Path(os.environ.get("EVENTS_NORMALIZED_PATH", "data_events/events.normalized.jsonl"))


def slugify(s: str) -> str:
    s = s.lower()
    s = re.sub(r"[^a-z0-9]+", "-", s)
    return s.strip("-")[:200]


def event_slug(ev: dict) -> str:
    return slugify(ev.get("id") or "")


def wp_upsert(session: requests.Session, base_url: str, ev: dict) -> None:
    slug = event_slug(ev)
    title = ev.get("title") or slug or "Dog Event"

    payload = {
        "title": title,
        "status": "publish",
        "content": json.dumps(ev, ensure_ascii=False),
        "slug": slug,
    }

    if WP_DRY_RUN:
        print(f"[dry_run] would upsert slug={slug!r} title={title!r}")
        return

    endpoint = f"{base_url}/wp-json/wp/v2/m2mr_event"
    auth = (WP_USER, WP_PASSWORD)

    # Look up by slug
    get_resp = session.get(endpoint, params={"slug": slug, "status": "any"}, auth=auth, timeout=60)
    if not get_resp.ok:
        print(f"WARN lookup failed slug={slug!r} status={get_resp.status_code}", file=sys.stderr)
        return

    existing = get_resp.json()
    if isinstance(existing, list) and existing:
        post_id = existing[0]["id"]
        resp = session.post(f"{endpoint}/{post_id}", json=payload, auth=auth, timeout=60)
        action = "updated"
    else:
        resp = session.post(endpoint, json=payload, auth=auth, timeout=60)
        action = "created"

    if resp.ok:
        print(f"{action} id={resp.json().get('id')} slug={slug!r}")
    else:
        print(f"ERROR {action} slug={slug!r} status={resp.status_code} {resp.text[:200]}", file=sys.stderr)


def main() -> int:
    if not WP_DRY_RUN:
        missing = [v for v in ("WP_BASE_URL", "WP_USER", "WP_PASSWORD") if not os.environ.get(v)]
        if missing:
            print(f"ERROR: missing env vars: {', '.join(missing)}", file=sys.stderr)
            return 2

    if not EVENTS_PATH.exists():
        print(f"ERROR: not found: {EVENTS_PATH}", file=sys.stderr)
        return 2

    session = requests.Session()
    count = 0

    with EVENTS_PATH.open("r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                ev = json.loads(line)
            except Exception:
                continue

            wp_upsert(session, WP_BASE_URL, ev)
            count += 1

            if WP_MAX_UPSERT and count >= WP_MAX_UPSERT:
                print(f"reached WP_MAX_UPSERT={WP_MAX_UPSERT}, stopping")
                break

    print(f"done upserted={count} dry_run={WP_DRY_RUN}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
