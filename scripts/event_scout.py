#!/usr/bin/env python3
"""SnoutHub Event Scout.

For each hub city, asks Claude (with live web search) to find real,
verifiable dog-friendly events in the next 60 days, the same way a human
editor would: organizer pages, park districts, breweries, Eventbrite,
city calendars. Results are deduped against what the site already has and
inserted as PENDING rows in event_submissions, so they appear in the
/admin/events moderation queue pre-labeled as scout finds. Nothing goes
live without a human click.

Usage (from repo root, .env.local must have ANTHROPIC_API_KEY + DATABASE_URL):

    python3 scripts/event_scout.py --limit 3          # test: first 3 hubs
    python3 scripts/event_scout.py --hub phoenix-az   # one hub
    python3 scripts/event_scout.py                    # all 36 priority hubs
    python3 scripts/event_scout.py --dry-run          # find, print, no insert

Cost: ~4-6 web searches per hub via the Anthropic API; a full 36-hub pass
is roughly $2-4. Weekly cadence keeps it under ~$15/mo.
"""
from __future__ import annotations

import argparse
import json
from datetime import date as _date, timedelta as _td
import os
import re
import sys
import uuid
from pathlib import Path

import requests

ROOT = Path(__file__).resolve().parent.parent
HUBS_FILE = ROOT / "data_events" / "hubs.events_priority.top36.json"
API_URL = "https://api.anthropic.com/v1/messages"


def load_env() -> dict:
    env = dict(os.environ)
    f = ROOT / ".env.local"
    if f.exists():
        for raw in f.read_text().splitlines():
            line = raw.strip()
            if line.startswith("export "):
                line = line[len("export "):]
            if not line or line.startswith("#") or "=" not in line:
                continue
            k, _, v = line.partition("=")
            env.setdefault(k.strip(), v.strip().strip('"').strip("'"))
    return env


PROMPT = """Find dog-friendly public events happening in the next 60 days in and around {city}, {region} ({country}).

Search the live web the way a local events editor would. Many dog events are only announced on Facebook, so also search for: "{city} dog events this weekend", local news and TV station weekend roundups, the city's tourism calendar (visit-{city} style sites), breweries and dog bars in {city} (their own sites list yappy hours), rescue organizations' adoption days and fundraisers, PetSmart/Petco adoption event pages, Eventbrite, and AllEvents-style listings. IMPORTANT: you are feeding a HUMAN MODERATION QUEUE, not publishing directly, so include every real event you find rather than holding back: a lead with partial detail is far more valuable than nothing. The bar is: a plausible real event with a title, a date (or recurring pattern) inside the window, and the URL where you saw it. Aggregator pages (allevents.in, bringfido, eventbrite lists, news roundups) are perfectly acceptable source URLs; so are public Facebook event announcements. Missing time, address, or organizer are fine: use null. Recurring events (a standing weekday or Saturday yappy hour) count: use the next occurrence date and mention the recurrence in the description. Only skip things that are clearly expired or clearly not real.

Respond with ONLY a JSON array (no prose before or after). Each element:
{{
  "title": "...",
  "startDateTime": "YYYY-MM-DDTHH:MM" (best known; date-only OK as YYYY-MM-DD),
  "endDateTime": "..." or null,
  "venueName": "..." or null,
  "addressLine1": "street address" or null,
  "url": "the event page URL",
  "description": "1-2 original sentences, warm dog-lover tone, no copied text",
  "organizerName": "..." or null,
  "organizerEmail": "only if publicly listed on the event/organizer page, else null"
}}

Return [] if you find nothing verifiable. Maximum 12 events."""


def scout_hub(hub: dict, api_key: str, model: str) -> list[dict]:
    body = {
        "model": model,
        "max_tokens": 12000,
        "tools": [{
            "type": "web_search_20250305",
            "name": "web_search",
            "max_uses": 6,
        }],
        "messages": [{
            "role": "user",
            "content": PROMPT.format(
                city=hub["city"], region=hub["regionCode"], country=hub["countryCode"]
            ),
        }],
    }
    r = requests.post(
        API_URL,
        headers={
            "x-api-key": api_key,
            "anthropic-version": "2023-06-01",
            "content-type": "application/json",
        },
        json=body,
        timeout=300,
    )
    if r.status_code != 200:
        print(f"  API error {r.status_code}: {r.text[:200]}")
        return []
    data = r.json()
    blocks = data.get("content", [])
    text = "".join(b.get("text", "") for b in blocks if b.get("type") == "text")
    m = re.search(r"\[[\s\S]*\]", text)
    if not m:
        print(f"  debug: stop_reason={data.get('stop_reason')} "
              f"searches_used={sum(1 for b in blocks if b.get('type') == 'server_tool_use')} "
              f"text_tail={text[-200:]!r}")
        return []
    try:
        events = json.loads(m.group(0))
    except json.JSONDecodeError:
        return []
    out = []
    for ev in events if isinstance(events, list) else []:
        if not isinstance(ev, dict):
            continue
        title = str(ev.get("title") or "").strip()
        url = str(ev.get("url") or "").strip()
        start = str(ev.get("startDateTime") or "").strip()
        if len(title) < 3 or not url.startswith("http") or not start:
            continue
        # Date sanity: the model occasionally returns past events (last year's
        # Halloween contest etc.). Drop anything that started before yesterday;
        # unparseable dates pass through for the human moderator to judge.
        try:
            if _date.fromisoformat(start[:10]) < _date.today() - _td(days=1):
                continue
        except ValueError:
            pass
        out.append(ev)
    return out


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--hubs-file", default=str(HUBS_FILE))
    ap.add_argument("--limit", type=int, default=0, help="only the first N hubs")
    ap.add_argument("--offset", type=int, default=0, help="skip the first N hubs (e.g. 36 = only the non-priority cities)")
    ap.add_argument("--hub", default="", help="single hubId, e.g. phoenix-az")
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()

    env = load_env()
    api_key = (env.get("ANTHROPIC_API_KEY") or "").strip()
    if not api_key:
        sys.exit("ANTHROPIC_API_KEY missing (add `export ANTHROPIC_API_KEY=...` to .env.local)")
    model = (env.get("ANTHROPIC_SCOUT_MODEL") or "").strip() or "claude-sonnet-4-5"

    hubs = json.load(open(args.hubs_file))
    if args.hub:
        hubs = [h for h in hubs if h["hubId"] == args.hub]
    if args.offset:
        hubs = hubs[args.offset :]
    if args.limit:
        hubs = hubs[: args.limit]
    if not hubs:
        sys.exit("no hubs matched")

    conn = None
    seen: set[tuple[str, str]] = set()
    if not args.dry_run:
        import psycopg
        db = (env.get("DATABASE_URL") or "").strip()
        if not db:
            sys.exit("DATABASE_URL missing from .env.local")
        conn = psycopg.connect(db)
        with conn.cursor() as cur:
            cur.execute("select lower(title), lower(city) from events where starts_at is null or starts_at > now() - interval '1 day'")
            seen.update((a or "", b or "") for a, b in cur.fetchall())
            cur.execute("select lower(title), lower(city) from event_submissions where created_at > now() - interval '90 days'")
            seen.update((a or "", b or "") for a, b in cur.fetchall())
        print(f"Dedupe set: {len(seen)} existing title+city pairs")

    total_found = total_new = 0
    for hub in hubs:
        label = f"{hub['city']}, {hub['regionCode']}"
        print(f"\n=== {label} ===", flush=True)
        events = scout_hub(hub, api_key, model)
        total_found += len(events)
        print(f"  found {len(events)} verifiable events")
        for ev in events:
            key = (ev["title"].lower(), hub["city"].lower())
            if key in seen:
                print(f"  skip (already have): {ev['title'][:50]}")
                continue
            seen.add(key)
            total_new += 1
            org = ev.get("organizerName") or ""
            oem = ev.get("organizerEmail") or ""
            print(f"  NEW: {ev['title'][:60]} | {ev.get('startDateTime')} | organizer: {org or 'n/a'} {('<' + oem + '>') if oem else ''}")
            if args.dry_run or conn is None:
                continue
            screening = {
                "verdict": "pass",
                "score": 5,
                "reasons": [
                    "found by SnoutHub Event Scout",
                    f"source: {ev.get('url')}",
                ] + ([f"organizer contact: {org} {oem}".strip()] if (org or oem) else []),
            }
            with conn.cursor() as cur:
                cur.execute(
                    """
                    insert into event_submissions (
                      id, status, title, start_date_time, end_date_time, city,
                      region_code, country_code, venue_name, address_line1, url,
                      description, submitter_name, submitter_email, screening
                    ) values (%s,'pending',%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                    """,
                    (
                        str(uuid.uuid4()), ev["title"][:200],
                        str(ev.get("startDateTime"))[:60],
                        (str(ev.get("endDateTime"))[:60] if ev.get("endDateTime") else None),
                        hub["city"], hub["regionCode"], hub["countryCode"],
                        (str(ev.get("venueName"))[:200] if ev.get("venueName") else None),
                        (str(ev.get("addressLine1"))[:200] if ev.get("addressLine1") else None),
                        str(ev.get("url"))[:300],
                        (str(ev.get("description"))[:2000] if ev.get("description") else None),
                        "SnoutHub Event Scout",
                        "scout@snouthub.com",
                        json.dumps(screening),
                    ),
                )
            conn.commit()

    print(f"\nDone. {total_found} found, {total_new} new -> moderation queue"
          + (" (dry run, nothing written)" if args.dry_run else ""))
    print("Review them at your SnoutHub Event Queue bookmark.")
    if conn:
        conn.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
