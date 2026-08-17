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


PROMPT = """Find dog events happening in the next 60 days in and around {city}, {region} ({country}).

Search the live web the way a local events editor would. Many dog events are only announced on Facebook, so also search for: "{city} dog events this weekend", local news and TV weekend roundups, the city tourism calendar (visit-{city} style sites), breweries and dog bars in {city} (their own sites list yappy hours), rescue organizations' adoption days and fundraisers, PetSmart/Petco adoption event pages, Eventbrite, and AllEvents-style listings.

THE BAR CHANGED ON 2026-08-16. READ THIS PART CAREFULLY.

You are NOT feeding a human moderation queue any more. Auto-triage publishes what you send, so anything you cannot substantiate becomes work for a person instead of a lead for one. An earlier version of this prompt told you that a lead with partial detail was more valuable than nothing. That is now false. A vague submission is worse than no submission, because a human has to read it and then throw it away.

Send an event ONLY if you can satisfy ALL THREE of these:

1. SOMEWHERE TO TURN UP. You have a venue name OR a street address. "Denver, CO" is a topic, not a place. If you cannot find either, drop the event.

2. IT IS A REAL DOG EVENT. Same bar as before. Do NOT drop an event just because the page never spells out the dog situation. A "Tucson Dog Festival" with no sentence saying "dogs welcome" is still a real find and still belongs in the queue for a human.
   What is new is that when the page DOES say it, you must quote it. Copy the exact sentence or phrase into `dogEvidence`, VERBATIM, no paraphrasing and no cleaning up, even if the grammar is bad. It is never shown to visitors, it is machine-checked, and it is what lets the event publish without a human reading it.

3. IT IS A SPECIFIC OCCURRENCE. A real event on a real date, not an awareness day, not a "check local businesses for activities" roundup, and never a reference page (Wikipedia, Britannica, National Day Calendar). A recurring event is fine: give the next occurrence and describe the recurrence.

Set `dogPolicy` to exactly one of:
  "dogs-welcome"  the page says a visitor may bring their own dog. Quote that sentence
                  in dogEvidence.
  "find-a-dog"    an adoption, rescue or shelter event. The dogs there belong to the
                  organization and are looking for people. Quote the sentence that
                  shows this. Do NOT mark it dogs-welcome unless the page separately
                  says visitors may bring their own dog too.
  "unclear"       a real dog event, but the page does not actually say either way.
                  Set dogEvidence to null. STILL SEND IT. A human will decide.

NEVER GUESS BETWEEN THE FIRST TWO. "unclear" is the honest answer and it costs nothing, because it just means a person looks. Guessing wrong is what sends someone to an adoption day with their own dog, or tells them to leave their dog home when it was welcome. If you are unsure, "unclear" is correct.

Quality over volume on requirements 1 and 3: do not pad with vague listings. But do not withhold a real event just because you could not quote it.

Respond with ONLY a JSON array (no prose before or after). Each element:
{{
  "title": "...",
  "startDateTime": "YYYY-MM-DDTHH:MM" (best known; date-only OK as YYYY-MM-DD),
  "endDateTime": "..." or null,
  "venueName": "..." (required unless addressLine1 is present),
  "addressLine1": "street address" (required unless venueName is present),
  "url": "the event page URL",
  "description": "1-2 original sentences, warm dog-lover tone, no copied text",
  "dogPolicy": "dogs-welcome" | "find-a-dog" | "unclear",
  "dogEvidence": "the VERBATIM sentence from the page about dogs" (null if unclear),
  "organizerName": "..." or null,
  "organizerEmail": "only if publicly listed on the event/organizer page, else null"
}}

Note `description` and `dogEvidence` are different on purpose. `description` is your own warm original prose and is shown to visitors. `dogEvidence` is a raw copied quote, is never displayed, and exists only so a machine can verify your claim. Never put your own words in `dogEvidence`.

Return [] if you find nothing that clears requirements 1 and 3. Maximum 12 events."""


def scout_hub(
    hub: dict, api_key: str, model: str
) -> tuple[list[dict], str | None]:
    """Returns (events, error).

    `error` is None when the API answered and we understood it, INCLUDING the
    legitimate case of a city with nothing on. It is a string when the run did
    not actually happen: HTTP failure, a response with no JSON array in it, or
    unparseable JSON.

    The distinction is the whole point. Before this, every one of those paths
    returned [] and was indistinguishable from a quiet week, so main() summed
    zeros, printed "Done. 0 found", and exited 0. The workflow has an
    `if: failure()` alert step and always has; it never fired because the
    sensor never tripped. The credit balance running out looked exactly like
    a Sunday with no dog events in 22 cities.
    """
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
        msg = f"HTTP {r.status_code}: {r.text[:200]}"
        print(f"  API error {msg}")
        return [], msg
    data = r.json()
    blocks = data.get("content", [])
    text = "".join(b.get("text", "") for b in blocks if b.get("type") == "text")
    m = re.search(r"\[[\s\S]*\]", text)
    if not m:
        stop = data.get("stop_reason")
        print(f"  debug: stop_reason={stop} "
              f"searches_used={sum(1 for b in blocks if b.get('type') == 'server_tool_use')} "
              f"text_tail={text[-200:]!r}")
        # `end_turn` with real text and no array is the model saying it found
        # nothing, which is a genuine answer. Anything else (max_tokens, an
        # empty body, a refusal) means the reply was cut off or never arrived,
        # and that is a failure, not a quiet city.
        if stop == "end_turn" and text.strip():
            return [], None
        return [], f"no JSON array in the reply (stop_reason={stop})"
    try:
        events = json.loads(m.group(0))
    except json.JSONDecodeError as err:
        # This branch used to return [] with NO message at all, so a malformed
        # reply was the quietest failure of the three.
        print(f"  JSON parse failed: {err}")
        return [], f"unparseable JSON: {err}"
    out = []
    for ev in events if isinstance(events, list) else []:
        if not isinstance(ev, dict):
            continue
        title = str(ev.get("title") or "").strip()
        url = str(ev.get("url") or "").strip()
        start = str(ev.get("startDateTime") or "").strip()
        if len(title) < 3 or not url.startswith("http") or not start:
            continue
        # Enforce the three requirements in code, not just in the prompt. A
        # prompt is a request; this is the contract. Every drop is printed so
        # a sudden collapse in yield is visible rather than silent.
        venue = str(ev.get("venueName") or "").strip()
        addr = str(ev.get("addressLine1") or "").strip()
        if not venue and not addr:
            # The ONLY thing this script drops outright. Such a row cannot
            # tell anyone where to turn up, and isPublishableEvent already
            # refuses it downstream, so it could only ever have sat in the
            # queue. The URL is printed so a dropped lead is still
            # recoverable from the run log rather than silently gone.
            print(f"  drop (no venue, no address): {title[:60]} | {url}")
            continue
        # Missing evidence DOWNGRADES to unclear, it never drops the event.
        # Dropping would be strictly worse than the old behaviour: a real find
        # the scout simply could not quote would go from "in the queue for
        # review" to "never submitted", and Dee would never see it. The only
        # thing that changes for these is that they still need a human, which
        # is exactly what happened before.
        policy = str(ev.get("dogPolicy") or "").strip()
        evidence = str(ev.get("dogEvidence") or "").strip()
        # A quote too short to hold a clause is a label, not evidence.
        if policy not in ("dogs-welcome", "find-a-dog") or len(evidence) < 15:
            if policy in ("dogs-welcome", "find-a-dog"):
                print(f"  downgrade to unclear (claim without a usable quote): {title[:60]}")
            policy, evidence = "unclear", ""
        ev["dogPolicy"] = policy
        ev["dogEvidence"] = evidence
        # Date sanity: the model occasionally returns past events (last year's
        # Halloween contest etc.). Drop anything that started before yesterday;
        # unparseable dates pass through for the human moderator to judge.
        try:
            if _date.fromisoformat(start[:10]) < _date.today() - _td(days=1):
                continue
        except ValueError:
            pass
        out.append(ev)
    return out, None


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
    failures: list[tuple[str, str]] = []
    for hub in hubs:
        label = f"{hub['city']}, {hub['regionCode']}"
        print(f"\n=== {label} ===", flush=True)
        events, err = scout_hub(hub, api_key, model)
        if err:
            failures.append((label, err))
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
            # `screening` answers "is this spam". The nested `scout` key is a
            # separate namespace for what the scout OBSERVED, kept here rather
            # than in new columns so this needs no migration. triageSubmissions
            # re-runs the audited eventPolicy rules against `dogEvidence`, so
            # the scout's claim is checked, never trusted.
            screening = {
                "verdict": "pass",
                "score": 5,
                "reasons": [
                    "found by SnoutHub Event Scout",
                    f"source: {ev.get('url')}",
                ] + ([f"organizer contact: {org} {oem}".strip()] if (org or oem) else []),
                "scout": {
                    "dogPolicy": ev["dogPolicy"],
                    "dogEvidence": ev["dogEvidence"][:1000],
                    "promptVersion": "2026-08-16-evidence",
                },
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

    # A city with nothing on is a fine answer and exits 0. A city the scout
    # could not reach is not, and must exit non-zero so the workflow's
    # `if: failure()` alert step actually runs.
    #
    # Partial failures fail the run too, on purpose. Any rows found before the
    # failure are already committed, so nothing is lost by exiting 1 - the only
    # thing that changes is that a sweep which quietly lost 6 of 22 cities
    # stops looking identical to one that swept all 22.
    if failures:
        print(f"\nFAILED on {len(failures)} of {len(hubs)} cities. "
              "These were NOT searched, so a low count above does not mean "
              "there was nothing to find:")
        for label, reason in failures:
            print(f"  - {label}: {reason}")
        if len(failures) == len(hubs):
            print("\nEVERY city failed. That is an API or credential problem, "
                  "not a quiet week. Check the Anthropic credit balance and the "
                  "key before assuming the scout has run dry.")
        return 1

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
