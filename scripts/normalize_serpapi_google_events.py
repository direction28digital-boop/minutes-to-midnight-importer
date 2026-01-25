#!/usr/bin/env python3
"""
Normalize SerpAPI google_events raw JSONL into a deduped events.normalized.jsonl.

Reads raw lines like those produced by scripts/serpapi_google_events_burn.py:
- expects obj.serpapiStatus == "Success"
- expects obj.response.events_results (list)

Outputs JSONL where each line is a normalized event record with a stable id.

Usage:
  python3 scripts/normalize_serpapi_google_events.py \
    --in-glob "data_events/raw/**/*.jsonl" \
    --out "data_events/events.normalized.jsonl"
"""

from __future__ import annotations

import argparse
import glob
import hashlib
import json
import os
from datetime import datetime, timezone
from typing import Any, Dict, Iterable, List, Optional, Tuple


def sha1_hex(s: str) -> str:
    return hashlib.sha1(s.encode("utf-8")).hexdigest()


def norm_str(x: Any) -> str:
    if x is None:
        return ""
    if isinstance(x, str):
        return " ".join(x.strip().lower().split())
    return " ".join(str(x).strip().lower().split())


def parse_iso(ts: Any) -> Optional[datetime]:
    if not isinstance(ts, str) or not ts:
        return None
    try:
        ts = ts.replace("Z", "+00:00")
        return datetime.fromisoformat(ts)
    except Exception:
        return None


def get_event_id(ev: Dict[str, Any]) -> Optional[str]:
    # SerpAPI keys vary; cover common variants
    for k in ("event_id", "eventId", "eventid", "id"):
        v = ev.get(k)
        if isinstance(v, str) and v.strip():
            return v.strip()
    return None


def make_stable_id(
    hub: Dict[str, Any],
    ev: Dict[str, Any],
) -> Tuple[str, Optional[str]]:
    source_event_id = get_event_id(ev)
    if source_event_id:
        return f"serpapi_google_events:{source_event_id}", source_event_id

    title = norm_str(ev.get("title"))
    date_raw = ev.get("date")
    date_s = norm_str(date_raw if isinstance(date_raw, str) else json.dumps(date_raw, ensure_ascii=False))
    address = ev.get("address")
    if isinstance(address, list):
        addr_s = norm_str(" | ".join([str(a) for a in address]))
    else:
        addr_s = norm_str(address)

    city = norm_str(hub.get("city"))
    region = norm_str(hub.get("regionCode"))
    country = norm_str(hub.get("countryCode"))

    key = f"{title}|{date_s}|{addr_s}|{city}|{region}|{country}"
    return f"serpapi_google_events:sha1:{sha1_hex(key)}", None


def iter_raw_lines(paths: Iterable[str]) -> Iterable[Dict[str, Any]]:
    for p in paths:
        try:
            with open(p, "r", encoding="utf-8") as f:
                for line in f:
                    line = line.strip()
                    if not line:
                        continue
                    try:
                        obj = json.loads(line)
                    except Exception:
                        continue
                    yield obj
        except FileNotFoundError:
            continue


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--in-glob", default="data_events/raw/**/*.jsonl")
    ap.add_argument("--out", default="data_events/events.normalized.jsonl")
    args = ap.parse_args()

    in_paths = sorted(glob.glob(args.in_glob, recursive=True))
    if not in_paths:
        print(f"no input files matched: {args.in_glob}")
        return 2

    # id -> (record, fetchedAt_dt)
    best: Dict[str, Tuple[Dict[str, Any], Optional[datetime]]] = {}

    raw_lines = 0
    ok_lines = 0
    events_seen = 0

    for obj in iter_raw_lines(in_paths):
        raw_lines += 1
        if obj.get("serpapiStatus") != "Success":
            continue
        resp = obj.get("response")
        if not isinstance(resp, dict):
            continue

        ok_lines += 1

        hub = obj.get("hub") if isinstance(obj.get("hub"), dict) else {}
        qinfo = obj.get("query") if isinstance(obj.get("query"), dict) else {}
        fetched_at = parse_iso(obj.get("ts"))

        events = resp.get("events_results")
        if not isinstance(events, list):
            continue

        for ev in events:
            if not isinstance(ev, dict):
                continue
            events_seen += 1

            stable_id, source_event_id = make_stable_id(hub, ev)

            rec: Dict[str, Any] = {
                "id": stable_id,
                "source": "serpapi_google_events",
                "sourceEventId": source_event_id,

                "hubId": hub.get("hubId"),
                "city": hub.get("city"),
                "regionCode": hub.get("regionCode"),
                "countryCode": hub.get("countryCode"),

                # Main fields (keep raw date since Google Events strings are not reliably parseable)
                "title": ev.get("title"),
                "date": ev.get("date"),
                "address": ev.get("address"),
                "link": ev.get("link"),
                "thumbnail": ev.get("thumbnail"),
                "description": ev.get("description"),
                "venue": ev.get("venue"),

                # Query context (useful for debugging/ranking)
                "query": {
                    "term": qinfo.get("term"),
                    "htichips": qinfo.get("htichips"),
                    "start": qinfo.get("start"),
                    "q": qinfo.get("q"),
                },

                "fetchedAt": obj.get("ts"),
            }

            prev = best.get(stable_id)
            if prev is None:
                best[stable_id] = (rec, fetched_at)
            else:
                _, prev_dt = prev
                # keep the newest fetched record
                if prev_dt is None or (fetched_at is not None and fetched_at > prev_dt):
                    best[stable_id] = (rec, fetched_at)

    out_dir = os.path.dirname(args.out)
    if out_dir:
        os.makedirs(out_dir, exist_ok=True)

    with open(args.out, "w", encoding="utf-8") as out_f:
        for stable_id in sorted(best.keys()):
            rec, _dt = best[stable_id]
            out_f.write(json.dumps(rec, ensure_ascii=False) + "\n")

    print(f"in_files={len(in_paths)} raw_lines={raw_lines} ok_lines={ok_lines} events_seen={events_seen} unique_events={len(best)}")
    print(f"wrote {args.out}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
