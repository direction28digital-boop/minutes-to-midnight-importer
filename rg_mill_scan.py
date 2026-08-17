#!/usr/bin/env python3
"""
rg_mill_scan.py v2 — find likely puppy-mill survivors in RescueGroups data.

RescueGroups has NO field for this. Verified against a live response: the
animals object carries 44 attributes and not one describes origin, surrender
reason, seizure circumstances or breeder history. `killReason` is '0' on every
record. `qualities` is a closed vocabulary with no mill term.

The only per-dog signal is free text: descriptionText and specialNeedsDetails.

WHAT CHANGED FROM v1
  v1 pulled the national feed and filtered by state in Python, so
  `--state MO --max-pages 5` scanned 1250 dogs from every state and kept
  whichever handful happened to be Missouri. The denominator was never
  reported, so "2 matches" looked like a finding when it was an artifact.

  v2 filters server-side with filterRadius (the same POST shape proven in
  rg_orgs_by_hub.py), probes whether the API will do the text match for us,
  and always prints the denominator.

Usage
  cd ~/Projects/m2m-rescuegroups-http
  set -a; source .env.local; set +a

  # everything within 150 miles of the Lake
  ./.venv-scan/bin/python rg_mill_scan.py --postal 65065 --miles 150

  # national sweep, no radius (slow, pages the whole available feed)
  ./.venv-scan/bin/python rg_mill_scan.py --national --max-pages 40

Writes mill_candidates.csv and prints a summary.
"""

import argparse
import csv
import html
import os
import re
import sys
import time
from collections import Counter, defaultdict
from pathlib import Path

import requests


def load_api_key():
    key = os.environ.get("RG_API_KEY")
    if key:
        return key
    env = Path(".env.local")
    if env.exists():
        m = re.search(r'RG_API_KEY="?([^"\n]+)"?', env.read_text())
        if m:
            return m.group(1)
    sys.exit("RG_API_KEY not found in env or .env.local")


API_KEY = load_api_key()
URL = "https://api.rescuegroups.org/v5/public/animals/search/available/dogs"
HEADERS = {"Content-Type": "application/vnd.api+json", "Authorization": API_KEY}

SIGNALS = [
    (5, r"puppy\s*mill"),
    (5, r"mill\s+(?:survivor|dog|mama|mom|rescue|breeder)"),
    (5, r"commercial\s+breed(?:er|ing)"),
    (4, r"breeder\s+(?:release|surrender|discard|dump)"),
    (4, r"retired\s+(?:breeder|breeding)"),
    (4, r"USDA[-\s]?(?:licensed|inspected|facility)"),
    (4, r"\bauction\b"),
    (3, r"(?:used|kept)\s+(?:for|as)\s+breeding"),
    (3, r"never\s+(?:lived\s+in\s+a\s+home|been\s+in\s+a\s+home|felt\s+grass|"
        r"walked\s+on\s+grass|had\s+a\s+name|seen\s+the\s+sun)"),
    (3, r"spent\s+(?:her|his|their)\s+(?:whole|entire)\s+life\s+in\s+a\s+(?:cage|crate|kennel)"),
    (3, r"\bhoard(?:er|ing)\b"),
    (2, r"lived\s+in\s+a\s+(?:cage|crate|kennel)"),
    (2, r"\bseiz(?:ed|ure)\b"),
    (2, r"\bAmish\b"),
    (2, r"\bbrood\s+(?:dog|bitch|mama)\b"),
]
COMPILED = [(w, re.compile(p, re.I)) for w, p in SIGNALS]

MILL_BREEDS = {
    "yorkshire terrier", "shih tzu", "maltese", "poodle",
    "cavalier king charles spaniel", "french bulldog", "dachshund",
    "chihuahua", "bichon frise", "pomeranian", "cocker spaniel",
    "havanese", "pekingese", "papillon", "schnauzer",
}


def clean(t):
    if not t:
        return ""
    return re.sub(r"<[^>]+>", " ", html.unescape(t))


def post(body, page, limit=250):
    for attempt in range(1, 6):
        try:
            r = requests.post(f"{URL}?limit={limit}&page={page}",
                              headers=HEADERS, json=body, timeout=60)
        except requests.RequestException as e:
            if attempt == 5:
                raise
            print(f"  page {page} attempt {attempt}: {e}")
            time.sleep(5 * attempt)
            continue
        if r.status_code == 429:
            time.sleep(20)
            continue
        return r
    raise RuntimeError("unreachable")


def probe_server_side_text_filter():
    """Ask the API to do the matching. If it can, we skip the whole feed."""
    body = {"data": {"filters": [{
        "fieldName": "animals.descriptionText",
        "operation": "contains",
        "criteria": "puppy mill",
    }]}}
    r = post(body, 1, limit=5)
    if r.status_code == 200:
        n = (r.json().get("meta") or {}).get("count")
        return True, body, n
    return False, None, f"HTTP {r.status_code}: {r.text[:200]}"


def score(text):
    hits, total = [], 0
    for w, rx in COMPILED:
        m = rx.search(text)
        if m:
            hits.append((w, m.group(0).lower().strip()))
            total += w
    return total, hits


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--postal", help="center postal code, e.g. 65065")
    ap.add_argument("--miles", type=int, default=150)
    ap.add_argument("--national", action="store_true", help="no radius filter")
    ap.add_argument("--max-pages", type=int, default=0, help="0 = all")
    ap.add_argument("--min-score", type=int, default=3)
    ap.add_argument("--out", default="mill_candidates.csv")
    args = ap.parse_args()

    if not args.postal and not args.national:
        sys.exit("Give --postal 65065 (with optional --miles) or --national")

    print("Probing whether RescueGroups will filter description text server-side...")
    ok, server_body, info = probe_server_side_text_filter()
    if ok:
        print(f"  YES. Server-side text filter works. Matching records: {info}")
        print("  Using it, so we only download dogs that already mention it.")
    else:
        print(f"  NO. {info}")
        print("  Falling back to downloading the feed and matching locally.")

    body = {"data": {}}
    if args.postal:
        body["data"]["filterRadius"] = {"miles": args.miles,
                                        "postalcode": args.postal}
    if ok:
        body["data"]["filters"] = server_body["data"]["filters"]

    rows, page, in_scope = [], 1, 0
    org_name, org_loc = {}, {}
    phrase_by_org = defaultdict(Counter)

    while True:
        r = post(body, page)
        r.raise_for_status()
        payload = r.json() if r.text.strip() else {}
        records = payload.get("data", [])
        if not records:
            break

        for inc in payload.get("included", []):
            if inc["type"] == "orgs":
                a = inc.get("attributes", {})
                org_name[inc["id"]] = a.get("name", "")
                org_loc[inc["id"]] = (a.get("city", ""), (a.get("state") or "").upper())

        for rec in records:
            in_scope += 1
            at = rec.get("attributes", {})
            rel = rec.get("relationships", {})
            org_rel = (rel.get("orgs", {}) or {}).get("data") or []
            org_id = org_rel[0].get("id", "") if org_rel else ""
            city, st = org_loc.get(org_id, ("", ""))

            blob = " ".join([clean(at.get("descriptionText")),
                             clean(at.get("specialNeedsDetails"))])
            pts, hits = score(blob)
            if not hits:
                continue
            for _, phrase in hits:
                phrase_by_org[phrase][org_id] += 1
            if (at.get("breedPrimary") or "").lower() in MILL_BREEDS:
                pts += 1

            rows.append({
                "id": rec.get("id"), "name": at.get("name"),
                "breed": at.get("breedPrimary"), "age": at.get("ageGroup"),
                "sex": at.get("sex"), "org": org_name.get(org_id, ""),
                "org_id": org_id, "city": city, "state": st,
                "score": pts,
                "matched": "; ".join(sorted({p for _, p in hits})),
                "housetrained": at.get("isHousetrained"),
                "qualities": ", ".join(at.get("qualities") or []),
                "url": f"https://rescuegroups.org/?ANIMALID={rec.get('id')}",
            })

        meta = payload.get("meta") or {}
        total_pages = int(meta.get("pages") or 1)
        print(f"page {page}/{total_pages}: {in_scope} dogs in scope, "
              f"{len(rows)} with a signal")
        if page >= total_pages:
            break
        page += 1
        if args.max_pages and page > args.max_pages:
            print(f"  stopping at --max-pages {args.max_pages} "
                  f"(of {total_pages} available)")
            break
        time.sleep(0.4)

    # Boilerplate demotion: if one org uses a phrase across most of its own
    # listings, that is its mission statement, not a fact about the dog.
    org_counts = Counter(r["org_id"] for r in rows)
    boiler = {(p, o) for p, per in phrase_by_org.items()
              for o, n in per.items()
              if n >= 5 and n >= 0.8 * max(org_counts[o], 1)}
    for r in rows:
        drop = [p for p in r["matched"].split("; ") if (p, r["org_id"]) in boiler]
        if drop:
            r["score"] -= sum(w for w, rx in COMPILED
                              if any(rx.fullmatch(p) for p in drop))
            r["matched"] += f"  [boilerplate: {', '.join(drop)}]"

    keep = sorted([r for r in rows if r["score"] >= args.min_score],
                  key=lambda r: -r["score"])

    if rows:
        with open(args.out, "w", newline="", encoding="utf-8") as f:
            w = csv.DictWriter(f, fieldnames=list(rows[0].keys()))
            w.writeheader()
            w.writerows(keep)

    scope = f"{args.miles}mi of {args.postal}" if args.postal else "national"
    print(f"\nscope:            {scope}")
    print(f"dogs in scope:    {in_scope}")
    print(f"any signal:       {len(rows)}")
    print(f"score >= {args.min_score}:      {len(keep)}  ->  {args.out}")
    if boiler:
        print(f"boilerplate demoted: {len(boiler)} org/phrase pairs")
    if keep:
        print("\nstates:", ", ".join(f"{s} {n}" for s, n in
                                     Counter(r["state"] for r in keep).most_common(10)))
        print("\ntop matches:")
        for r in keep[:10]:
            print(f"  {r['score']:>3}  {r['name']:<18} {r['org']:<32} "
                  f"{r['state']}  {r['matched'][:60]}")


if __name__ == "__main__":
    main()
