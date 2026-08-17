#!/usr/bin/env python3
"""
rg_probe_filters.py — find out what the RescueGroups animals endpoint
actually honours, instead of assuming.

Why this exists: rg_mill_scan.py v2 reported "server-side text filter works,
matching records: 0". That conclusion was unsound. It treated HTTP 200 as
proof the filter was understood, but an unrecognised fieldName most likely
returns 200 with an empty result set. "No matches" and "filter ignored" look
identical from the outside.

This runs controls. If a filter for a word that appears in essentially every
dog description also returns 0, the filter is being ignored and any result
built on it is meaningless.

Run:
  cd ~/Projects/m2m-rescuegroups-http
  set -a; source .env.local; set +a
  ./.venv-scan/bin/python rg_probe_filters.py
"""

import json
import os
import re
import sys
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
    sys.exit("RG_API_KEY not found")


URL = "https://api.rescuegroups.org/v5/public/animals/search/available/dogs"
HEADERS = {"Content-Type": "application/vnd.api+json",
           "Authorization": load_api_key()}


def probe(label, body, show_meta=False):
    try:
        r = requests.post(f"{URL}?limit=5&page=1", headers=HEADERS,
                          json=body, timeout=60)
    except requests.RequestException as e:
        print(f"  {label:<52} NETWORK ERROR {e}")
        return None
    if r.status_code != 200:
        print(f"  {label:<52} HTTP {r.status_code}  {r.text[:120]}")
        return None
    payload = r.json() if r.text.strip() else {}
    meta = payload.get("meta") or {}
    n = meta.get("count")
    got = len(payload.get("data", []))
    print(f"  {label:<52} count={str(n):<8} returned={got}")
    if show_meta:
        print(f"      full meta: {json.dumps(meta)[:300]}")
    return n


def textfilter(field, criteria, op="contains"):
    return {"data": {"filters": [
        {"fieldName": field, "operation": op, "criteria": criteria}]}}


print("=" * 78)
print("1. BASELINE  (no filters at all)")
print("=" * 78)
base = probe("no body", {"data": {}}, show_meta=True)
if not base:
    sys.exit("\nBaseline returned nothing. Stop here, the endpoint or key is the problem.")

print()
print("=" * 78)
print("2. CONTROLS  (words that must appear in nearly every description)")
print("   If these come back 0, the text filter is being IGNORED.")
print("=" * 78)
for term in ["dog", "the", "she", "home"]:
    probe(f'descriptionText contains "{term}"',
          textfilter("animals.descriptionText", term))

print()
print("=" * 78)
print("3. FIELD NAME SPELLINGS  (which form does the API recognise?)")
print("=" * 78)
for field in ["animals.descriptionText", "descriptionText",
              "animals.description", "animals.descriptionHtml",
              "animals.searchString"]:
    probe(f'{field} contains "dog"', textfilter(field, "dog"))

print()
print("=" * 78)
print("4. THE ACTUAL QUESTION")
print("=" * 78)
for term in ["puppy mill", "puppymill", "mill survivor",
             "commercial breeder", "breeder release", "auction"]:
    probe(f'descriptionText contains "{term}"',
          textfilter("animals.descriptionText", term))

print()
print("=" * 78)
print("5. RADIUS  (proven on /orgs/search, never verified on /animals)")
print("=" * 78)
probe("filterRadius 150mi of 65065",
      {"data": {"filterRadius": {"miles": 150, "postalcode": "65065"}}})
probe("filterRadius 500mi of 65065",
      {"data": {"filterRadius": {"miles": 500, "postalcode": "65065"}}})
probe("filterRadius 25mi of 30303 (Atlanta, known org there)",
      {"data": {"filterRadius": {"miles": 25, "postalcode": "30303"}}})

print()
print("=" * 78)
print("6. A CONTROL ON A DIFFERENT FIELD  (is `filters` honoured at all?)")
print("=" * 78)
probe('animals.sex equals "Female"',
      {"data": {"filters": [{"fieldName": "animals.sex",
                             "operation": "equals", "criteria": "Female"}]}})
probe('animals.ageGroup equals "Adult"',
      {"data": {"filters": [{"fieldName": "animals.ageGroup",
                             "operation": "equals", "criteria": "Adult"}]}})

print()
print("=" * 78)
print("HOW TO READ THIS")
print("=" * 78)
print(f"""
 Baseline was {base}.

 If section 2 controls return roughly the baseline  -> text filtering works,
   and section 4 numbers are real answers to your question.
 If section 2 controls return 0                     -> `filters` is ignored on
   this endpoint. Every zero in section 4 is meaningless, and the only route
   left is downloading the feed and matching locally.
 If section 6 works but section 2 does not          -> `filters` is honoured,
   but not on free-text fields. Same conclusion, match locally.
 If section 5 returns 0 everywhere                  -> filterRadius does not
   apply to /animals, so geography also has to be done client-side.
""")
