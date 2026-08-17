#!/usr/bin/env python3
"""
Pull rescue/shelter organizations from the RescueGroups v5 API for every
SnoutHub hub city (radius search around a central postal code per hub).

Run from ~/Projects/m2m-rescuegroups-http:
    python3 rg_orgs_by_hub.py

Reads RG_API_KEY from the environment, falling back to .env.local.
Writes:
    data/orgs_by_hub.json   (full records, deduped, with hub tags)
    data/orgs_by_hub.csv    (flat file for outreach / ranking)
"""

import csv
import json
import os
import re
import time
from pathlib import Path

import requests

# ---------------------------------------------------------------- config

# hub-slug -> (postal code near city center, radius miles)
HUBS = {
    "atlanta": ("30303", 40),
    "austin": ("78701", 40),
    "boston-providence": ("02108", 60),
    "bridgeport": ("06604", 30),
    "buffalo": ("14202", 40),
    "calgary": ("T2P 1J9", 40),
    "charlotte": ("28202", 40),
    "chicago": ("60601", 40),
    "cincinnati": ("45202", 40),
    "cleveland": ("44113", 40),
    "columbus": ("43215", 40),
    "dallas-fort-worth": ("75201", 60),
    "denver": ("80202", 40),
    "detroit": ("48226", 40),
    "edmonton": ("T5J 0N3", 40),
    "grand-canyon": ("86023", 80),
    "green-bay": ("54301", 40),
    "houston": ("77002", 40),
    "indianapolis": ("46204", 40),
    "jacksonville": ("32202", 40),
    "kansas-city": ("64106", 40),
    "lake-havasu-city": ("86403", 50),
    "las-vegas": ("89101", 40),
    "los-angeles": ("90012", 40),
    "memphis": ("38103", 40),
    "miami-fort-lauderdale": ("33128", 50),
    "palm-springs": ("92262", 40),
    "phoenix": ("85004", 40),
    "sacramento": ("95814", 40),
    "san-diego": ("92101", 40),
    "san-francisco": ("94102", 40),
    "santa-cruz": ("95060", 30),
    "savannah": ("31401", 40),
    "st-louis": ("63101", 40),
    "tucson": ("85701", 40),
    "vancouver-ca": ("V6B 1A1", 40),
}

ORGS_URL = "https://api.rescuegroups.org/v5/public/orgs/search"
DATA_DIR = Path("data")
DATA_DIR.mkdir(exist_ok=True)


def load_api_key() -> str:
    key = os.environ.get("RG_API_KEY")
    if key:
        return key
    env_file = Path(".env.local")
    if env_file.exists():
        m = re.search(r'RG_API_KEY="?([^"\n]+)"?', env_file.read_text())
        if m:
            return m.group(1)
    raise SystemExit("RG_API_KEY not found in env or .env.local")


API_KEY = load_api_key()
HEADERS = {"Content-Type": "application/vnd.api+json", "Authorization": API_KEY}


def fetch_orgs_for_hub(postal: str, miles: int) -> list[dict]:
    """Radius search for orgs around a postal code, all pages."""
    orgs: list[dict] = []
    page = 1
    while True:
        body = {"data": {"filterRadius": {"miles": miles, "postalcode": postal}}}
        r = requests.post(
            f"{ORGS_URL}?limit=250&page={page}",
            headers=HEADERS,
            json=body,
            timeout=60,
        )
        r.raise_for_status()
        payload = r.json() if r.text.strip() else {}
        data = payload.get("data", [])
        orgs.extend(data)
        meta = payload.get("meta", {}) or {}
        total_pages = int(meta.get("pages") or 1)
        if page >= total_pages or not data:
            break
        page += 1
        time.sleep(0.4)
    return orgs


def main() -> None:
    by_id: dict[str, dict] = {}
    per_hub_counts: dict[str, int] = {}

    for hub, (postal, miles) in HUBS.items():
        try:
            raw = fetch_orgs_for_hub(postal, miles)
        except Exception as e:  # keep going; report at the end
            print(f"!! {hub}: FAILED — {e}")
            per_hub_counts[hub] = -1
            continue

        per_hub_counts[hub] = len(raw)
        for org in raw:
            oid = str(org.get("id"))
            attrs = org.get("attributes", {}) or {}
            if oid not in by_id:
                by_id[oid] = {"id": oid, "hubs": [], "attributes": attrs}
            if hub not in by_id[oid]["hubs"]:
                by_id[oid]["hubs"].append(hub)

        print(f"{hub}: {len(raw)} orgs")
        time.sleep(0.4)

    records = list(by_id.values())

    json_path = DATA_DIR / "orgs_by_hub.json"
    json_path.write_text(json.dumps(records, ensure_ascii=False, indent=1))

    # Flat CSV for outreach/ranking
    csv_path = DATA_DIR / "orgs_by_hub.csv"
    fields = [
        "org_id", "name", "type", "hubs", "city", "state", "postalcode",
        "country", "email", "phone", "url", "facebookUrl", "services",
        "about",
    ]
    with csv_path.open("w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fields)
        w.writeheader()
        for rec in records:
            a = rec["attributes"]
            services = a.get("services")
            if isinstance(services, list):
                services = "; ".join(str(s) for s in services)
            about = (a.get("about") or "")
            about = re.sub(r"\s+", " ", about)[:300]
            w.writerow({
                "org_id": rec["id"],
                "name": a.get("name") or "",
                "type": a.get("type") or "",
                "hubs": "; ".join(rec["hubs"]),
                "city": a.get("city") or "",
                "state": a.get("state") or "",
                "postalcode": a.get("postalcode") or "",
                "country": a.get("country") or "",
                "email": a.get("email") or "",
                "phone": a.get("phone") or "",
                "url": a.get("url") or "",
                "facebookUrl": a.get("facebookUrl") or "",
                "services": services or "",
                "about": about,
            })

    print("\n--- summary ---")
    for hub, n in per_hub_counts.items():
        print(f"{hub}: {'FAILED' if n < 0 else n}")
    print(f"\nUnique orgs: {len(records)}")
    print(f"Wrote {json_path} and {csv_path}")


if __name__ == "__main__":
    main()
