# events_fetch_and_normalize_serpapi.py

import os
from pathlib import Path
import json
import re
import time
from datetime import datetime
from typing import Any, Dict, TypedDict, List
from collections import defaultdict

import requests

SERPAPI_BASE_URL = "https://serpapi.com/search.json"


def _env_int(name: str, default: int) -> int:
    raw = os.environ.get(name)
    if raw is None:
        return default
    raw = str(raw).strip()
    if raw == "":
        return default
    try:
        return int(raw)
    except ValueError:
        return default


def _env_float(name: str, default: float) -> float:
    raw = os.environ.get(name)
    if raw is None:
        return default
    raw = str(raw).strip()
    if raw == "":
        return default
    try:
        return float(raw)
    except ValueError:
        return default


# Controls search spend + runtime
SERPAPI_MAX_PAGES = _env_int("SERPAPI_MAX_PAGES", 2)  # upper-bound searches per state/province
SERPAPI_THROTTLE_SECONDS = _env_float("SERPAPI_THROTTLE_SECONDS", 0.25)

# Optional batching across states/provinces (0 = process all)
SERPAPI_STATE_BATCH_SIZE = _env_int("SERPAPI_STATE_BATCH_SIZE", 0)
SERPAPI_STATE_BATCH_START = _env_int("SERPAPI_STATE_BATCH_START", 0)

# Optional cap on WP upserts per run (0 = unlimited)
WP_MAX_UPSERT_EVENTS = _env_int("WP_MAX_UPSERT_EVENTS", 0)

# Safely read SerpApi key
SERPAPI_API_KEY = os.environ.get("SERPAPI_API_KEY") or os.environ.get("SERPAPI_KEY")
if not SERPAPI_API_KEY:
    raise RuntimeError(
        "Missing SerpApi API key. Please set SERPAPI_API_KEY (or SERPAPI_KEY) in your environment."
    )

WP_BASE_URL = os.environ["WP_BASE_URL"]
WP_USER = os.environ["WP_USER"]
WP_PASSWORD = os.environ["WP_PASSWORD"]

DATA_DIR = Path("data_events")
DATA_DIR.mkdir(exist_ok=True)

US_CITIES_FILE = DATA_DIR / "cities.events.us.json"
CA_CITIES_FILE = DATA_DIR / "cities.events.ca.json"

# This is where normalized events will be written as JSONL
OUTPUT_EVENTS_PATH = DATA_DIR / "events.normalized.jsonl"


class Region(TypedDict):
    hubId: str
    city: str
    regionCode: str
    countryCode: str


def load_regions() -> List[Region]:
    regions: List[Region] = []

    for path in (US_CITIES_FILE, CA_CITIES_FILE):
        if not path.exists():
            continue
        with path.open("r", encoding="utf-8") as f:
            data = json.load(f)
        for item in data:
            regions.append(
                {
                    "hubId": str(item["hubId"]),
                    "city": str(item["city"]),
                    "regionCode": str(item["regionCode"]),
                    "countryCode": str(item["countryCode"]),
                }
            )

    return regions


def slugify(text: str) -> str:
    text = text.lower()
    text = re.sub(r"[^a-z0-9]+", "-", text)
    return text.strip("-")[:60]


# ---------------------------------------------------------------------------
# helpers to work at state or province level instead of per city
# ---------------------------------------------------------------------------

US_STATE_NAMES: Dict[str, str] = {
    "AL": "Alabama",
    "AK": "Alaska",
    "AZ": "Arizona",
    "AR": "Arkansas",
    "CA": "California",
    "CO": "Colorado",
    "CT": "Connecticut",
    "DE": "Delaware",
    "FL": "Florida",
    "GA": "Georgia",
    "HI": "Hawaii",
    "ID": "Idaho",
    "IL": "Illinois",
    "IN": "Indiana",
    "IA": "Iowa",
    "KS": "Kansas",
    "KY": "Kentucky",
    "LA": "Louisiana",
    "ME": "Maine",
    "MD": "Maryland",
    "MA": "Massachusetts",
    "MI": "Michigan",
    "MN": "Minnesota",
    "MS": "Mississippi",
    "MO": "Missouri",
    "MT": "Montana",
    "NE": "Nebraska",
    "NV": "Nevada",
    "NH": "New Hampshire",
    "NJ": "New Jersey",
    "NM": "New Mexico",
    "NY": "New York",
    "NC": "North Carolina",
    "ND": "North Dakota",
    "OH": "Ohio",
    "OK": "Oklahoma",
    "OR": "Oregon",
    "PA": "Pennsylvania",
    "RI": "Rhode Island",
    "SC": "South Carolina",
    "SD": "South Dakota",
    "TN": "Tennessee",
    "TX": "Texas",
    "UT": "Utah",
    "VT": "Vermont",
    "VA": "Virginia",
    "WA": "Washington",
    "WV": "West Virginia",
    "WI": "Wisconsin",
    "WY": "Wyoming",
    "DC": "District of Columbia",
}

CA_PROVINCE_NAMES: Dict[str, str] = {
    "AB": "Alberta",
    "BC": "British Columbia",
    "MB": "Manitoba",
    "NB": "New Brunswick",
    "NL": "Newfoundland and Labrador",
    "NS": "Nova Scotia",
    "NT": "Northwest Territories",
    "NU": "Nunavut",
    "ON": "Ontario",
    "PE": "Prince Edward Island",
    "QC": "Quebec",
    "SK": "Saskatchewan",
    "YT": "Yukon",
}


def state_location_label(region_code: str, country_code: str) -> str:
    region_code = (region_code or "").upper()
    country_code = (country_code or "").upper()

    if country_code == "US":
        name = US_STATE_NAMES.get(region_code, region_code)
        return f"{name},United States"

    if country_code == "CA":
        name = CA_PROVINCE_NAMES.get(region_code, region_code)
        return f"{name},Canada"

    return f"{region_code},{country_code}"


DOG_QUERY = 'dog OR dogs OR puppy OR puppies OR canine OR "dog friendly" OR "pet friendly" OR "dog events"'


def fetch_serpapi_events_for_state(
    region_code: str,
    country_code: str,
    api_key: str,
    max_pages: int = 2,
) -> List[Dict[str, Any]]:
    location = state_location_label(region_code, country_code)
    gl = "us" if country_code.upper() == "US" else "ca"

    print(
        f"Fetching events for state/province {region_code}, {country_code} "
        f"(location={location}, max_pages={max_pages})"
    )

    all_events: List[Dict[str, Any]] = []
    start = 0
    page = 1

    while page <= max_pages:
        params = {
            "engine": "google_events",
            "q": DOG_QUERY,
            "hl": "en",
            "gl": gl,
            "location": location,
            "start": start,
            "api_key": api_key,
        }

        try:
            response = requests.get(SERPAPI_BASE_URL, params=params, timeout=30)
        except requests.RequestException as e:
            print(f"Network error talking to SerpApi for {location}: {e}")
            break

        if response.status_code == 429:
            print("SerpApi returned 429 (out of searches or rate limited).")
            break

        if response.status_code == 401:
            print(
                "SerpApi returned 401 Invalid API key. "
                "Check SERPAPI_API_KEY or SERPAPI_KEY in your environment."
            )
            break

        if not response.ok:
            print(
                f"SerpApi returned HTTP {response.status_code} for {location}: "
                f"{response.text[:500]}"
            )
            break

        data = response.json()
        events = data.get("events_results", []) or []
        print(f"  Page {page}: got {len(events)} raw events")

        if not events:
            break

        all_events.extend(events)

        # google_events returns ~10 results per page. If we get fewer,
        # assume we are at the end.
        if len(events) < 10:
            break

        start += 10
        page += 1

        if SERPAPI_THROTTLE_SECONDS > 0:
            time.sleep(SERPAPI_THROTTLE_SECONDS)

    print(f"Total {len(all_events)} raw events for {location}")
    return all_events


POSITIVE_KEYWORDS = [
    "dog",
    "dogs",
    "puppy",
    "puppies",
    "canine",
    "pet",
    "pets",
    "adoption",
    "rescue",
    "humane",
    "clinic",
    "vaccine",
]

NEGATIVE_KEYWORDS = [
    "snoop dogg",
    "hot dog",
    "hotdog eating",
]


def is_dog_event(event: Dict[str, Any]) -> bool:
    text_parts = [event.get("title", ""), event.get("description", "")]
    text = " ".join(text_parts).lower()

    if any(bad in text for bad in NEGATIVE_KEYWORDS):
        return False

    if not any(word in text for word in POSITIVE_KEYWORDS):
        return False

    return True


# ---------------- Date parsing helpers for SerpApi google_events ----------------

MONTH_INDEX: Dict[str, int] = {
    "jan": 1,
    "feb": 2,
    "mar": 3,
    "apr": 4,
    "may": 5,
    "jun": 6,
    "jul": 7,
    "aug": 8,
    "sep": 9,
    "sept": 9,
    "oct": 10,
    "nov": 11,
    "dec": 12,
}

MAX_PAST_DAYS_NO_YEAR = 183  # about 6 months


def _parse_month_day_year_like(text: str, now: datetime | None = None) -> datetime | None:
    text = (text or "").strip()
    if not text:
        return None

    try:
        dt = datetime.fromisoformat(text)
        return dt
    except Exception:
        pass

    m = re.search(r"([A-Za-z]{3,9})\s+(\d{1,2})(?:,\s*(\d{4}))?", text)
    if not m:
        return None

    month_name, day_str, year_str = m.groups()
    month_key = month_name.lower()[:3]
    month = MONTH_INDEX.get(month_key)
    if month is None:
        return None

    try:
        day = int(day_str)
    except Exception:
        return None

    if now is None:
        now = datetime.utcnow()
    today = now.date()

    if year_str:
        year = int(year_str)
    else:
        year = today.year
        candidate_date = datetime(year, month, day).date()
        diff_days = (candidate_date - today).days
        if diff_days < -MAX_PAST_DAYS_NO_YEAR:
            year = today.year + 1

    try:
        return datetime(year, month, day, 12, 0, 0)
    except Exception:
        return None


def get_serpapi_start_end_iso(raw: Dict[str, Any]) -> tuple[str, str]:
    when = raw.get("date") or {}
    if not isinstance(when, dict):
        when = {}

    start_raw = (when.get("start_date") or when.get("start") or "").strip()
    end_raw = (when.get("end_date") or when.get("end") or "").strip()
    when_raw = (when.get("when") or "").strip()

    now = datetime.utcnow()

    start_source = start_raw or when_raw
    end_source = end_raw or ""

    start_dt = _parse_month_day_year_like(start_source, now=now)
    end_dt = _parse_month_day_year_like(end_source, now=now)

    start_iso = start_dt.isoformat() if start_dt else ""
    end_iso = end_dt.isoformat() if end_dt else ""

    return start_iso, end_iso


LOCATION_LINE_RE = re.compile(
    r"^(?P<city>.*?),\s*(?P<region>[A-Za-z]{2})(?:\s+(?P<zip>\d{5}))?"
)


def extract_location_from_event(raw: Dict[str, Any], fallback_region: Region | None) -> tuple[str, str, str, str, str]:
    address = raw.get("address") or ""
    address_line1 = ""

    if isinstance(address, list):
        if address:
            address_line1 = address[-1]
    elif isinstance(address, str):
        address_line1 = address
    else:
        address_line1 = ""

    city = ""
    region_code = ""
    postal_code = ""

    if address_line1:
        m = LOCATION_LINE_RE.search(address_line1)
        if m:
            city = (m.group("city") or "").strip()
            region_code = (m.group("region") or "").strip().upper()
            zip_code = m.group("zip")
            if zip_code:
                postal_code = zip_code.strip()

    if fallback_region:
        if not region_code:
            region_code = fallback_region["regionCode"]
        if not city:
            city = fallback_region["city"]
        country_code = fallback_region["countryCode"]
    else:
        country_code = "US"

    return city, region_code, country_code, address_line1, postal_code


def normalize_event(raw: Dict[str, Any], region: Region | None) -> Dict[str, Any]:
    title = raw.get("title") or ""
    description = raw.get("description") or ""
    venue = raw.get("venue", {}) or {}

    start_iso, end_iso = get_serpapi_start_end_iso(raw)
    city, region_code, country_code, address_line1, postal_code = extract_location_from_event(raw, region)

    link = raw.get("link") or raw.get("event_url") or ""

    slug = slugify(f"{city}-{start_iso}-{title}")
    m2m_id = f"serpapi-{slug}"

    normalized = {
        "m2mId": m2m_id,
        "source": "SerpApiGoogleEvents",
        "title": title,
        "startDateTime": start_iso,
        "endDateTime": end_iso,
        "city": city,
        "regionCode": region_code,
        "countryCode": country_code,
        "postalCode": postal_code,
        "venueName": venue if isinstance(venue, str) else venue.get("name") or "",
        "addressLine1": address_line1,
        "url": link,
        "sourceUrl": link,
        "description": description,
        "tags": [],
        "lastUpdatedEpoch": int(datetime.utcnow().timestamp()),
    }

    return normalized


def write_normalized_events(events: List[Dict[str, Any]]) -> Path:
    with OUTPUT_EVENTS_PATH.open("w", encoding="utf-8") as out:
        for ev in events:
            out.write(json.dumps(ev, ensure_ascii=False) + "\n")
    return OUTPUT_EVENTS_PATH


def is_future_event(raw: Dict[str, Any]) -> bool:
    start_iso, _ = get_serpapi_start_end_iso(raw)
    if not start_iso:
        return True

    try:
        dt = datetime.fromisoformat(start_iso)
    except Exception:
        return True

    today = datetime.utcnow().date()
    return dt.date() >= today


def upsert_events_to_wp(normalized_path: Path, max_count: int | None = None) -> None:
    base_url = f"{WP_BASE_URL.rstrip('/')}/wp-json/wp/v2/m2mr_event"
    session = requests.Session()

    count = 0
    with normalized_path.open("r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            data = json.loads(line)
            m2m_id = data.get("m2mId")
            title = data.get("title") or m2m_id or "Dog Event"
            slug = (m2m_id or "").lower()

            payload = {
                "title": title,
                "status": "publish",
                "content": json.dumps(data, ensure_ascii=False),
            }

            params = {"slug": slug, "status": "any"} if slug else {"status": "any"}

            get_resp = session.get(
                base_url,
                params=params,
                auth=(WP_USER, WP_PASSWORD),
                timeout=60,
            )
            if not get_resp.ok:
                print("Failed to lookup event", m2m_id, get_resp.status_code, get_resp.text)
                continue

            existing = get_resp.json()
            if isinstance(existing, list) and existing:
                post_id = existing[0].get("id")
                update_url = f"{base_url}/{post_id}"
                resp = session.post(
                    update_url,
                    json=payload,
                    auth=(WP_USER, WP_PASSWORD),
                    timeout=60,
                )
                action = "Updated"
            else:
                create_payload = {**payload, "slug": slug} if slug else payload
                resp = session.post(
                    base_url,
                    json=create_payload,
                    auth=(WP_USER, WP_PASSWORD),
                    timeout=60,
                )
                action = "Created"

            if not resp.ok:
                print("Failed to upsert event", m2m_id, resp.status_code, resp.text)
            else:
                created = resp.json()
                print(f"{action} event post", created.get("id"), "for", m2m_id)

            count += 1
            if max_count is not None and count >= max_count:
                break

    print(f"Upserted {count} events in WordPress")


def main() -> None:
    regions = load_regions()
    print(f"Loaded {len(regions)} regions")

    regions_by_state: Dict[tuple[str, str], List[Region]] = defaultdict(list)
    for region in regions:
        key = (region["regionCode"], region["countryCode"])
        regions_by_state[key].append(region)

    all_state_keys = sorted(regions_by_state.keys(), key=lambda k: (k[1], k[0]))
    print(f"Found {len(all_state_keys)} unique state/province keys")

    state_keys = all_state_keys
    if SERPAPI_STATE_BATCH_SIZE and len(all_state_keys) > 0:
        start = SERPAPI_STATE_BATCH_START % len(all_state_keys)
        rotated = all_state_keys[start:] + all_state_keys[:start]
        state_keys = rotated[:SERPAPI_STATE_BATCH_SIZE]

    print(
        f"Processing {len(state_keys)} state/province keys with SERPAPI_MAX_PAGES={SERPAPI_MAX_PAGES}. "
        f"Upper-bound searches this run: {len(state_keys) * max(SERPAPI_MAX_PAGES, 0)}"
    )

    all_normalized: List[Dict[str, Any]] = []

    for (region_code, country_code) in state_keys:
        region_group = regions_by_state[(region_code, country_code)]
        fallback_region = region_group[0]

        raw_events = fetch_serpapi_events_for_state(
            region_code=region_code,
            country_code=country_code,
            api_key=SERPAPI_API_KEY,
            max_pages=max(SERPAPI_MAX_PAGES, 0),
        )

        for ev in raw_events:
            if not is_dog_event(ev):
                continue
            if not is_future_event(ev):
                continue
            normalized = normalize_event(ev, fallback_region)
            all_normalized.append(normalized)

    if not all_normalized:
        print("No normalized events, nothing to write or upsert.")
        return

    # Dedupe by m2mId and sort by soonest startDateTime first
    dedup: Dict[str, Dict[str, Any]] = {}
    for ev in all_normalized:
        mid = str(ev.get("m2mId") or "")
        if not mid:
            continue
        if mid not in dedup:
            dedup[mid] = ev

    normalized_list = list(dedup.values())

    def _sort_key(ev: Dict[str, Any]) -> str:
        s = str(ev.get("startDateTime") or "")
        return s if s else "9999-12-31T23:59:59"

    normalized_list.sort(key=_sort_key)

    if WP_MAX_UPSERT_EVENTS and WP_MAX_UPSERT_EVENTS > 0:
        normalized_list = normalized_list[:WP_MAX_UPSERT_EVENTS]
        print(f"WP_MAX_UPSERT_EVENTS={WP_MAX_UPSERT_EVENTS} -> upserting {len(normalized_list)} events")

    output_path = write_normalized_events(normalized_list)
    print(f"Normalized events written to {output_path}")

    upsert_events_to_wp(output_path, max_count=None)


if __name__ == "__main__":
    main()
