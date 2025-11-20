import os
from pathlib import Path
import json
import re
import requests
from datetime import datetime

SERPAPI_API_KEY = os.environ["SERPAPI_API_KEY"]
WP_BASE_URL = os.environ["WP_BASE_URL"]
WP_USER = os.environ["WP_USER"]
WP_PASSWORD = os.environ["WP_PASSWORD"]

DATA_DIR = Path("data_events")
DATA_DIR.mkdir(exist_ok=True)

REGIONS = [
    {"hubId": "las-vegas", "city": "Las Vegas", "regionCode": "NV", "countryCode": "US"},
    {"hubId": "phoenix", "city": "Phoenix", "regionCode": "AZ", "countryCode": "US"},
    {"hubId": "denver", "city": "Denver", "regionCode": "CO", "countryCode": "US"},
    # add more hubs as needed
]


def slugify(text: str) -> str:
    text = text.lower()
    text = re.sub(r"[^a-z0-9]+", "-", text)
    return text.strip("-")[:60]


def fetch_serpapi_events(city: str, region_code: str, country_code: str) -> list[dict]:
    params = {
        "engine": "google_events",
        "q": f"dog events in {city}, {region_code}",  # city + state in query
        "hl": "en",
        "gl": "us",
        "api_key": SERPAPI_KEY,
    }
    response = requests.get("https://serpapi.com/search", params=params, timeout=60)
    if not response.ok:
        print("SerpApi error", response.status_code, response.text)
        response.raise_for_status()
    data = response.json()
    return data.get("events_results", [])

POSITIVE_KEYWORDS = [
    "dog", "dogs", "puppy", "puppies", "canine",
    "pet", "pets", "adoption", "rescue", "humane", "clinic", "vaccine",
]

NEGATIVE_KEYWORDS = [
    "snoop dogg", "hot dog", "hotdog eating",
]


def is_dog_event(event: dict) -> bool:
    text_parts = [event.get("title", ""), event.get("description", "")]
    text = " ".join(text_parts).lower()

    if any(bad in text for bad in NEGATIVE_KEYWORDS):
        return False

    if not any(word in text for word in POSITIVE_KEYWORDS):
        return False

    return True


def normalize_event(raw: dict, region: dict) -> dict:
    title = raw.get("title") or ""
    description = raw.get("description") or ""
    venue = raw.get("venue", {}) or {}
    address = raw.get("address", "") or ""

    when = raw.get("date", {}) or {}
    # SerpApi uses 'start_date' / 'start_time' etc depending on engine version
    start = when.get("start_date") or when.get("start") or ""
    end = when.get("end_date") or when.get("end") or ""

    try:
        start_iso = datetime.fromisoformat(start).isoformat()
    except Exception:
        start_iso = start

    try:
        end_iso = datetime.fromisoformat(end).isoformat()
    except Exception:
        end_iso = end

    city = region["city"]
    region_code = region["regionCode"]
    country_code = region["countryCode"]

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
        "venueName": venue if isinstance(venue, str) else venue.get("name") or "",
        "addressLine1": address,
        "url": link,
        "sourceUrl": link,
        "description": description,
        "tags": [],
        "lastUpdatedEpoch": int(datetime.utcnow().timestamp()),
    }

    return normalized


def write_normalized_events(events: list[dict]) -> Path:
    output_path = DATA_DIR / "events.normalized.jsonl"
    with output_path.open("w", encoding="utf-8") as out:
        for ev in events:
            out.write(json.dumps(ev, ensure_ascii=False) + "\n")
    return output_path


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

            params = {"slug": slug} if slug else {}
            get_resp = session.get(base_url, params=params, auth=(WP_USER, WP_PASSWORD), timeout=60)
            if not get_resp.ok:
                print("Failed to lookup event", m2m_id, get_resp.status_code, get_resp.text)
                continue

            existing = get_resp.json()
            if isinstance(existing, list) and existing:
                post_id = existing[0].get("id")
                update_url = f"{base_url}/{post_id}"
                resp = session.post(update_url, json=payload, auth=(WP_USER, WP_PASSWORD), timeout=60)
                action = "Updated"
            else:
                create_payload = {**payload, "slug": slug} if slug else payload
                resp = session.post(base_url, json=create_payload, auth=(WP_USER, WP_PASSWORD), timeout=60)
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


def main():
    all_normalized: list[dict] = []

    for region in REGIONS:
        print(f"Fetching events for {region['city']}, {region['regionCode']}")
        raw_events = fetch_serpapi_events(region["city"], region["regionCode"], region["countryCode"])
        print(f"Got {len(raw_events)} raw events")

        for ev in raw_events:
            if not is_dog_event(ev):
                continue
            normalized = normalize_event(ev, region)
            all_normalized.append(normalized)

    output_path = write_normalized_events(all_normalized)
    print(f"Normalized events written to {output_path}")

    # First test only push first 10
    upsert_events_to_wp(output_path, max_count=10)


if __name__ == "__main__":
    main()