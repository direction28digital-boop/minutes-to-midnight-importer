import os
import re
from pathlib import Path
import json
import time
import requests
from requests.exceptions import ConnectionError as RequestsConnectionError, ReadTimeout as RequestsReadTimeout
from json import JSONDecodeError as StdJSONDecodeError

API_KEY = os.environ["RG_API_KEY"]
WP_BASE_URL = os.environ["WP_BASE_URL"]
WP_USER = os.environ["WP_USER"]
WP_PASSWORD = os.environ["WP_PASSWORD"]

BASE_URL = "https://api.rescuegroups.org/v5/public/animals/search/available/dogs"
DATA_DIR = Path("data")
DATA_DIR.mkdir(exist_ok=True)


def slugify(value: str) -> str:
    """Simple slug helper for org and dog names."""
    value = value.lower()
    value = re.sub(r"[^a-z0-9]+", "-", value)
    return value.strip("-") or "item"


def fetch_page(page: int, limit: int = 250) -> dict:
    # RG docs: Content-Type is required header
    headers = {
        "Content-Type": "application/vnd.api+json",
        "Authorization": API_KEY,
    }

    params = {
        "page": page,
        "limit": limit,
        # we need orgs for location; pictures will come from animal attributes
        "include": "orgs",
    }

    response = requests.get(BASE_URL, headers=headers, params=params, timeout=60)

    try:
        response.raise_for_status()
    except requests.HTTPError as e:
        print("RG HTTP error:", e)
        print("Status code:", response.status_code)
        print("Headers:", response.headers)
        print("Body snippet:", response.text[:500])
        raise

    if not response.text.strip():
        print(f"RG returned empty body for page {page} (status 200).")
        return {"data": [], "included": [], "meta": {}}

    try:
        return response.json()
    except StdJSONDecodeError:
        print("Failed to decode JSON from RescueGroups")
        print("Status code:", response.status_code)
        print("Headers:", response.headers)
        print("Body length:", len(response.text))
        print("Raw body repr:", repr(response.text[:200]))
        raise


def map_country_to_code(country: str) -> str:
    if not country:
        return ""
    c = country.lower().strip()
    if "united states" in c or c == "us":
        return "US"
    if "canada" in c or c == "ca":
        return "CA"
    return country[:2].upper()


def normalize_animals(page_data: dict) -> list[dict]:
    animals = page_data.get("data", [])
    included = page_data.get("included", [])

    orgs_by_id: dict[str, dict] = {}

    # Build org lookup so we can attach city/state to each dog
    for inc in included:
        if inc.get("type") == "orgs":
            orgs_by_id[inc["id"]] = inc

    normalized: list[dict] = []

    for animal in animals:
        attrs = animal.get("attributes", {}) or {}
        rels = animal.get("relationships", {}) or {}

        org_id = None
        if "orgs" in rels:
            org_data = rels["orgs"].get("data")
            if isinstance(org_data, list) and org_data:
                org_id = org_data[0].get("id")

        org_attrs: dict = {}
        if org_id and org_id in orgs_by_id:
            org_attrs = orgs_by_id[org_id].get("attributes", {}) or {}

        city = org_attrs.get("city") or ""
        region = org_attrs.get("state") or ""
        postal = org_attrs.get("postalcode") or ""
        country = map_country_to_code(org_attrs.get("country") or "")

        status = "Available"

        age = attrs.get("ageGroup") or attrs.get("ageString") or ""
        sex_raw = (attrs.get("sex") or "").lower()
        if sex_raw == "male":
            sex = "Male"
        elif sex_raw == "female":
            sex = "Female"
        else:
            sex = "Unknown"

        size_raw = (attrs.get("sizeGroup") or "").lower()
        if size_raw in ("small", "medium", "large", "x-large", "xl"):
            if size_raw == "x-large":
                size = "XL"
            else:
                size = size_raw.capitalize()
        else:
            size = "Unknown"

        primary_breed = attrs.get("breedPrimary") or ""
        secondary_breed = attrs.get("breedSecondary") or ""
        mixed = bool(attrs.get("isBreedMixed"))

        m2m_id = f"rg-{org_id}-{animal['id']}"

        # Build a stable image key based on org + RG animal ID + dog name
        org_name = org_attrs.get("name") or f"org-{org_id or 'unknown'}"
        org_slug = slugify(org_name)
        dog_name = attrs.get("name") or f"dog-{animal['id']}"
        dog_slug = slugify(dog_name)
        image_key = f"{org_slug}/{animal['id']}-{dog_slug}.jpg"

        # Simple, reliable photo: use the thumbnail URL from attributes
        photos: list[dict] = []
        thumb_url = attrs.get("pictureThumbnailUrl")
        if isinstance(thumb_url, str) and thumb_url.startswith("http"):
            photos.append(
                {
                    "url": thumb_url,
                    "width": None,
                    "height": None,
                    "isPrimary": True,
                }
            )

        now_epoch = int(time.time())

        normalized.append(
            {
                "m2mId": m2m_id,
                "source": "RescueGroups",
                "sourceAnimalId": str(animal["id"]),
                "orgId": org_id,
                "name": attrs.get("name") or "",
                "status": status,
                "species": "Dog",
                "primaryBreed": primary_breed,
                "secondaryBreed": secondary_breed,
                "mixed": mixed,
                "age": age,
                "sex": sex,
                "size": size,
                "location": {
                    "city": city,
                    "regionCode": region,
                    "countryCode": country,
                    "postalCode": postal,
                },
                "summary": "",
                "descriptionPlain": attrs.get("descriptionText") or "",
                "descriptionHtml": attrs.get("descriptionHtml") or "",
                "compatibility": {
                    "goodWithDogs": attrs.get("isDogsOk") or "",
                    "goodWithCats": attrs.get("isCatsOk") or "",
                    "goodWithKids": attrs.get("isKidsOk") or "",
                },
                # now populated with at most one RG thumbnail URL
                "photos": photos,
                "org": {
                    "name": org_attrs.get("name") or "",
                    "city": city,
                    "regionCode": region,
                    "countryCode": country,
                    "websiteUrl": org_attrs.get("url") or "",
                },
                "petProfileUrl": attrs.get("url") or "",
                "imageKey": image_key,
                "lastUpdatedEpoch": now_epoch,
                # NEW: when this dog was last seen in a daily RG snapshot
                "lastSeenEpoch": now_epoch,
            }
        )

    return normalized

def push_animals_to_wp(normalized_path: Path, max_count: int | None = None) -> None:
    base_url = f"{WP_BASE_URL.rstrip('/')}/wp-json/wp/v2/m2mr_animal"
    session = requests.Session()

    def safe_post(url: str, payload: dict) -> tuple[bool, requests.Response | None]:
        """POST with simple retry on connection errors."""
        for attempt in range(3):
            try:
                resp = session.post(
                    url,
                    json=payload,
                    auth=(WP_USER, WP_PASSWORD),
                    timeout=60,
                )
                return True, resp
            except (RequestsConnectionError, RequestsReadTimeout) as e:
                print(f"POST error talking to {url} (attempt {attempt + 1}/3): {e}")
                time.sleep(2 * (attempt + 1))  # backoff: 2s, 4s, 6s
        return False, None

    def safe_get(url: str, params: dict) -> tuple[bool, requests.Response | None]:
        """GET with simple retry on connection errors/timeouts."""
        for attempt in range(3):
            try:
                resp = session.get(
                    url,
                    params=params,
                    auth=(WP_USER, WP_PASSWORD),
                    timeout=60,
                )
                return True, resp
            except (RequestsConnectionError, RequestsReadTimeout) as e:
                print(f"GET error talking to {url} (attempt {attempt + 1}/3): {e}")
                time.sleep(2 * (attempt + 1))
        return False, None

    def draft_missing_published_animals(seen_slugs: set[str]) -> None:
        """Draft published RG animals in WP that were not seen in the current snapshot."""
        drafted = 0
        page = 1

        while True:
            params = {
                "per_page": 100,
                "page": page,
                "status": "publish",
                "context": "edit",
            }

            ok_get, resp = safe_get(base_url, params)
            if not ok_get or resp is None or not resp.ok:
                status = resp.status_code if resp is not None else "no-response"
                text = resp.text if resp is not None else ""
                print("Failed to list WP animals for cleanup", status, text)
                break

            posts = resp.json()
            if not isinstance(posts, list) or not posts:
                break

            for post in posts:
                post_id = post.get("id")
                slug = (post.get("slug") or "").lower().strip()

                if not post_id or not slug:
                    continue

                # Only manage RescueGroups records created by this pipeline
                if not slug.startswith("rg-"):
                    continue

                if slug in seen_slugs:
                    continue

                ok_post, upd = safe_post(f"{base_url}/{post_id}", {"status": "draft"})
                if not ok_post or upd is None or not upd.ok:
                    status = upd.status_code if upd is not None else "no-response"
                    text = upd.text if upd is not None else ""
                    print("Failed to draft missing animal", post_id, slug, status, text)
                    continue

                drafted += 1
                print(f"Drafted missing animal post {post_id} slug={slug}")

            page += 1

        print(f"Drafted {drafted} missing animals in WordPress")

    seen_slugs: set[str] = set()
    count = 0

    with normalized_path.open("r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue

            data = json.loads(line)
            m2m_id = data.get("m2mId")
            title = data.get("name") or m2m_id or "RescueGroups Animal"
            slug = (m2m_id or "").lower().strip()

            if slug:
                seen_slugs.add(slug)

            payload = {
                "title": title,
                "status": "publish",
                "content": json.dumps(data, ensure_ascii=False),
            }

            # IMPORTANT: allow finding existing posts even if they were drafted previously
            params = {"slug": slug, "status": "any"} if slug else {"status": "any"}

            ok_get, get_resp = safe_get(base_url, params)
            if not ok_get or get_resp is None or not get_resp.ok:
                status = get_resp.status_code if get_resp is not None else "no-response"
                text = get_resp.text if get_resp is not None else ""
                print("Failed to lookup animal", m2m_id, status, text)
                continue

            existing = get_resp.json()
            if isinstance(existing, list) and existing:
                post_id = existing[0].get("id")
                update_url = f"{base_url}/{post_id}"
                ok_post, resp = safe_post(update_url, payload)
                action = "Updated"
            else:
                create_payload = {**payload, "slug": slug} if slug else payload
                ok_post, resp = safe_post(base_url, create_payload)
                action = "Created"

            if not ok_post or resp is None or not resp.ok:
                status = resp.status_code if resp is not None else "no-response"
                text = resp.text if resp is not None else ""
                print("Failed to upsert animal", m2m_id, status, text)
            else:
                created = resp.json()
                print(f"{action} animal post", created.get("id"), "for", m2m_id)

            count += 1
            if max_count is not None and count >= max_count:
                break

    print(f"Upserted {count} animals in WordPress")

    # Cleanup: only run when we processed the full file (avoid accidental drafts during testing)
    if max_count is None:
        draft_missing_published_animals(seen_slugs)
    else:
        print("Skipping cleanup because max_count was set (test run).")

    base_url = f"{WP_BASE_URL.rstrip('/')}/wp-json/wp/v2/m2mr_animal"
    session = requests.Session()

    def safe_post(url: str, payload: dict) -> tuple[bool, requests.Response | None]:
        """POST with simple retry on connection errors."""
        for attempt in range(3):
            try:
                resp = session.post(
                    url,
                    json=payload,
                    auth=(WP_USER, WP_PASSWORD),
                    timeout=60,
                )
                return True, resp
            except (RequestsConnectionError, RequestsReadTimeout) as e:
                print(f"POST error talking to {url} (attempt {attempt + 1}/3): {e}")
                time.sleep(2 * (attempt + 1))  # backoff: 2s, 4s, 6s
        return False, None

    def safe_get(url: str, params: dict) -> tuple[bool, requests.Response | None]:
        """GET with simple retry on connection errors/timeouts."""
        for attempt in range(3):
            try:
                resp = session.get(
                    url,
                    params=params,
                    auth=(WP_USER, WP_PASSWORD),
                    timeout=60,
                )
                return True, resp
            except (RequestsConnectionError, RequestsReadTimeout) as e:
                print(f"GET error talking to {url} (attempt {attempt + 1}/3): {e}")
                time.sleep(2 * (attempt + 1))
        return False, None

    count = 0
    with normalized_path.open("r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            data = json.loads(line)
            m2m_id = data.get("m2mId")
            title = data.get("name") or m2m_id or "RescueGroups Animal"
            slug = (m2m_id or "").lower()

            payload = {
                "title": title,
                "status": "publish",
                "content": json.dumps(data, ensure_ascii=False),
            }

            params = {"slug": slug, "status": "any"} if slug else {"status": "any"}
            ok_get, get_resp = safe_get(base_url, params)
            if not ok_get or get_resp is None or not get_resp.ok:
                status = get_resp.status_code if get_resp is not None else "no-response"
                text = get_resp.text if get_resp is not None else ""
                print("Failed to lookup animal", m2m_id, status, text)
                continue

            existing = get_resp.json()
            if isinstance(existing, list) and existing:
                post_id = existing[0].get("id")
                update_url = f"{base_url}/{post_id}"
                ok_post, resp = safe_post(update_url, payload)
                action = "Updated"
            else:
                create_payload = {**payload, "slug": slug} if slug else payload
                ok_post, resp = safe_post(base_url, create_payload)
                action = "Created"

            if not ok_post or resp is None or not resp.ok:
                status = resp.status_code if resp is not None else "no-response"
                text = resp.text if resp is not None else ""
                print("Failed to upsert animal", m2m_id, status, text)
            else:
                created = resp.json()
                print(f"{action} animal post", created.get("id"), "for", m2m_id)

            count += 1
            if max_count is not None and count >= max_count:
                break

    print(f"Upserted {count} animals in WordPress")

def main():
    output_path = DATA_DIR / "animals.normalized.jsonl"
    with output_path.open("w", encoding="utf-8") as out:
        page = 1
        while True:
            page_data = fetch_page(page)
            normalized = normalize_animals(page_data)
            if not normalized:
                break
            for item in normalized:
                out.write(json.dumps(item, ensure_ascii=False) + "\n")

            meta = page_data.get("meta", {})
            total_pages = int(meta.get("pages") or 0)
            if total_pages and page >= total_pages:
                break
            page += 1

    print(f"Normalized animals written to {output_path}")

    push_animals_to_wp(output_path)


if __name__ == "__main__":
    main()
