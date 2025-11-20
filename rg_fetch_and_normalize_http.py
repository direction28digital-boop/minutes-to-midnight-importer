import os
from pathlib import Path
import json
import requests

API_KEY = os.environ["Ymwe0yrg"]
WP_BASE_URL = os.environ["https://orange-pigeon-586276.hostingersite.com"]
WP_USER = os.environ["m2m_ingest"]
WP_PASSWORD = os.environ["Itcb4fungni!"]
BASE_URL = "https://api.rescuegroups.org/v5/public/animals/search/available/dogs"
DATA_DIR = Path("data")
DATA_DIR.mkdir(exist_ok=True)


def fetch_page(page: int, limit: int = 250) -> dict:
    headers = {
        "Content-Type": "application/vnd.api+json",
        "Authorization": API_KEY,
    }

    params = {
        "page": page,
        "limit": limit,
        "include": "orgs,locations,pictures",
    }

    response = requests.get(BASE_URL, headers=headers, params=params, timeout=60)
    response.raise_for_status()
    return response.json()


def map_country_to_code(country: str) -> str:
    if not country:
        return ""
    c = country.lower().strip()
    if "united states" in c or c == "us":
        return "US"
    if "canada" in c or c == "ca":
        return "CA"
    return country[:2].upper()


def normalize_animals(page_data: dict):
    animals = page_data.get("data", [])
    included = page_data.get("included", [])

    orgs_by_id = {}
    pictures_by_animal_id = {}

    for inc in included:
        inc_type = inc.get("type")
        if inc_type == "orgs":
            orgs_by_id[inc["id"]] = inc
        elif inc_type == "pictures":
            rels = inc.get("relationships", {})
            animal_rel = rels.get("animal", {}).get("data", {})
            animal_id = animal_rel.get("id")
            if not animal_id:
                continue
            pictures_by_animal_id.setdefault(animal_id, []).append(inc)

    normalized = []

    for animal in animals:
        attrs = animal.get("attributes", {})
        rels = animal.get("relationships", {})

        org_id = None
        if "orgs" in rels:
            org_data = rels["orgs"]["data"]
            if isinstance(org_data, list) and org_data:
                org_id = org_data[0]["id"]

        org_attrs = {}
        if org_id and org_id in orgs_by_id:
            org_attrs = orgs_by_id[org_id].get("attributes", {})

        city = org_attrs.get("city") or ""
        region = org_attrs.get("state") or ""
        postal = org_attrs.get("postalcode") or ""
        country = map_country_to_code(org_attrs.get("country") or "")

        photos = []
        for pic in pictures_by_animal_id.get(animal["id"], []):
            p_attrs = pic.get("attributes", {})
            original = p_attrs.get("original") or {}
            large = p_attrs.get("large") or {}
            small = p_attrs.get("small") or {}
            photos.append(
                {
                    "sourceUrl": original.get("url"),
                    "largeUrl": large.get("url"),
                    "thumbnailUrl": small.get("url"),
                    "mediaId": pic.get("id"),
                    "order": int(p_attrs.get("order") or 0),
                }
            )

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
                "photos": photos,
                "org": {
                    "name": org_attrs.get("name") or "",
                    "city": city,
                    "regionCode": region,
                    "countryCode": country,
                    "websiteUrl": org_attrs.get("url") or "",
                },
                "petProfileUrl": "",
                "lastUpdatedEpoch": 0,
            }
        )

    return normalized


def push_animals_to_wp(normalized_path: Path, max_count: int | None = None) -> None:
    base_url = f"{WP_BASE_URL.rstrip('/')}/wp-json/wp/v2/m2mr_animal"
    session = requests.Session()

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

            # upsert logic: check if a post with this slug already exists
            params = {"slug": slug} if slug else {}
            get_resp = session.get(base_url, params=params, auth=(WP_USER, WP_PASSWORD), timeout=60)
            if not get_resp.ok:
                print("Failed to lookup animal", m2m_id, get_resp.status_code, get_resp.text)
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
                print("Failed to upsert animal", m2m_id, resp.status_code, resp.text)
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