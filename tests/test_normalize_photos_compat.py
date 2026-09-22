"""normalize_animals: full-size photos + tri-state compatibility (2026-09-22)."""
import os

os.environ.setdefault("RG_API_KEY", "test")
os.environ.setdefault("WP_BASE_URL", "http://example.invalid")
os.environ.setdefault("WP_USER", "test")
os.environ.setdefault("WP_PASSWORD", "test")

import rg_fetch_and_normalize_http as rg  # noqa: E402


def pic(pid, order, w=843, h=1280):
    base = f"https://cdn.rescuegroups.org/1/pictures/animals/1/{pid}.jpg"
    return {
        "type": "pictures",
        "id": pid,
        "attributes": {
            "original": {"resolutionX": w, "resolutionY": h, "url": base},
            "large": {"resolutionX": 500, "resolutionY": 759, "url": base + "?width=500"},
            "order": order,
        },
    }


PAGE = {
    "data": [
        {
            "id": "10",
            "attributes": {
                "name": "Doli",
                "sex": "Female",
                "isDogsOk": False,
                "isCatsOk": None,
                "isKidsOk": True,
                "isHousetrained": True,
                "isAdoptionPending": False,
                "pictureCount": 3,
                "qualities": ["apartment", "timid"],
                "energyLevel": "Moderate",
                "vocalLevel": "Quiet",
                "newPeopleReaction": "Friendly",
                "killReason": "should never appear",
                "pictureThumbnailUrl": "https://cdn.rescuegroups.org/thumb.jpg",
            },
            "relationships": {
                "orgs": {"data": [{"type": "orgs", "id": "5"}]},
                "pictures": {"data": [
                    {"type": "pictures", "id": "p2"},
                    {"type": "pictures", "id": "p1"},
                    {"type": "pictures", "id": "missing"},
                ]},
            },
        }
    ],
    "included": [
        {"type": "orgs", "id": "5", "attributes": {"name": "Angels Rescue", "city": "Houston", "state": "TX", "country": "United States"}},
        pic("p1", 1),
        pic("p2", 2),
    ],
}


def test_compatibility_is_tri_state():
    dog = rg.normalize_animals(PAGE)[0]
    assert dog["compatibility"] == {"goodWithDogs": False, "goodWithCats": None, "goodWithKids": True}


def test_photos_full_sorted_and_full_size():
    dog = rg.normalize_animals(PAGE)[0]
    urls = [p["url"] for p in dog["photosFull"]]
    assert urls == [
        "https://cdn.rescuegroups.org/1/pictures/animals/1/p1.jpg",
        "https://cdn.rescuegroups.org/1/pictures/animals/1/p2.jpg",
    ]
    assert dog["photosFull"][0]["width"] == 843


def test_legacy_photos_unchanged():
    dog = rg.normalize_animals(PAGE)[0]
    assert dog["photos"] == [{"url": "https://cdn.rescuegroups.org/thumb.jpg", "width": None, "height": None, "isPrimary": True}]


def test_new_fields_and_no_kill_reason():
    dog = rg.normalize_animals(PAGE)[0]
    assert dog["qualities"] == ["apartment", "timid"]
    assert dog["isHousetrained"] is True and dog["isAdoptionPending"] is False
    assert dog["pictureCount"] == 3
    assert "killReason" not in str(dog)


def test_tri_state_strings():
    assert rg.tri_state("Yes") is True and rg.tri_state("no") is False
    assert rg.tri_state("") is None and rg.tri_state("maybe") is None
