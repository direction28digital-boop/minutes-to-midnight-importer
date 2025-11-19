import json
from pathlib import Path

data = json.loads(Path("animals_page1.json").read_text(encoding="utf-8"))

print("Top level keys:", data.keys())
print("Meta:", data.get("meta"))

animals = data.get("data", [])
print("Number of animals:", len(animals))

if animals:
    first = animals[0]
    print("\nFirst animal keys:", first.keys())
    attrs = first.get("attributes", {})
    print("Attributes keys:", list(attrs.keys()))
    rels = first.get("relationships", {})
    print("Relationships keys:", list(rels.keys()))

included = data.get("included", [])
types = {}
for inc in included:
    t = inc.get("type")
    types.setdefault(t, 0)
    types[t] += 1
print("\nIncluded types and counts:", types)

for inc in included:
    if inc.get("type") == "orgs":
        print("\nOrg sample attributes:", inc.get("attributes", {}))
        break

for inc in included:
    if inc.get("type") == "pictures":
        print("\nPicture sample attributes:", inc.get("attributes", {}))
        break