import json, urllib.request

URL = ""
KEY = ""

headers = {
    "apikey": KEY,
    "Authorization": f"Bearer {KEY}",
    "Content-Type": "application/json",
    "Prefer": "resolution=merge-duplicates"
}

with open("data/gods_metadata.json") as f:
    meta = json.load(f)

with open("data/council_ratings.json") as f:
    ratings = json.load(f)

ratings_lookup = {r["God"]: r for r in ratings}

meta_records = [
    {
        "god_name":    r["God"],
        "title":       r.get("Title"),
        "pantheon":    r.get("Pantheon"),
        "role":        r.get("Role"),
        "class":       r.get("Class"),
        "attack_type": r.get("Attack Type"),
        "damage_type": r.get("Damage Type"),
        "tier":        r.get("Tier", "U"),
        "rank":        r.get("Rank", 999),
        "movement":    r.get("Movement", 0),
    }
    for r in meta
]

req = urllib.request.Request(
    f"{URL}/rest/v1/gods_metadata",
    data=json.dumps(meta_records).encode(),
    headers=headers, method="POST"
)
with urllib.request.urlopen(req) as res:
    print(f"gods_metadata: {res.status}")

PLAYERS = ["Joey", "Darian", "Jami", "Jamie", "Mike"]
ratings_records = []
for r in meta:
    god = r["God"]
    row = ratings_lookup.get(god, {})
    rec = {"god_name": god}
    for p in PLAYERS:
        val = row.get(p)
        rec[p.lower()] = int(val) if val and val != 0 else None
    ratings_records.append(rec)

req = urllib.request.Request(
    f"{URL}/rest/v1/council_ratings",
    data=json.dumps(ratings_records).encode(),
    headers=headers, method="POST"
)
with urllib.request.urlopen(req) as res:
    print(f"council_ratings: {res.status}")

print("Done!")