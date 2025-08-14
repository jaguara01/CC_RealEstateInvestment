import json

with open("search_name.json", "r", encoding="utf-8") as f:
    data = json.load(f)

places = data.get("places", [])
print(f"Number of places found: {len(places)}")

# Extract and print display names (or fallback to 'name' if not available)
print(" Places found:")
for place in data.get("places", []):
    display_name = place.get("displayName", {}).get("text") or place.get("name")
    print("-", display_name)
