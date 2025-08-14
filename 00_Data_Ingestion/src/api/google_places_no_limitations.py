import requests
import json
import utils
import os

# Grid of (latitude, longitude) points across Barcelona
grid_centers = [
    (41.425, 2.115),
    (41.425, 2.145),
    (41.425, 2.175),
    (41.425, 2.205),
    (41.425, 2.235),
    (41.405, 2.115),
    (41.405, 2.145),
    (41.405, 2.175),
    (41.405, 2.205),
    (41.405, 2.235),
    (41.385, 2.115),
    (41.385, 2.145),
    (41.385, 2.175),
    (41.385, 2.205),
    (41.385, 2.235),
    (41.365, 2.115),
    (41.365, 2.145),
    (41.365, 2.175),
    (41.365, 2.205),
    (41.365, 2.235),
    (41.345, 2.115),
    (41.345, 2.145),
    (41.345, 2.175),
    (41.345, 2.205),
    (41.345, 2.235),
]

radius = 3000  # Radius in meters (3km)
query = "search"
all_places = {}
GOOGLE_PLACES_API_KEY = os.getenv("GOOGLE_PLACES_API_KEY")
# api_key = utils.get_google_places_api_key()
url = "https://places.googleapis.com/v1/places:searchText"
headers = {
    "Content-Type": "application/json",
    "X-Goog-Api-Key": GOOGLE_PLACES_API_KEY,
    "X-Goog-FieldMask": "*",
}

for lat, lon in grid_centers:
    print(f"\n🔍 Searching in area centered at ({lat}, {lon})...")

    body = {
        "textQuery": query,
        "locationBias": {
            "circle": {"center": {"latitude": lat, "longitude": lon}, "radius": radius}
        },
    }

    try:
        response = requests.post(url, headers=headers, json=body)
        response.raise_for_status()
        data = response.json()

        for place in data.get("places", []):
            all_places[place["id"]] = place

    except requests.exceptions.HTTPError as err:
        print(f"❌ HTTP error for ({lat}, {lon}): {err}")
        print(f"📦 Full response: {err.response.text}")

# Save unique results to file
with open("search_name.json", "w", encoding="utf-8") as f:
    json.dump({"places": list(all_places.values())}, f, ensure_ascii=False, indent=2)

print(f"\n✅ Completed. Total unique places found: {len(all_places)}")
