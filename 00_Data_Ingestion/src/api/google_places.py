import requests
import utils
import json
import os


def search_places(query_text, location_bias=None):
    """
    Sends a POST request to the Google Places API (New) using searchText.
    """
    GOOGLE_PLACES_API_KEY = os.getenv("GOOGLE_PLACES_API_KEY")
    # api_key = utils.get_google_places_api_key()
    url = "https://places.googleapis.com/v1/places:searchText"

    headers = {
        "Content-Type": "application/json",
        "X-Goog-Api-Key": GOOGLE_PLACES_API_KEY,
        "X-Goog-FieldMask": "*",  # Request all available fields
    }

    body = {"textQuery": query_text}

    if location_bias:
        body["locationBias"] = (
            location_bias  # Optional, e.g., 'circle:2000@41.38879,2.15899'
        )

    response = requests.post(url, headers=headers, json=body)
    response.raise_for_status()
    return response.json()


if __name__ == "__main__":
    try:
        # Search for CAP (health centres) in Barcelona
        results = search_places("CAP Barcelona")

        # Save results to a JSON file
        with open("cap_barcelona_results.json", "w", encoding="utf-8") as f:
            json.dump(results, f, ensure_ascii=False, indent=2)

        print("✅ Search completed and saved to cap_barcelona_results.json")

    except requests.exceptions.HTTPError as err:
        print(f"❌ HTTP error: {err}")
        print(f"📦 Full response: {err.response.text}")
