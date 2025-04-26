# ✅ Corrected fetch_volume.py

import requests

def fetch_volume(keywords: list[str]) -> dict[str, dict]:
    from src.config import secret  # Adjust if your secret fetching is different

    DFS_USER = secret("DFS_USER")
    DFS_PASS = secret("DFS_PASS")

    auth = (DFS_USER, DFS_PASS)
    url = "https://api.dataforseo.com/v3/keywords_data/google_ads/search_volume/live"

    payload = {
        "keywords": keywords,
        "language_code": "en",
        "location_code": 1023191,  # Bengaluru, India
    }

    try:
        response = requests.post(url, json=[payload], auth=auth)
        if response.status_code != 200:
            print(f"DataForSEO API error: {response.text}")
            return {}

        data = response.json()
        enriched = {}

        tasks = data.get("tasks", [])
        if tasks and "result" in tasks[0]:
            results = tasks[0]["result"]
            for r in results:
                keyword = r.get("keyword", "")
                enriched[keyword] = {
                    "search_volume": r.get("search_volume", 0),
                    "competition": r.get("competition", 0.0),
                    "cpc": (r.get("cpc", 0.0) / 100.0) if r.get("cpc") else 0.0,
                }

        return enriched
    except Exception as e:
        print(f"Error fetching volume from DataForSEO: {e}")
        return {}
