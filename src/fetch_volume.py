import requests
from typing import List, Dict
from src.config import secret

DFS_USER = secret("DFS_USER")
DFS_PASS = secret("DFS_PASS")
URL      = "https://api.dataforseo.com/v3/dataforseo_labs/google/historical_search_volume/live"

def fetch_volume(keywords: List[str]) -> Dict:
    """Return DataForSEO result block (one request = one 'task')."""
    payload = [{
        "keywords": keywords,
        "location_code": 2840,   # India
        "language_code": "en"
    }]
    r = requests.post(URL, auth=(DFS_USER, DFS_PASS), json=payload, timeout=300)
    r.raise_for_status()
    return r.json()["tasks"][0]["result"]
