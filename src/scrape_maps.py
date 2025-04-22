import requests, json, time
from typing import List, Dict
from src.config import secret

APIFY_TOKEN = secret("APIFY_TOKEN")
TASK_ID     = "avadhut.zecode~google-maps-scraper-task"   # <- replace with your Task

def run_scrape(brand: str, city: str) -> List[Dict]:
    """Run the Google‑Maps actor Task and return list of place dicts."""
    url = f"https://api.apify.com/v2/actor-tasks/{TASK_ID}/run-sync-get-dataset-items"
    params = {"token": APIFY_TOKEN}
    payload = {
        "searchStringsArray": [brand],
        "locationQuery": city,
        "maxReviews": 0,
        "maxImages": 0,
        "maxItems": 20          # tweak for cost/speed
    }
    resp = requests.post(url, params=params, json=payload, timeout=300)
    resp.raise_for_status()
    return resp.json()
    
