import requests, json, time
from typing import List, Dict
from src.config import secret

APIFY_TOKEN = secret("APIFY_TOKEN")
# Make sure this is your correct task ID
TASK_ID = "avadhut.zecode~google-maps-scraper-task"  # Replace with your actual task ID

def run_scrape(brand: str, city: str) -> List[Dict]:
    """Run the Google‑Maps actor Task and return list of place dicts."""
    try:
        print(f"Starting Apify scrape for {brand} in {city}...")
        url = f"https://api.apify.com/v2/actor-tasks/{TASK_ID}/run-sync-get-dataset-items"
        params = {"token": APIFY_TOKEN}
        payload = {
            "searchStringsArray": [brand],
            "locationQuery": city,
            "maxReviews": 0,
            "maxImages": 0,
            "maxItems": 20
        }
        
        print(f"Sending request to Apify with payload: {json.dumps(payload)}")
        resp = requests.post(url, params=params, json=payload, timeout=300)
        
        # Check if the request was successful
        if resp.status_code == 200:
            data = resp.json()
            print(f"Apify request successful, received {len(data)} items")
            
            # Print a sample of the first item if available
            if data and len(data) > 0:
                print(f"First item sample: {json.dumps(data[0])[0:500]}...")
                
                # Check for expected fields
                if 'name' not in data[0] and 'title' not in data[0]:
                    print(f"WARNING: Neither 'name' nor 'title' field found in data. Available fields: {list(data[0].keys())}")
            else:
                print("No data items returned from Apify")
            
            return data
        else:
            print(f"Apify request failed with status code: {resp.status_code}")
            print(f"Response content: {resp.text}")
            return []
    
    except Exception as e:
        print(f"Error in run_scrape: {str(e)}")
        import traceback
        traceback.print_exc()
        return []
