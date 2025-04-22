import requests, json, time
from typing import List, Dict
from src.config import secret

APIFY_TOKEN = secret("APIFY_TOKEN")
# Update this to your actual Apify task ID
TASK_ID = "avadhut.sawant~google-maps-scraper-task"  # Or your new task ID

def run_scrape(brand: str, city: str) -> List[Dict]:
    """Run the Google‑Maps actor Task and return list of place dicts."""
    print(f"Starting Apify scrape for {brand} in {city}...")
    print(f"Using Apify token: {APIFY_TOKEN[:5]}***")
    print(f"Using Task ID: {TASK_ID}")
    
    try:
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
        
        print(f"Apify response status code: {resp.status_code}")
        
        # Check if the request was successful
        if resp.status_code == 200:
            data = resp.json()
            print(f"Apify request successful, received {len(data) if isinstance(data, list) else 'non-list'} items")
            
            # Save a sample of the response for debugging
            if isinstance(data, list) and data:
                first_item = data[0]
                print(f"Available fields in first item: {list(first_item.keys())}")
                print(f"First item sample: {json.dumps(first_item)[0:300]}...")
                return data
            else:
                print(f"Unexpected data format: {data}")
                return []
        else:
            print(f"Apify request failed with status code: {resp.status_code}")
            print(f"Response content: {resp.text}")
            return []
    
    except Exception as e:
        print(f"Error in run_scrape: {str(e)}")
        import traceback
        traceback.print_exc()
        return []
