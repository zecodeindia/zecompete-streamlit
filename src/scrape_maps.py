import requests, json, time, os
from typing import List, Dict
from src.config import secret

APIFY_TOKEN = secret("APIFY_TOKEN")
# Make sure this is your correct task ID
TASK_ID = "your-new-task-id"  # Replace with your actual task ID

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
            
            # Print the type of data received
            print(f"Data type: {type(data)}")
            
            # Save a sample of the response for debugging
            if isinstance(data, list) and data:
                print(f"First item sample: {json.dumps(data[0])[0:200]}...")
                
                # Check for expected fields
                first_item = data[0]
                print(f"Available fields in first item: {list(first_item.keys())}")
                
                # Save full response to a file for inspection
                debug_dir = os.path.join(os.path.dirname(__file__), '..', 'debug')
                os.makedirs(debug_dir, exist_ok=True)
                debug_file = os.path.join(debug_dir, f'apify_response_{brand}_{city}.json')
                with open(debug_file, 'w') as f:
                    json.dump(data, f, indent=2)
                print(f"Full response saved to {debug_file}")
                
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
