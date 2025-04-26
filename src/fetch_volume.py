# ✅ Corrected fetch_volume.py

import requests
import json
import time
from typing import List, Dict, Any

def fetch_volume(keywords: List[str]) -> Dict[str, Dict]:
    """
    Fetch search volume data from DataForSEO API
    
    Args:
        keywords: List of keywords to fetch volume for
        
    Returns:
        Dictionary mapping keywords to their volume data
    """
    from src.config import secret  # Get secrets

    # Get credentials
    DFS_USER = secret("DFS_USER")
    DFS_PASS = secret("DFS_PASS")

    # Check if credentials exist
    if not DFS_USER or not DFS_PASS:
        print("ERROR: DataForSEO credentials missing")
        return {}

    # Print masked credentials for debugging
    print(f"Using DataForSEO credentials: {DFS_USER[:3]}.../{DFS_PASS[:3]}...")

    # Setup auth and URL
    auth = (DFS_USER, DFS_PASS)
    
    # IMPORTANT: Use the correct API endpoint - the Google endpoint, not Google Ads
    url = "https://api.dataforseo.com/v3/keywords_data/google/search_volume/live"

    # Prepare payload
    payload = {
        "keywords": keywords,
        "language_code": "en",
        "location_code": 1023191,  # Bengaluru, India
    }

    try:
        # Make the API request
        print(f"Sending request to DataForSEO for {len(keywords)} keywords...")
        response = requests.post(url, json=[payload], auth=auth)
        
        # Log response status
        print(f"DataForSEO API response status: {response.status_code}")
        
        # Handle non-200 responses
        if response.status_code != 200:
            print(f"DataForSEO API error: {response.text}")
            return {}

        # Parse the response
        data = response.json()
        
        # Save raw response for debugging
        with open("dataforseo_response.json", "w") as f:
            json.dump(data, f, indent=2)
        print("Raw response saved to dataforseo_response.json")
        
        # Check for API errors
        if data.get("status_code") != 20000:
            print(f"DataForSEO API error: {data.get('status_message', 'Unknown error')}")
            return {}

        # Process the results
        enriched = {}
        
        # Extract tasks from response
        tasks = data.get("tasks", [])
        if not tasks:
            print("No tasks found in DataForSEO response")
            return {}
            
        print(f"Found {len(tasks)} tasks in response")
        
        # Process each task
        for task in tasks:
            # Check task status
            if task.get("status_code") != 20000:
                print(f"Task error: {task.get('status_message', 'Unknown error')}")
                continue
                
            # Get results from task
            results = task.get("result", [])
            if not results:
                print("No results found in task")
                continue
                
            print(f"Processing {len(results)} results")
            
            # Process each result
            for r in results:
                keyword = r.get("keyword", "")
                if not keyword:
                    continue
                    
                # Extract core metrics
                search_volume = r.get("search_volume", 0)
                competition = r.get("competition_index", 0.0)
                cpc = r.get("cpc", 0.0)
                
                # Add to enriched data
                enriched[keyword] = {
                    "search_volume": search_volume,
                    "competition": competition,
                    "cpc": cpc / 100.0 if cpc else 0.0,  # Convert to dollars if needed
                }
                
                # Print first result for debugging
                if keyword == keywords[0]:
                    print(f"Sample result for '{keyword}': {json.dumps(enriched[keyword])}")

        print(f"Successfully processed {len(enriched)} keywords")
        return enriched
        
    except Exception as e:
        print(f"Error fetching volume from DataForSEO: {e}")
        import traceback
        traceback.print_exc()
        return {}
