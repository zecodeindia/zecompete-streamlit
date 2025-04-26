"""
fetch_volume.py - Improved DataForSEO API implementation
"""
import requests
import time
import json
from typing import List, Dict, Any, Optional

def fetch_volume(
    keywords: List[str],
    location_code: int = 1023191,  # Bengaluru, India by default
    language_code: str = "en",
    include_clickstream: bool = True  # Support for clickstream data
) -> List[Dict[str, Any]]:
    """
    Fetch search volume data from DataForSEO
    
    Args:
        keywords: List of keywords to get volume for
        location_code: Location code (default: Bengaluru)
        language_code: Language code (default: English)
        include_clickstream: Whether to include clickstream data
        
    Returns:
        List of result dictionaries with volume data
    """
    from src.config import secret
    
    # Get API credentials
    try:
        DFS_USER = secret("DFS_USER")
        DFS_PASS = secret("DFS_PASS")
    except Exception as e:
        print(f"Error fetching DataForSEO credentials: {str(e)}")
        return []
        
    if not DFS_USER or not DFS_PASS:
        print("DataForSEO credentials missing")
        return []
    
    # Prepare authentication and API endpoint
    auth = (DFS_USER, DFS_PASS)
    url = "https://api.dataforseo.com/v3/keywords_data/google_ads/search_volume/live"
    
    # Prepare the API payload
    payload = {
        "keywords": keywords,
        "language_code": language_code,
        "location_code": location_code,
        "include_adult_keywords": False
    }
    
    # Add optional parameters
    if include_clickstream:
        payload["include_clickstream_data"] = True
    
    # DataForSEO has a limit on batch size, so process in chunks if needed
    results = []
    chunk_size = 100
    
    for i in range(0, len(keywords), chunk_size):
        chunk = keywords[i:i+chunk_size]
        chunk_payload = {**payload, "keywords": chunk}
        
        try:
            print(f"Sending request to DataForSEO for {len(chunk)} keywords...")
            response = requests.post(url, json=[chunk_payload], auth=auth)
            
            # Add debugging for response status and content
            print(f"DataForSEO API response status: {response.status_code}")
            
            if response.status_code != 200:
                print(f"DataForSEO API error: {response.text}")
                continue
                
            data = response.json()
            
            # Check for API errors
            if data.get("status_code") != 20000:
                error_message = data.get("status_message", "Unknown error")
                print(f"DataForSEO API returned error: {error_message}")
                continue
                
            # Process successful response
            tasks = data.get("tasks", [])
            if not tasks:
                print("No tasks found in DataForSEO response")
                continue
                
            # Extract results from the task
            for task in tasks:
                if task.get("status_code") != 20000:
                    print(f"Task error: {task.get('status_message', 'Unknown error')}")
                    continue
                    
                task_results = task.get("result", [])
                if task_results:
                    results.extend(task_results)
                    print(f"Processed {len(task_results)} keywords successfully")
                else:
                    print("No results found in task")
            
            # Respect API rate limits
            if i + chunk_size < len(keywords):
                print("Sleeping 1 second to respect API rate limits...")
                time.sleep(1)
                
        except Exception as e:
            print(f"Error fetching volume from DataForSEO: {str(e)}")
            import traceback
            traceback.print_exc()
    
    # Print sample of results for debugging
    if results:
        print(f"Total keywords with data: {len(results)}")
        print(f"Sample result structure: {json.dumps(results[0], indent=2)[:500]}...")
    else:
        print("No results returned from DataForSEO")
        
    return results

# Test function - uncomment to test directly
"""
if __name__ == "__main__":
    test_keywords = ["zara bangalore", "h&m near me", "max fashion store"]
    results = fetch_volume(test_keywords)
    for r in results:
        print(f"Keyword: {r.get('keyword')}, Volume: {r.get('search_volume')}")
"""
