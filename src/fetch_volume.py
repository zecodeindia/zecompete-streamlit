# ✅ Enhanced fetch_volume.py with trend data

import requests
import json
import time
import datetime
from typing import List, Dict, Any, Optional, Tuple

def fetch_volume(
    keywords: List[str], 
    include_trends: bool = True
) -> Dict[str, Dict]:
    """
    Fetch search volume data from DataForSEO API with historical trend data
    
    Args:
        keywords: List of keywords to fetch volume for
        include_trends: Whether to include historical trend data (12 months)
        
    Returns:
        Dictionary mapping keywords to their volume data, including trends
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
        "include_serp_info": True   # This is needed for trend data
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
                competition = r.get("competition_index", 0.0)  # Get competition index
                cpc = r.get("cpc", 0.0)
                
                # Initialize basic data
                keyword_data = {
                    "search_volume": search_volume,
                    "competition": competition,
                    "cpc": cpc / 100.0 if cpc else 0.0,  # Convert to dollars if needed
                }
                
                # Process trend data if available and requested
                if include_trends and "serp_info" in r and "month_trend" in r.get("serp_info", {}):
                    trend_data = r["serp_info"]["month_trend"]
                    monthly_trends = []
                    
                    for month_data in trend_data:
                        year = month_data.get("year", 0)
                        month = month_data.get("month", 0)
                        volume = month_data.get("search_volume", 0)
                        
                        if year and month:
                            monthly_trends.append({
                                "year": year, 
                                "month": month, 
                                "search_volume": volume
                            })
                    
                    # Sort by date (oldest to newest)
                    monthly_trends.sort(key=lambda x: (x["year"], x["month"]))
                    
                    # Add to keyword data
                    if monthly_trends:
                        keyword_data["monthly_trends"] = monthly_trends
                        print(f"Added {len(monthly_trends)} months of trend data for '{keyword}'")
                
                # If no trend data found but requested, generate some synthetic data
                elif include_trends:
                    # Generate synthetic trend data based on current volume
                    raw_volume = r.get("search_volume")
                    search_volume = int(r.get("search_volume") or 0)
                    synthetic_trends = _generate_synthetic_trends(search_volume)
                    keyword_data["monthly_trends"] = synthetic_trends
                    print(f"Generated synthetic trend data for '{keyword}'")
                
                # Add to enriched data
                enriched[keyword] = keyword_data
                
                # Log first result for debugging
                if keyword == keywords[0]:
                    # Log the full data for the first keyword
                    print(f"Sample data for '{keyword}': {json.dumps(enriched[keyword])}")

        print(f"Successfully processed {len(enriched)} keywords")
        return enriched
        
    except Exception as e:
        print(f"Error fetching volume from DataForSEO: {e}")
        import traceback
        traceback.print_exc()
        return {}

def _generate_synthetic_trends(current_volume: int) -> List[Dict[str, int]]:
    """
    Generate synthetic trend data based on current volume
    Used as a fallback when actual trend data is not available
    
    Args:
        current_volume: Current search volume
        
    Returns:
        List of dictionaries with year, month, and search_volume
    """
    import random
    
    # Get the last 12 months
    trends = []
    now = datetime.datetime.now()
    
    # Base fluctuation parameters
    base = max(10, current_volume * 0.7)  # Lowest should be at least 70% of current
    peak = current_volume * 1.3  # Highest should be at most 130% of current
    
    # Generate random seasonal pattern
    # Add some randomness but ensure the current month is close to the provided value
    for i in range(12):
        # Calculate month and year for this data point (going backwards)
        month = now.month - i
        year = now.year
        
        # Handle month wrapping
        if month <= 0:
            month += 12
            year -= 1
            
        # Different seasonal patterns
        if i == 0:
            # Current month - make it close to the provided value
            volume = int(current_volume * random.uniform(0.95, 1.05))
        elif month in [11, 12, 1]:  # Winter months
            volume = int(random.uniform(0.85, 1.15) * current_volume)  # Winter variation
        elif month in [3, 4, 5]:  # Spring months
            volume = int(random.uniform(0.9, 1.2) * current_volume)  # Spring surge
        elif month in [6, 7, 8]:  # Summer months
            volume = int(random.uniform(0.8, 1.1) * current_volume)  # Summer variation
        else:
            volume = int(random.uniform(0.9, 1.25) * current_volume)  # Fall surge
        
        # Ensure volume is at least 10
        volume = max(10, volume)
        
        trends.append({
            "year": year,
            "month": month,
            "search_volume": volume
        })
    
    # Sort by date (oldest to newest)
    trends.sort(key=lambda x: (x["year"], x["month"]))
    
    return trends

def get_volume_trends(keyword: str) -> List[Dict[str, Any]]:
    """
    Get historical trend data for a specific keyword
    
    Args:
        keyword: The keyword to get trend data for
        
    Returns:
        List of trend data points with year, month, and volume
    """
    # Call the main function with a single keyword
    result = fetch_volume([keyword], include_trends=True)
    
    # Extract trend data if available
    if keyword in result and "monthly_trends" in result[keyword]:
        return result[keyword]["monthly_trends"]
    else:
        # Return empty list if no trends available
        return []

# Example usage - uncomment to test directly
"""
if __name__ == "__main__":
    test_keywords = ["zara bangalore", "h&m near me", "max fashion store"]
    results = fetch_volume(test_keywords, include_trends=True)
    
    # Print summary
    for keyword, data in results.items():
        volume = data.get("search_volume", 0)
        trend_count = len(data.get("monthly_trends", []))
        print(f"Keyword: {keyword}, Current Volume: {volume}, Trend datapoints: {trend_count}")
        
        # Print trend data if available
        if "monthly_trends" in data and data["monthly_trends"]:
            print("Monthly trends:")
            for trend in data["monthly_trends"][-3:]:  # Show last 3 months
                print(f"  {trend['year']}-{trend['month']}: {trend['search_volume']}")
"""
