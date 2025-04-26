""""""
debug_dataforseo.py - Direct testing script for DataForSEO API
Run this file directly to test the API connection and response parsing
"""
import requests
import json
import time
import os
import sys
import traceback
import pandas as pd

# Add the project root to sys.path to allow importing src modules
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# Try to import from src.config, but provide fallbacks if that fails
try:
    from src.config import secret
    def get_secret(key):
        return secret(key)
except ImportError:
    # Fallback to direct environment variable access
    def get_secret(key):
        return os.environ.get(key)

def test_dataforseo_connection():
    """Test basic connection to DataForSEO API"""
    print("\n=== Testing DataForSEO API Connection ===\n")
    
    # Get credentials
    dfs_user = get_secret("DFS_USER")
    dfs_pass = get_secret("DFS_PASS")
    
    if not dfs_user or not dfs_pass:
        print("❌ ERROR: DataForSEO credentials not found")
        print("Please set DFS_USER and DFS_PASS in your environment variables or secrets")
        return False
    
    # Check credentials (first few chars only)
    print(f"Using credentials: {dfs_user[:3]}.../{dfs_pass[:3]}...")
    
    # Test authentication
    auth = (dfs_user, dfs_pass)
    url = "https://api.dataforseo.com/v3/merchant/google/products/task_get/regular"
    
    try:
        response = requests.get(url, auth=auth)
        print(f"Authentication test status code: {response.status_code}")
        
        if response.status_code == 200:
            print("✅ API connection successful")
            return True
        else:
            print(f"❌ API connection failed: {response.text}")
            return False
    except Exception as e:
        print(f"❌ API connection error: {str(e)}")
        return False

def test_search_volume_api(keywords=None):
    """Test the search volume API endpoint"""
    print("\n=== Testing DataForSEO Search Volume API ===\n")
    
    if keywords is None:
        keywords = ["zara bangalore", "h&m near me", "max fashion store", "trends clothing"]
    
    print(f"Testing with keywords: {keywords}")
    
    # Get credentials
    dfs_user = get_secret("DFS_USER")
    dfs_pass = get_secret("DFS_PASS")
    
    if not dfs_user or not dfs_pass:
        print("❌ ERROR: DataForSEO credentials not found")
        return None
    
    auth = (dfs_user, dfs_pass)
    url = "https://api.dataforseo.com/v3/keywords_data/google_ads/search_volume/live"
    
    payload = {
        "keywords": keywords,
        "language_code": "en",
        "location_code": 1023191,  # Bengaluru, India
        "include_adult_keywords": False
    }
    
    try:
        print("Sending request to DataForSEO API...")
        response = requests.post(url, json=[payload], auth=auth)
        
        print(f"API Response Status: {response.status_code}")
        
        if response.status_code != 200:
            print(f"❌ API request failed: {response.text}")
            return None
        
        # Save raw response for debugging
        with open("dataforseo_raw_response.json", "w") as f:
            f.write(response.text)
        print("Raw response saved to dataforseo_raw_response.json")
        
        # Parse response
        data = response.json()
        
        # Check for API errors
        if data.get("status_code") != 20000:
            error_message = data.get("status_message", "Unknown error")
            print(f"❌ DataForSEO API returned error: {error_message}")
            return None
        
        print("✅ API request successful")
        
        # Process response
        tasks = data.get("tasks", [])
        if not tasks:
            print("❌ No tasks found in response")
            return None
        
        print(f"Found {len(tasks)} tasks in response")
        
        all_results = []
        
        for task in tasks:
            if task.get("status_code") != 20000:
                print(f"❌ Task error: {task.get('status_message', 'Unknown error')}")
                continue
            
            results = task.get("result", [])
            if not results:
                print("❌ No results found in task")
                continue
            
            print(f"Task has {len(results)} results")
            
            # Print full structure of first result for debugging
            if results:
                print("\nSample result structure:")
                print(json.dumps(results[0], indent=2))
            
            all_results.extend(results)
        
        print(f"\nProcessed {len(all_results)} total results")
        
        # Convert to pandas DataFrame for easier viewing
        if all_results:
            rows = []
            for item in all_results:
                # Extract basic data
                keyword = item.get("keyword", "")
                search_volume = item.get("search_volume", 0)
                competition = item.get("competition", 0.0)
                cpc = item.get("cpc", 0.0)
                
                # Extract monthly data if available
                if "monthly_searches" in item and item["monthly_searches"]:
                    for monthly in item["monthly_searches"]:
                        rows.append({
                            "keyword": keyword,
                            "year": monthly.get("year", 0),
                            "month": monthly.get("month", 0),
                            "search_volume": monthly.get("search_volume", 0),
                            "competition": competition,
                            "cpc": cpc
                        })
                else:
                    # Add a single row for overall volume
                    import datetime
                    now = datetime.datetime.now()
                    rows.append({
                        "keyword": keyword,
                        "year": now.year,
                        "month": now.month,
                        "search_volume": search_volume,
                        "competition": competition,
                        "cpc": cpc
                    })
            
            if rows:
                df = pd.DataFrame(rows)
                print("\nResults as DataFrame:")
                print(df)
                
                # Save DataFrame to CSV
                df.to_csv("dataforseo_results.csv", index=False)
                print("Results saved to dataforseo_results.csv")
                
                return df
            else:
                print("❌ No rows created from results")
                return None
        else:
            print("❌ No results to process")
            return None
            
    except Exception as e:
        print(f"❌ Error testing search volume API: {str(e)}")
        traceback.print_exc()
        return None

def check_current_keywords_in_pinecone():
    """Check current keywords in Pinecone for debugging"""
    print("\n=== Checking Current Keywords in Pinecone ===\n")
    
    try:
        # Import Pinecone
        from pinecone import Pinecone
        pc = Pinecone(api_key=get_secret("PINECONE_API_KEY"))
        index = pc.Index("zecompete")
        
        # Get stats
        stats = index.describe_index_stats()
        
        if "keywords" not in stats.get("namespaces", {}):
            print("❌ No 'keywords' namespace found in Pinecone")
            return None
        
        count = stats["namespaces"]["keywords"].get("vector_count", 0)
        print(f"Found {count} keyword vectors in Pinecone")
        
        # Query some samples
        dimension = stats.get("dimension", 1536)
        dummy_vector = [0.0] * dimension
        
        results = index.query(
            vector=dummy_vector,
            top_k=10,
            namespace="keywords",
            include_metadata=True
        )
        
        if not results.matches:
            print("❌ No matches found in query")
            return None
        
        print(f"Retrieved {len(results.matches)} sample keywords")
        
        # Check metadata
        rows = []
        for i, match in enumerate(results.matches):
            if not match.metadata:
                print(f"❌ Match {i} has no metadata")
                continue
            
            print(f"\nMatch {i} metadata:")
            print(json.dumps(match.metadata, indent=2))
            
            rows.append(match.metadata)
        
        if rows:
            df = pd.DataFrame(rows)
            print("\nSample keywords as DataFrame:")
            print(df)
            
            # Save DataFrame to CSV
            df.to_csv("pinecone_keywords_sample.csv", index=False)
            print("Sample saved to pinecone_keywords_sample.csv")
            
            return df
        else:
            print("❌ No valid metadata found in matches")
            return None
            
    except Exception as e:
        print(f"❌ Error checking Pinecone: {str(e)}")
        traceback.print_exc()
        return None

def test_embed_upsert():
    """Test the embed_upsert function with sample data"""
    print("\n=== Testing embed_upsert Function ===\n")
    
    try:
        from src.embed_upsert import upsert_keywords
        
        # Create sample data
        import random
        
        keywords = ["zara bangalore", "h&m near me", "max fashion store", "trends clothing"]
        
        rows = []
        for keyword in keywords:
            # Create some variation
            volume = random.randint(50, 500)
            competition = round(random.uniform(0.1, 0.9), 2)
            cpc = round(random.uniform(0.5, 3.0), 2)
            
            rows.append({
                "keyword": keyword,
                "year": 2025,
                "month": 4,
                "search_volume": volume,
                "competition": competition,
                "cpc": cpc,
                "city": "Bengaluru"
            })
        
        df = pd.DataFrame(rows)
        print("Sample DataFrame for upsert:")
        print(df)
        
        # Try upsert
        print("\nAttempting upsert_keywords...")
        upsert_keywords(df, "Bengaluru")
        
        print("✅ upsert_keywords completed without errors")
        
        # Verify upload
        check_current_keywords_in_pinecone()
        
        return True
    except Exception as e:
        print(f"❌ Error testing embed_upsert: {str(e)}")
        traceback.print_exc()
        return False

def execute_test_pipeline(test_name=None):
    """Run the selected test or all tests if none specified"""
    all_tests = {
        "connection": test_dataforseo_connection,
        "volume": test_search_volume_api,
        "pinecone": check_current_keywords_in_pinecone,
        "upsert": test_embed_upsert
    }
    
    if test_name:
        if test_name in all_tests:
            print(f"\n====== Running Test: {test_name} ======\n")
            all_tests[test_name]()
        else:
            print(f"Unknown test: {test_name}")
            print(f"Available tests: {', '.join(all_tests.keys())}")
    else:
        # Run all tests in sequence
        print("\n====== Running All Tests ======\n")
        
        # First test connection
        if not test_dataforseo_connection():
            print("\n❌ Connection test failed, stopping tests")
            return
        
        # Then test volume API
        volume_df = test_search_volume_api()
        
        # Check Pinecone
        check_current_keywords_in_pinecone()
        
        # Test upsert if we have volume data
        if volume_df is not None and not volume_df.empty:
            test_embed_upsert()
        else:
            print("\n⚠️ Skipping upsert test due to missing volume data")

if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description="Debug DataForSEO API integration")
    parser.add_argument("--test", choices=["connection", "volume", "pinecone", "upsert", "all"],
                        default="all", help="Select which test to run")
    parser.add_argument("--keywords", nargs="+", help="Custom keywords to test with")
    
    args = parser.parse_args()
    
    if args.test == "all":
        execute_test_pipeline()
    elif args.test == "volume" and args.keywords:
        test_search_volume_api(args.keywords)
    else:
        execute_test_pipeline(args.test)
debug_dataforseo.py - Direct testing script for DataForSEO API
Run this file directly to test the API connection and response parsing
"""
import requests
import json
import time
import os
import sys
import traceback
import pandas as pd

# Add the project root to sys.path to allow importing src modules
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# Try to import from src.config, but provide fallbacks if that fails
try:
    from src.config import secret
    def get_secret(key):
        return secret(key)
except ImportError:
    # Fallback to direct environment variable access
    def get_secret(key):
        return os.environ.get(key)

def test_dataforseo_connection():
    """Test basic connection to DataForSEO API"""
    print("\n=== Testing DataForSEO API Connection ===\n")
    
    # Get credentials
    dfs_user = get_secret("DFS_USER")
    dfs_pass = get_secret("DFS_PASS")
    
    if not dfs_user or not dfs_pass:
        print("❌ ERROR: DataForSEO credentials not found")
        print("Please set DFS_USER and DFS_PASS in your environment variables or secrets")
        return False
    
    # Check credentials (first few chars only)
    print(f"Using credentials: {dfs_user[:3]}.../{dfs_pass[:3]}...")
    
    # Test authentication
    auth = (dfs_user, dfs_pass)
    url = "https://api.dataforseo.com/v3/merchant/google/products/task_get/regular"
    
    try:
        response = requests.get(url, auth=auth)
        print(f"Authentication test status code: {response.status_code}")
        
        if response.status_code == 200:
            print("✅ API connection successful")
            return True
        else:
            print(f"❌ API connection failed: {response.text}")
            return False
    except Exception as e:
        print(f"❌ API connection error: {str(e)}")
        return False

def test_search_volume_api(keywords=None):
    """Test the search volume API endpoint"""
    print("\n=== Testing DataForSEO Search Volume API ===\n")
    
    if keywords is None:
        keywords = ["zara bangalore", "h&m near me", "max fashion store", "trends clothing"]
    
    print(f"Testing with keywords: {keywords}")
    
    # Get credentials
    dfs_user = get_secret("DFS_USER")
    dfs_pass = get_secret("DFS_PASS")
    
    if not dfs_user or not dfs_pass:
        print("❌ ERROR: DataForSEO credentials not found")
        return None
    
    auth = (dfs_user, dfs_pass)
    url = "https://api.dataforseo.com/v3/keywords_data/google_ads/search_volume/live"
    
    payload = {
        "keywords": keywords,
        "language_code": "en",
        "location_code": 1023191,  # Bengaluru, India
        "include_adult_keywords": False
    }
    
    try:
        print("Sending request to DataForSEO API...")
        response = requests.post(url, json=[payload], auth=auth)
        
        print(f"API Response Status: {response.status_code}")
        
        if response.status_code != 200:
            print(f"❌ API request failed: {response.text}")
            return None
        
        # Save raw response for debugging
        with open("dataforseo_raw_response.json", "w") as f:
            f.write(response.text)
        print("Raw response saved to dataforseo_raw_response.json")
        
        # Parse response
        data = response.json()
        
        # Check for API errors
        if data.get("status_code") != 20000:
            error_message = data.get("status_message", "Unknown error")
            print(f"❌ DataForSEO API returned error: {error_message}")
            return None
        
        print("✅ API request successful")
        
        # Process response
        tasks = data.get("tasks", [])
        if not tasks:
            print("❌ No tasks found in response")
            return None
        
        print(f"Found {len(tasks)} tasks in response")
        
        all_results = []
        
        for task in tasks:
            if task.get("status_code") != 20000:
                print(f"❌ Task error: {task.get('status_message', 'Unknown error')}")
                continue
            
            results = task.get("result", [])
            if not results:
                print("❌ No results found in task")
                continue
            
            print(f"Task has {len(results)} results")
            
            # Print full structure of first result for debugging
            if results:
                print("\nSample result structure:")
                print(json.dumps(results[0], indent=2))
            
            all_results.extend(results)
        
        print(f"\nProcessed {len(all_results)} total results")
        
        # Convert to pandas DataFrame for easier viewing
        if all_results:
            rows = []
            for item in all_results:
                # Extract basic data
                keyword = item.get("keyword", "")
                search_volume = item.get("search_volume", 0)
                competition = item.get("competition", 0.0)
                cpc = item.get("cpc", 0.0)
                
                # Extract monthly data if available
                if "monthly_searches" in item and item["monthly_searches"]:
                    for monthly in item["monthly_searches"]:
                        rows.append({
                            "keyword": keyword,
                            "year": monthly.get("year", 0),
                            "month": monthly.get("month", 0),
                            "search_volume": monthly.get("search_volume", 0),
                            "competition": competition,
                            "cpc": cpc
                        })
                else:
                    # Add a single row for overall volume
                    import datetime
                    now = datetime.datetime.now()
                    rows.append({
                        "keyword": keyword,
                        "year": now.year,
                        "month": now.month,
                        "search_volume": search_volume,
                        "competition": competition,
                        "cpc": cpc
                    })
            
            if rows:
                df = pd.DataFrame(rows)
                print("\nResults as DataFrame:")
                print(df)
                
                # Save DataFrame to CSV
                df.to_csv("dataforseo_results.csv", index=False)
                print("Results saved to dataforseo_results.csv")
                
                return df
            else:
                print("❌ No rows created from results")
                return None
        else:
            print("❌ No results to process")
            return None
            
    except Exception as e:
        print(f"❌ Error testing search volume API: {str(e)}")
        traceback.print_exc()
        return None

def check_current_keywords_in_pinecone():
    """Check current keywords in Pinecone for debugging"""
    print("\n=== Checking Current Keywords in Pinecone ===\n")
    
    try:
        # Import Pinecone
        from pinecone import Pinecone
        pc = Pinecone(api_key=get_secret("PINECONE_API_KEY"))
        index = pc.Index("zecompete")
        
        # Get stats
        stats = index.describe_index_stats()
        
        if "keywords" not in stats.get("namespaces", {}):
            print("❌ No 'keywords' namespace found in Pinecone")
            return None
        
        count = stats["namespaces"]["keywords"].get("vector_count", 0)
        print(f"Found {count} keyword vectors in Pinecone")
        
        # Query some samples
        dimension = stats.get("dimension", 1536)
        dummy_vector = [0.0] * dimension
        
        results = index.query(
            vector=dummy_vector,
            top_k=10,
            namespace="keywords",
            include_metadata=True
        )
        
        if not results.matches:
            print("❌ No matches found in query")
            return None
        
        print(f"Retrieved {len(results.matches)} sample keywords")
        
        # Check metadata
        rows = []
        for i, match in enumerate(results.matches):
            if not match.metadata:
                print(f"❌ Match {i} has no metadata")
                continue
            
            print(f"\nMatch {i} metadata:")
            print(json.dumps(match.metadata, indent=2))
            
            rows.append(match.metadata)
        
        if rows:
            df = pd.DataFrame(rows)
            print("\nSample keywords as DataFrame:")
            print(df)
            
            # Save DataFrame to CSV
            df.to_csv("pinecone_keywords_sample.csv", index=False)
            print("Sample saved to pinecone_keywords_sample.csv")
            
            return df
        else:
            print("❌ No valid metadata found in matches")
            return None
            
    except Exception as e:
        print(f"❌ Error checking Pinecone: {str(e)}")
        traceback.print_exc()
        return None

def test_embed_upsert():
    """Test the embed_upsert function with sample data"""
    print("\n=== Testing embed_upsert Function ===\n")
    
    try:
        from src.embed_upsert import upsert_keywords
        
        # Create sample data
        import random
        
        keywords = ["zara bangalore", "h&m near me", "max fashion store", "trends clothing"]
        
        rows = []
        for keyword in keywords:
            # Create some variation
            volume = random.randint(50, 500)
            competition = round(random.uniform(0.1, 0.9), 2)
            cpc = round(random.uniform(0.5, 3.0), 2)
            
            rows.append({
                "keyword": keyword,
                "year": 2025,
                "month": 4,
                "search_volume": volume,
                "competition": competition,
                "cpc": cpc,
                "city": "Bengaluru"
            })
        
        df = pd.DataFrame(rows)
        print("Sample DataFrame for upsert:")
        print(df)
        
        # Try upsert
        print("\nAttempting upsert_keywords...")
        upsert_keywords(df, "Bengaluru")
        
        print("✅ upsert_keywords completed without errors")
        
        # Verify upload
        check_current_keywords_in_pinecone()
        
        return True
    except Exception as e:
        print(f"❌ Error testing embed_upsert: {str(e)}")
        traceback.print_exc()
        return False

def execute_test_pipeline(test_name=None):
    """Run the selected test or all tests if none specified"""
    all_tests = {
        "connection": test_dataforseo_connection,
        "volume": test_search_volume_api,
        "pinecone": check_current_keywords_in_pinecone,
        "upsert": test_embed_upsert
    }
    
    if test_name:
        if test_name in all_tests:
            print(f"\n====== Running Test: {test_name} ======\n")
            all_tests[test_name]()
        else:
            print(f"Unknown test: {test_name}")
            print(f"Available tests: {', '.join(all_tests.keys())}")
    else:
        # Run all tests in sequence
        print("\n====== Running All Tests ======\n")
        
        # First test connection
        if not test_dataforseo_connection():
            print("\n❌ Connection test failed, stopping tests")
            return
        
        # Then test volume API
        volume_df = test_search_volume_api()
        
        # Check Pinecone
        check_current_keywords_in_pinecone()
        
        # Test upsert if we have volume data
        if volume_df is not None and not volume_df.empty:
            test_embed_upsert()
        else:
            print("\n⚠️ Skipping upsert test due to missing volume data")

if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description="Debug DataForSEO API integration")
    parser.add_argument("--test", choices=["connection", "volume", "pinecone", "upsert", "all"],
                        default="all", help="Select which test to run")
    parser.add_argument("--keywords", nargs="+", help="Custom keywords to test with")
    
    args = parser.parse_args()
    
    if args.test == "all":
        execute_test_pipeline()
    elif args.test == "volume" and args.keywords:
        test_search_volume_api(args.keywords)
    else:
        execute_test_pipeline(args.test)
