"""
Task manager to track and process Apify tasks
"""
import time
import json
import os
from typing import Dict, List, Optional
import pandas as pd
from src.scrape_maps import check_task_status, get_dataset_id_from_run, fetch_dataset_items
from src.embed_upsert import upsert_places

# Directory to store task state
TASK_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "task_data")
os.makedirs(TASK_DIR, exist_ok=True)

TASK_STATE_FILE = os.path.join(TASK_DIR, "task_state.json")

def load_task_state() -> Dict:
    """Load the current task state from disk"""
    if os.path.exists(TASK_STATE_FILE):
        try:
            with open(TASK_STATE_FILE, 'r') as f:
                return json.load(f)
        except Exception as e:
            print(f"Error loading task state: {str(e)}")
    return {"tasks": {}}

def save_task_state(state: Dict):
    """Save the task state to disk"""
    try:
        with open(TASK_STATE_FILE, 'w') as f:
            json.dump(state, f, indent=2)
    except Exception as e:
        print(f"Error saving task state: {str(e)}")

def add_task(run_id: str, brand: str, city: str):
    """Add a new task to the state"""
    state = load_task_state()
    
    state["tasks"][run_id] = {
        "brand": brand,
        "city": city,
        "status": "RUNNING",
        "created_at": time.time(),
        "updated_at": time.time(),
        "processed": False
    }
    
    save_task_state(state)
    print(f"Added task {run_id} for {brand} in {city} to state")

def update_task_status(run_id: str, status: str):
    """Update a task's status"""
    state = load_task_state()
    
    if run_id in state["tasks"]:
        state["tasks"][run_id]["status"] = status
        state["tasks"][run_id]["updated_at"] = time.time()
        save_task_state(state)
        print(f"Updated task {run_id} status to {status}")
    else:
        print(f"Task {run_id} not found in state")

def mark_task_processed(run_id: str):
    """Mark a task as processed"""
    state = load_task_state()
    
    if run_id in state["tasks"]:
        state["tasks"][run_id]["processed"] = True
        state["tasks"][run_id]["updated_at"] = time.time()
        save_task_state(state)
        print(f"Marked task {run_id} as processed")
    else:
        print(f"Task {run_id} not found in state")

def get_pending_tasks() -> List[Dict]:
    """Get all tasks that are complete but not processed"""
    state = load_task_state()
    
    pending = []
    for run_id, task in state["tasks"].items():
        if task["status"] == "SUCCEEDED" and not task["processed"]:
            task_copy = task.copy()
            task_copy["run_id"] = run_id
            pending.append(task_copy)
    
    return pending

def get_running_tasks() -> List[Dict]:
    """Get all tasks that are still running"""
    state = load_task_state()
    
    running = []
    for run_id, task in state["tasks"].items():
        if task["status"] == "RUNNING":
            task_copy = task.copy()
            task_copy["run_id"] = run_id
            running.append(task_copy)
    
    return running

def check_running_tasks():
    """Check the status of all running tasks"""
    running_tasks = get_running_tasks()
    
    for task in running_tasks:
        run_id = task["run_id"]
        current_status = check_task_status(run_id)
        
        if current_status != "RUNNING" and current_status != "UNKNOWN":
            update_task_status(run_id, current_status)
            print(f"Task {run_id} updated from RUNNING to {current_status}")

def process_pending_tasks() -> int:
    """Process all pending tasks, returns number of tasks processed"""
    pending_tasks = get_pending_tasks()
    processed = 0
    
    for task in pending_tasks:
        run_id = task["run_id"]
        brand = task["brand"]
        city = task["city"]
        
        print(f"Processing completed task {run_id} for {brand} in {city}")
        
        # Get the dataset ID
        dataset_id = get_dataset_id_from_run(run_id)
        
        if not dataset_id:
            print(f"No dataset ID found for run {run_id}")
            # Mark as processed anyway to avoid endless retries
            mark_task_processed(run_id)
            continue
        
        # Get the data
        data = fetch_dataset_items(dataset_id)
        
        if not data:
            print(f"No data found for dataset {dataset_id}")
            mark_task_processed(run_id)
            continue
        
        # Process the data
        try:
            # 1. Convert data to DataFrame
            df = pd.json_normalize(data)
            print(f"Converted {len(data)} data points to DataFrame")
            
            # 2. Clean DataFrame
            # Keep essential columns
            keep_cols = [c for c in df.columns if c in ["name", "title", "placeId", "totalScore", "reviewsCount", 
                                                        "gpsCoordinates.lat", "gpsCoordinates.lng", 
                                                        "address", "latitude", "longitude"]]
            
            if keep_cols:
                df = df[keep_cols].drop_duplicates(subset=keep_cols[0], keep="first").reset_index(drop=True)
            
            # 3. Upsert places to Pinecone
            print(f"Upserting {len(df)} places to Pinecone maps namespace")
            upsert_places(df, brand, city)
            
            # 4. Generate keywords and fetch search volumes
            try:
                # Import the enhanced keyword pipeline functionality directly
                from enhanced_keyword_pipeline import run_business_keyword_pipeline
                
                # Run the keyword pipeline for the city
                print(f"Running business keyword pipeline for {city}...")
                success = run_business_keyword_pipeline(city)
                
                if success:
                    print(f"Successfully completed keyword pipeline for {city}")
                else:
                    print(f"Keyword pipeline failed for {city}")
            except ImportError:
                # If enhanced_keyword_pipeline is not available, try to use the one from business_keywords_tab
                try:
                    from business_keywords_tab import run_business_keyword_pipeline
                    
                    print(f"Running business keyword pipeline from business_keywords_tab for {city}...")
                    success = run_business_keyword_pipeline(city)
                    
                    if success:
                        print(f"Successfully completed keyword pipeline for {city}")
                    else:
                        print(f"Keyword pipeline failed for {city}")
                except Exception as e:
                    print(f"Error running keyword pipeline from business_keywords_tab: {str(e)}")
                    import traceback
                    traceback.print_exc()
            except Exception as e:
                print(f"Error running keyword pipeline: {str(e)}")
                import traceback
                traceback.print_exc()
            
            # Mark task as processed
            mark_task_processed(run_id)
            processed += 1
            print(f"Successfully processed task {run_id}")
            
        except Exception as e:
            print(f"Error processing task {run_id}: {str(e)}")
            import traceback
            traceback.print_exc()
            # Don't mark as processed so we can retry later
    
    return processed

def process_all_tasks():
    """Check running tasks and process any pending tasks"""
    # First update the status of running tasks
    check_running_tasks()
    
    # Then process any pending tasks
    processed = process_pending_tasks()
    
    return processed
