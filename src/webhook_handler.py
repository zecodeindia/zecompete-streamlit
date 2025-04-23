"""
Webhook handler for Apify task notifications
"""
import json
import hmac
import hashlib
import secrets
import streamlit as st
from typing import Dict, Any, Optional
from src.config import secret
from src.scrape_maps import fetch_dataset_items
from src.task_manager import add_task, update_task_status, process_all_tasks

def generate_webhook_secret() -> str:
    """Generate a random webhook secret"""
    return secrets.token_hex(16)

def get_webhook_secret() -> str:
    """Get or create a webhook secret"""
    # Try to get from Streamlit secrets
    try:
        return secret("WEBHOOK_SECRET")
    except:
        # Generate a new one
        if "webhook_secret" not in st.session_state:
            st.session_state.webhook_secret = generate_webhook_secret()
        return st.session_state.webhook_secret

def verify_webhook_signature(payload: Dict[str, Any], signature: str, secret: str) -> bool:
    """Verify the webhook signature"""
    if not signature or not secret:
        return False
        
    # Create a signature from the payload
    payload_str = json.dumps(payload, separators=(',', ':'))
    expected_signature = hmac.new(
        secret.encode('utf-8'),
        payload_str.encode('utf-8'),
        hashlib.sha256
    ).hexdigest()
    
    # Compare with the provided signature
    return hmac.compare_digest(expected_signature, signature)

def create_apify_webhook(task_id: str, callback_url: str) -> Optional[str]:
    """Create a webhook in Apify to notify when a task completes"""
    import requests
    
    # Get Apify token
    try:
        token = secret("APIFY_TOKEN")
    except:
        print("Apify token not found")
        return None
    
    # Get or create webhook secret
    webhook_secret = get_webhook_secret()
    
    # Create the webhook
    url = f"https://api.apify.com/v2/actor-tasks/{task_id}/webhooks"
    
    # Prepare the request
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json"
    }
    
    # Prepare the payload
    payload = {
        "isEnabled": True,
        "eventTypes": ["ACTOR.RUN.SUCCEEDED"],
        "requestUrl": callback_url,
        "payloadTemplate": json.dumps({
            "runId": "{{actorRunId}}",
            "datasetId": "{{defaultDatasetId}}",
            "taskId": "{{actorTaskId}}",
            "secret": webhook_secret
        }),
        "contentType": "application/json"
    }
    
    # Send the request
    try:
        response = requests.post(url, json=payload, headers=headers)
        
        if response.status_code == 201:
            webhook_data = response.json()
            webhook_id = webhook_data.get("id")
            print(f"Created webhook with ID: {webhook_id}")
            return webhook_id
        else:
            print(f"Failed to create webhook: {response.status_code} - {response.text}")
            return None
    except Exception as e:
        print(f"Error creating webhook: {str(e)}")
        return None

def handle_webhook_payload(payload: Dict[str, Any]) -> bool:
    """Handle a webhook payload from Apify"""
    # Extract relevant information
    run_id = payload.get("runId")
    dataset_id = payload.get("datasetId")
    task_id = payload.get("taskId")
    payload_secret = payload.get("secret")
    
    # Verify the secret
    webhook_secret = get_webhook_secret()
    if payload_secret != webhook_secret:
        print("Invalid webhook secret")
        return False
    
    if not run_id or not dataset_id:
        print("Missing required fields in webhook payload")
        return False
    
    # Process the webhook
    print(f"Processing webhook for run {run_id}, dataset {dataset_id}")
    
    # Update the task status
    update_task_status(run_id, "SUCCEEDED")
    
    # Process all pending tasks
    processed = process_all_tasks()
    
    return processed > 0

def process_dataset_directly(dataset_id: str, brand: str, city: str) -> bool:
    """Process an Apify dataset directly without a webhook"""
    from src.run_pipeline import run
    import pandas as pd
    
    # Fetch the dataset
    data = fetch_dataset_items(dataset_id)
    
    if not data:
        print(f"No data found for dataset {dataset_id}")
        return False
    
    try:
        # Option 1: Use the high-level run function
        run(brand, city)
        return True
    except Exception as e:
        print(f"Error processing dataset {dataset_id}: {str(e)}")
        
        # Option 2: Try direct processing
        try:
            from src.embed_upsert import upsert_places
            df = pd.json_normalize(data)
            upsert_places(df, brand, city)
            return True
        except Exception as e2:
            print(f"Error with direct processing: {str(e2)}")
            return False
