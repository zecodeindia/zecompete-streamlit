# apify_test.py
import streamlit as st
import requests
import json
import pandas as pd

st.set_page_config(page_title="Apify Test", layout="wide")
st.title("Apify Connection Test")

# Get Apify token
apify_token = st.secrets["APIFY_TOKEN"]
st.write(f"Using Apify token: {apify_token[:5]}***")

# Input for task ID
task_id = st.text_input("Apify Task ID", "your-task-id-here")

# Inputs for test
brand = st.text_input("Brand to search", "Zara")
city = st.text_input("City to search", "Bengaluru")

if st.button("Test Apify Connection", key="test_apify"):
    st.write(f"Testing Apify connection for {brand} in {city}...")
    
    try:
        url = f"https://api.apify.com/v2/actor-tasks/{task_id}/run-sync-get-dataset-items"
        params = {"token": apify_token}
        payload = {
            "searchStringsArray": [brand],
            "locationQuery": city,
            "maxReviews": 0,
            "maxImages": 0,
            "maxItems": 20
        }
        
        st.write(f"Sending request to Apify with payload:")
        st.json(payload)
        
        with st.spinner("Waiting for Apify response..."):
            resp = requests.post(url, params=params, json=payload, timeout=300)
        
        st.write(f"Response status code: {resp.status_code}")
        
        if resp.status_code == 200:
            data = resp.json()
            
            if isinstance(data, list):
                st.success(f"Successfully received {len(data)} items from Apify")
                
                if data:
                    # Show sample of first item
                    st.subheader("First Result Sample")
                    st.json(data[0])
                    
                    # Show available fields
                    st.subheader("Available Fields")
                    st.write(list(data[0].keys()))
                    
                    # Convert to DataFrame for easier viewing
                    df = pd.json_normalize(data)
                    st.subheader("Data Preview")
                    st.dataframe(df)
                    
                    # Check for crucial fields
                    if 'name' in df.columns:
                        st.success("✅ 'name' field is present")
                    else:
                        st.error("❌ 'name' field is missing")
                        
                    if 'placeId' in df.columns or 'id' in df.columns:
                        st.success("✅ ID field is present")
                    else:
                        st.error("❌ ID field is missing")
                else:
                    st.warning("No data items returned from Apify")
            else:
                st.error(f"Unexpected response format. Expected list, got {type(data)}")
                st.json(data)
        else:
            st.error(f"Request failed with status code: {resp.status_code}")
            st.write("Response content:")
            st.write(resp.text)
    
    except Exception as e:
        st.error(f"Error testing Apify connection: {str(e)}")
        import traceback
        st.write(traceback.format_exc())
