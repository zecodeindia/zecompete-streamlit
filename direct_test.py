# direct_test.py
import streamlit as st
import requests
import json
import pandas as pd
from openai import OpenAI
from pinecone import Pinecone

st.set_page_config(page_title="Direct Pipeline Test", layout="wide")
st.title("Direct Apify to Pinecone Test")

# Get tokens from secrets
apify_token = st.secrets["APIFY_TOKEN"]
openai_api_key = st.secrets["OPENAI_API_KEY"]
pinecone_api_key = st.secrets["PINECONE_API_KEY"]

# Inputs for test
task_id = st.text_input("Apify Task ID", "avadhut.sawant~google-maps-scraper-task")
brand = st.text_input("Brand to search", "Zudio")
city = st.text_input("City to search", "Bengaluru")

# Function to run the direct test
def run_direct_test():
    log = []
    
    # Step 1: Scrape from Apify
    log.append(f"1. Scraping data for {brand} in {city} using Apify...")
    
    url = f"https://api.apify.com/v2/actor-tasks/{task_id}/run-sync-get-dataset-items"
    params = {"token": apify_token}
    payload = {
        "searchStringsArray": [brand],
        "locationQuery": city,
        "maxReviews": 0,
        "maxImages": 0,
        "maxItems": 5  # Limit to 5 for testing
    }
    
    response = requests.post(url, params=params, json=payload, timeout=300)
    
    if response.status_code != 200:
        log.append(f"❌ Apify request failed with status code: {response.status_code}")
        log.append(f"Response content: {response.text}")
        return log
    
    data = response.json()
    
    if not isinstance(data, list) or not data:
        log.append(f"❌ Unexpected data format or empty data: {type(data)}")
        return log
    
    log.append(f"✅ Apify returned {len(data)} items")
    log.append(f"Sample item fields: {list(data[0].keys())}")
    
    # Step 2: Generate embeddings with OpenAI
    log.append("2. Generating embeddings with OpenAI...")
    
    # Extract place names
    place_names = []
    for item in data:
        if 'name' in item:
            place_names.append(item['name'])
        elif 'title' in item:
            place_names.append(item['title'])
    
    if not place_names:
        log.append("❌ No place names found in data")
        return log
    
    log.append(f"Found {len(place_names)} place names: {place_names}")
    
    # Generate embeddings
    client = OpenAI(api_key=openai_api_key)
    try:
        response = client.embeddings.create(
            model="text-embedding-3-small",
            input=place_names
        )
        embeddings = [d.embedding for d in response.data]
        log.append(f"✅ Generated {len(embeddings)} embeddings")
    except Exception as e:
        log.append(f"❌ OpenAI embedding generation failed: {str(e)}")
        return log
    
    # Step 3: Upload to Pinecone
    log.append("3. Uploading to Pinecone...")
    
    pc = Pinecone(api_key=pinecone_api_key)
    index = pc.Index("zecompete")
    
    # Prepare records
    records = []
    for i, (item, embedding) in enumerate(zip(data, embeddings)):
        record_id = f"direct-test-{brand}-{city}-{i}"
        
        # Extract metadata
        metadata = {
            "brand": brand,
            "city": city,
            "name": item.get('name', item.get('title', f"{brand} location"))
        }
        
        # Add other fields if available
        if 'rating' in item:
            metadata['rating'] = float(item['rating'])
        if 'reviewsCount' in item:
            metadata['reviews'] = int(item['reviewsCount'])
            
        records.append((record_id, embedding, metadata))
    
    # Upsert to Pinecone
    try:
        result = index.upsert(vectors=records, namespace="direct_test")
        log.append(f"✅ Pinecone upsert successful: {result}")
    except Exception as e:
        log.append(f"❌ Pinecone upsert failed: {str(e)}")
        return log
    
    # Step 4: Verify upload
    log.append("4. Verifying data in Pinecone...")
    
    try:
        stats = index.describe_index_stats()
        log.append(f"Index stats: {stats}")
        
        # Check for our namespace
        namespaces = stats.get("namespaces", {})
        if "direct_test" in namespaces:
            count = namespaces["direct_test"].get("vector_count", 0)
            log.append(f"✅ Found {count} vectors in 'direct_test' namespace")
        else:
            log.append("❌ 'direct_test' namespace not found in index")
    except Exception as e:
        log.append(f"❌ Failed to verify upload: {str(e)}")
    
    return log

# Run the test
if st.button("Run Direct Test"):
    with st.spinner("Running direct test..."):
        logs = run_direct_test()
    
    st.subheader("Test Results")
    for log_entry in logs:
        st.write(log_entry)
    
    st.subheader("Current Index Stats")
    try:
        pc = Pinecone(api_key=pinecone_api_key)
        index = pc.Index("zecompete")
        stats = index.describe_index_stats()
        
        # Display stats
        st.write(f"Dimension: {stats.get('dimension')}")
        st.write(f"Total vectors: {stats.get('total_vector_count')}")
        
        # Display namespaces
        namespaces = stats.get("namespaces", {})
        if namespaces:
            st.write("Namespaces:")
            for ns_name, ns_data in namespaces.items():
                st.write(f"- {ns_name}: {ns_data.get('vector_count')} vectors")
        else:
            st.write("No namespaces found")
    except Exception as e:
        st.error(f"Failed to get index stats: {str(e)}")
