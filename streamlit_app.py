import os, itertools, pandas as pd, streamlit as st
import time
from openai import OpenAI
from pinecone import Pinecone

st.set_page_config(page_title="Competitor Mapper", layout="wide")
st.title("🗺️  Competitor Location & Demand Explorer")

# Initialize connections
pc = Pinecone(api_key=st.secrets["PINECONE_API_KEY"])
index = pc.Index("zecompete")
openai_client = OpenAI(api_key=st.secrets["OPENAI_API_KEY"])

# Define helper functions
def get_embeddings(texts):
    """Generate embeddings for a list of texts"""
    response = openai_client.embeddings.create(
        model="text-embedding-3-small",
        input=texts
    )
    return [d.embedding for d in response.data]

def run_apify_scrape(brand, city):
    """Run Apify scraper and return results"""
    import requests
    
    apify_token = st.secrets["APIFY_TOKEN"]
    task_id = "avadhut.sawant~google-maps-scraper-task"  # Update with your task ID
    
    url = f"https://api.apify.com/v2/actor-tasks/{task_id}/run-sync-get-dataset-items"
    params = {"token": apify_token}
    payload = {
        "searchStringsArray": [brand],
        "locationQuery": city,
        "maxReviews": 0,
        "maxImages": 0,
        "maxItems": 20
    }
    
    try:
        response = requests.post(url, params=params, json=payload, timeout=300)
        if response.status_code == 200:
            return response.json()
        else:
            st.error(f"Apify request failed: {response.status_code}, {response.text}")
            return []
    except Exception as e:
        st.error(f"Error running Apify scrape: {str(e)}")
        return []

def run_analysis(brand, city, log_container):
    """Run the full analysis pipeline for a brand and city"""
    log_container.write(f"Processing {brand} in {city}...")
    
    # Step 1: Scrape places
    log_container.write("Step 1: Scraping places from Google Maps...")
    places = run_apify_scrape(brand, city)
    
    if not places:
        log_container.warning(f"No places found for {brand} in {city}")
        return
    
    log_container.write(f"Found {len(places)} places")
    
    # Step 2: Get place names for embeddings
    place_names = []
    for place in places:
        if 'name' in place:
            place_names.append(place['name'])
        elif 'title' in place:
            place_names.append(place['title'])
    
    if not place_names:
        log_container.warning("No place names found in data")
        return
    
    # Step 3: Generate embeddings
    log_container.write("Step 2: Generating embeddings...")
    embeddings = get_embeddings(place_names)
    log_container.write(f"Generated {len(embeddings)} embeddings")
    
    # Step 4: Store in Pinecone
    log_container.write("Step 3: Storing data in Pinecone...")
    records = []
    
    for i, (place, embedding) in enumerate(zip(places, embeddings)):
        # Create record ID
        record_id = f"place-{brand}-{city}-{i}"
        if 'placeId' in place:
            record_id = f"place-{place['placeId']}"
        
        # Create metadata
        metadata = {
            "brand": brand,
            "city": city,
            "name": place.get('name', place.get('title', f"{brand} location"))
        }
        
        # Add other fields if available
        try:
            if 'totalScore' in place:
                metadata['rating'] = float(place['totalScore'])
            elif 'rating' in place:
                metadata['rating'] = float(place['rating'])
                
            if 'reviewsCount' in place:
                metadata['reviews'] = int(place['reviewsCount'])
                
            if 'gpsCoordinates' in place and isinstance(place['gpsCoordinates'], dict):
                metadata['lat'] = place['gpsCoordinates'].get('lat')
                metadata['lng'] = place['gpsCoordinates'].get('lng')
        except:
            pass
            
        records.append((record_id, embedding, metadata))
    
    # Upsert to Pinecone
    result = index.upsert(vectors=records, namespace=f"places_{brand}_{city}")
    log_container.write(f"Stored {len(records)} places in Pinecone")
    
    # Step 5: Generate keywords
    log_container.write("Step 4: Generating search keywords...")
    prompt = ("Give comma‑separated search phrases (≤3 words) "
              "to find these stores:\n\n" + "\n".join(place_names))
    
    response = openai_client.chat.completions.create(
        model="gpt-4o-mini",
        messages=[{"role": "user", "content": prompt}],
        temperature=0.3
    )
    
    keywords = [kw.strip() for kw in response.choices[0].message.content.split(",") if kw.strip()]
    log_container.write(f"Generated {len(keywords)} keywords: {', '.join(keywords)}")
    
    # Step 6: Fetch search volumes
    # (This step can be added back later)
    
    log_container.write(f"✅ Analysis completed for {brand} in {city}")

# User interface
brands = st.text_input("Brands (comma)", "Zudio, Max Fashion, Zara, H&M, Trends")
cities = st.text_input("Cities (comma)", "Bengaluru, Hyderabad")

if st.button("Run analysis", key="run_analysis_button"):
    log_container = st.container()
    log_container.subheader("Processing Logs")
    
    for b, c in itertools.product(
            map(str.strip, brands.split(",")),
            map(str.strip, cities.split(","))):
        run_analysis(b, c, log_container)
    
    st.success("Data ready!")

# Create tabs
tabs = st.tabs(["Ask", "Explore Data", "Diagnostic"])

with tabs[0]:
    q = st.text_area("Ask a question about the data")
    if st.button("Answer", key="answer_button") and q:
        st.write("This feature will be implemented soon.")

with tabs[1]:
    st.subheader("Explore the data")
    
    # Get available namespaces
    try:
        stats = index.describe_index_stats()
        namespaces = list(stats.get("namespaces", {}).keys())
        
        selected_namespace = st.selectbox("Select namespace", namespaces if namespaces else ["No data"])
        
        if namespaces and selected_namespace in namespaces:
            st.write(f"Showing data from '{selected_namespace}'")
            
            # Query the namespace
            dummy_vector = [0.0] * stats.get('dimension', 1536)
            results = index.query(
                vector=dummy_vector,
                top_k=10,
                namespace=selected_namespace,
                include_metadata=True
            )
            
            if results.matches:
                for i, match in enumerate(results.matches):
                    st.write(f"Record {i+1}:")
                    st.write(f"ID: {match.id}")
                    
                    if match.metadata:
                        # Create a more visual display of metadata
                        cols = st.columns(3)
                        for j, (key, value) in enumerate(match.metadata.items()):
                            cols[j % 3].metric(key, value)
                    
                    st.write("---")
            else:
                st.write("No data found in this namespace")
    except Exception as e:
        st.error(f"Error exploring data: {str(e)}")

with tabs[2]:
    st.subheader("Diagnostic Information")
    
    if st.button("Refresh Stats", key="refresh_stats"):
        try:
            stats = index.describe_index_stats()
            
            st.write(f"Dimension: {stats.get('dimension')}")
            st.write(f"Total vectors: {stats.get('total_vector_count')}")
            st.write(f"Index fullness: {stats.get('index_fullness')}")
            
            namespaces = stats.get("namespaces", {})
            if namespaces:
                st.success(f"Found {len(namespaces)} namespaces")
                
                # Display namespaces and counts
                for ns_name, ns_data in namespaces.items():
                    st.write(f"• {ns_name}: {ns_data.get('vector_count', 0)} vectors")
            else:
                st.warning("No namespaces found in the index")
        except Exception as e:
            st.error(f"Error getting index stats: {str(e)}")
    
    # Test direct upload
    st.subheader("Test Direct Upload")
    test_text = st.text_input("Test text", "Test Store in Bengaluru")
    test_ns = st.text_input("Test namespace", "test_direct")
    
    if st.button("Upload Test", key="upload_test"):
        try:
            # Generate embedding
            embeddings = get_embeddings([test_text])
            
            # Upsert to Pinecone
            test_id = f"test-{int(time.time())}"
            result = index.upsert(
                vectors=[(test_id, embeddings[0], {"text": test_text})],
                namespace=test_ns
            )
            
            st.success(f"Test upload successful: {result}")
        except Exception as e:
            st.error(f"Test upload failed: {str(e)}")
