import os, itertools, pandas as pd, streamlit as st
import time, json
from pinecone import Pinecone
from openai import OpenAI

# Set up the app
st.set_page_config(page_title="Competitor Mapper", layout="wide")
st.title("🗺️  Competitor Location & Demand Explorer")

# Initialize Pinecone
try:
    from src.config import secret
    pc = Pinecone(api_key=secret("PINECONE_API_KEY"))
    idx = pc.Index("zecompete")
    pinecone_success = True
    st.success("✅ Successfully connected to Pinecone!")
except Exception as e:
    st.error(f"Error initializing Pinecone: {str(e)}")
    pinecone_success = False

# Import the modules we need
try:
    from src.run_pipeline import run
    from src.analytics import insight_question
    from src.embed_upsert import _embed, upsert_places
    import_success = True
    st.success("✅ Successfully imported all modules!")
except Exception as e:
    st.error(f"Import error: {str(e)}")
    import_success = False

# Option to upload Apify CSV directly
st.subheader("Upload Apify CSV (Optional)")
uploaded_file = st.file_uploader("Upload the Apify CSV file", type="csv")

if uploaded_file is not None:
    st.write("Processing uploaded CSV file...")
    df = pd.read_csv(uploaded_file)
    st.write(f"CSV contains {len(df)} rows and {len(df.columns)} columns")
    
    # Sample of the data
    st.write("Sample of the data:")
    st.dataframe(df.head(3))
    
    # Upload to Pinecone button
    if st.button("Upload CSV data to Pinecone"):
        if pinecone_success and import_success:
            try:
                # Normalize the data for Pinecone
                normalized_df = pd.DataFrame()
                
                # Map columns to expected format
                if 'title' in df.columns:
                    normalized_df['name'] = df['title']
                
                if 'placeId' in df.columns:
                    normalized_df['placeId'] = df['placeId']
                
                if 'totalScore' in df.columns:
                    normalized_df['totalScore'] = df['totalScore']
                
                if 'reviewsCount' in df.columns:
                    normalized_df['reviewsCount'] = df['reviewsCount']
                
                if 'address' in df.columns:
                    normalized_df['address'] = df['address']
                
                if 'city' in df.columns:
                    normalized_df['city'] = df['city']
                else:
                    normalized_df['city'] = "Bengaluru"  # Default city
                
                # Extract brand from searchString if available
                if 'searchString' in df.columns:
                    brand = df['searchString'].iloc[0] if not df.empty else "Unknown"
                else:
                    brand = "Zecode"  # Default brand
                
                # Extract GPS coordinates
                if 'location/lat' in df.columns and 'location/lng' in df.columns:
                    # Create a new column for GPS coordinates
                    normalized_df['gpsCoordinates'] = df.apply(
                        lambda row: {
                            'lat': row['location/lat'], 
                            'lng': row['location/lng']
                        } if pd.notna(row['location/lat']) and pd.notna(row['location/lng']) else None, 
                        axis=1
                    )
                
                st.write("Uploading to Pinecone...")
                # Generate embeddings and upload to Pinecone
                upsert_places(df, brand, "Bengaluru")
                st.success("✅ CSV data uploaded to Pinecone successfully!")
            except Exception as e:
                st.error(f"Error uploading to Pinecone: {str(e)}")
        else:
            st.error("Cannot upload to Pinecone due to connection or import issues")

# Standard interface for brand/city search
brands = st.text_input("Brands (comma)", "Zudio, Max Fashion, Zara, H&M, Trends")
cities = st.text_input("Cities (comma)", "Bengaluru, Hyderabad")

if st.button("Run analysis", key="run_analysis_button"):
    log_container = st.container()
    log_container.subheader("Processing Logs")
    
    st.info("Note: If Google Maps data can't be accessed via Apify, the app will create sample data to demonstrate functionality.")
    
    for b, c in itertools.product(
            map(str.strip, brands.split(",")),
            map(str.strip, cities.split(","))):
        log_container.write(f"Processing {b} in {c}...")
        try:
            run(b, c)
            log_container.write(f"✅ Completed processing {b} in {c}")
        except Exception as e:
            log_container.error(f"❌ Error processing {b} in {c}: {str(e)}")
    
    st.success("Data ready!")

tabs = st.tabs(["Ask", "Explore Data", "Diagnostic"])

with tabs[0]:
    q = st.text_area("Ask a question about the data")
    if st.button("Answer", key="answer_button") and q:
        try:
            answer = insight_question(q)
            st.write(answer)
        except Exception as e:
            st.error(f"Error: {str(e)}")
            st.write("Please try a simpler question or check the Diagnostic tab to verify data exists.")

with tabs[1]:
    try:
        res = idx.describe_index_stats()
        
        # Display as text, not JSON
        st.subheader("Index Statistics")
        st.write("Dimension:", res.get("dimension", "N/A"))
        st.write("Total vector count:", res.get("total_vector_count", 0))
        st.write("Index fullness:", res.get("index_fullness", 0))
        
        # Handle namespaces specifically
        st.subheader("Namespaces")
        namespaces = res.get("namespaces", {})
        if namespaces:
            for ns_name, ns_data in namespaces.items():
                st.write(f"Namespace: {ns_name}")
                st.write(f"Vector count: {ns_data.get('vector_count', 0)}")
                
                # Add button to view data from this namespace
                if st.button(f"View data from {ns_name}", key=f"view_{ns_name}"):
                    try:
                        # Create a dummy vector for search
                        dummy_vector = [0] * res.get("dimension", 1536)
                        
                        # Query to get records
                        results = idx.query(
                            vector=dummy_vector,
                            top_k=10,
                            namespace=ns_name,
                            include_metadata=True
                        )
                        
                        # Display results in a table
                        if results.matches:
                            # Extract metadata
                            data = []
                            for match in results.matches:
                                if match.metadata:
                                    data.append(match.metadata)
                            
                            if data:
                                df = pd.DataFrame(data)
                                st.dataframe(df)
                            else:
                                st.write("No metadata available for these records")
                        else:
                            st.write("No records found")
                    except Exception as e:
                        st.error(f"Error retrieving data: {str(e)}")
        else:
            st.write("No namespaces found")
    except Exception as e:
        st.error(f"Error fetching index stats: {str(e)}")

with tabs[2]:
    st.subheader("Diagnostic Information")
    
    # Check namespaces and count
    try:
        stats = idx.describe_index_stats()
        
        # Display basic stats as text
        st.write(f"Dimension: {stats.get('dimension')}")
        st.write(f"Total vectors: {stats.get('total_vector_count')}")
        st.write(f"Index fullness: {stats.get('index_fullness')}")
        
        namespaces = stats.get("namespaces", {})
        if namespaces:
            st.success(f"Found {len(namespaces)} namespaces: {', '.join(namespaces.keys())}")
            
            # Show sample data from each namespace
            for ns in namespaces:
                st.subheader(f"Sample data from '{ns}' namespace")
                try:
                    # Fetch a few vectors to verify content
                    query_response = idx.query(
                        vector=[0] * stats.get("dimension", 1536),  # Dummy vector for metadata-only query
                        top_k=5,
                        namespace=ns,
                        include_metadata=True
                    )
                    
                    if query_response.matches:
                        st.write(f"Found {len(query_response.matches)} records")
                        # Convert to DataFrame for better display
                        data = []
                        for match in query_response.matches:
                            if match.metadata:
                                data.append(match.metadata)
                        
                        if data:
                            df = pd.DataFrame(data)
                            st.dataframe(df)
                        else:
                            st.write("No metadata available for these records")
                    else:
                        st.warning(f"No records found in namespace '{ns}'")
                except Exception as e:
                    st.error(f"Error querying namespace '{ns}': {str(e)}")
        else:
            st.warning("No namespaces found in the index. Data may not have been uploaded successfully.")
    except Exception as e:
        st.error(f"Error accessing Pinecone: {str(e)}")
