import os
import sys
import itertools
import pandas as pd
import streamlit as st
import time
import json

# Fix import issues by adding the current directory to the path
current_dir = os.path.dirname(os.path.abspath(__file__))
if current_dir not in sys.path:
    sys.path.append(current_dir)

# Now try the imports with explicit relative imports
try:
    from src.run_pipeline import run
    from src.analytics import insight_question
    import pinecone
except ImportError as e:
    st.error(f"Import error: {e}")
    st.stop()

st.set_page_config(page_title="Competitor Mapper", layout="wide")
st.title("🗺️  Competitor Location & Demand Explorer")

brands = st.text_input("Brands (comma)", "Zudio, Max Fashion, Zara, H&M, Trends")
cities = st.text_input("Cities (comma)", "Bengaluru, Hyderabad")

if st.button("Run analysis", key="run_analysis_button"):
    log_container = st.container()
    log_container.subheader("Processing Logs")
    
    # Add this note about potential fallback data
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

# Initialize Pinecone directly (old style API)
pinecone.init(api_key=st.secrets["PINECONE_API_KEY"], environment="us-east-1")
idx = pinecone.Index("zecompete")

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
        
        # Display system path for debugging
        st.subheader("System Information")
        st.write("Python Path:")
        st.code("\n".join(sys.path))
        
        st.write("Current Directory Structure:")
        try:
            files = os.listdir(current_dir)
            st.write(f"Main directory: {files}")
            if 'src' in files:
                src_files = os.listdir(os.path.join(current_dir, 'src'))
                st.write(f"src directory: {src_files}")
        except Exception as e:
            st.write(f"Error listing directory: {e}")
        
        namespaces = stats.get("namespaces", {})
        if namespaces:
            st.success(f"Found {len(namespaces)} namespaces: {', '.join(namespaces.keys())}")
            
            # Show sample data from each namespace
            for ns in namespaces:
                st.subheader(f"Sample data from '{ns}' namespace")
                try:
                    # Fetch a few vectors to verify content
                    query_response = idx.query(
                        vector=[0] * 1536,  # Dummy vector for metadata-only query
                        top_k=5,
                        namespace=ns,
                        include_metadata=True
                    )
                    
                    if query_response.matches:
                        st.write(f"Found {len(query_response.matches)} records")
                        for i, match in enumerate(query_response.matches):
                            st.write(f"Record {i+1}:")
                            # Display metadata as regular text 
                            for key, value in match.metadata.items():
                                st.write(f"{key}: {value}")
                            st.write("---")
                    else:
                        st.warning(f"No records found in namespace '{ns}'")
                except Exception as e:
                    st.error(f"Error querying namespace '{ns}': {str(e)}")
        else:
            st.warning("No namespaces found in the index. Data may not have been uploaded successfully.")
    except Exception as e:
        st.error(f"Error accessing Pinecone: {str(e)}")
