# Updated streamlit_app.py with better error handling for Preview tab
import os, itertools, pandas as pd, streamlit as st
import json
from src.run_pipeline import run
from src.analytics import insight_question
from pinecone import Pinecone  # Updated import

st.set_page_config(page_title="Competitor Mapper", layout="wide")
st.title("🗺️  Competitor Location & Demand Explorer")

brands = st.text_input("Brands (comma)", "Zudio, Max Fashion, Zara, H&M, Trends")
cities = st.text_input("Cities (comma)", "Bengaluru, Hyderabad")

if st.button("Run analysis"):
    for b, c in itertools.product(
            map(str.strip, brands.split(",")),
            map(str.strip, cities.split(","))):
        run(b, c)
    st.success("Data ready!")

# Updated Pinecone initialization
pc = Pinecone(api_key=st.secrets["PINECONE_API_KEY"])
idx = pc.Index("zecompete")

tabs = st.tabs(["Ask", "Preview", "Diagnostic"])

with tabs[0]:
    q = st.text_area("Ask a question about the data")
    if st.button("Answer") and q:
        try:
            answer = insight_question(q)
            st.write(answer)
        except Exception as e:
            st.error(f"Error: {str(e)}")
            st.write("Please try a simpler question or check the Diagnostic tab to verify data exists.")

with tabs[1]:
    try:
        res = idx.describe_index_stats()
        # Instead of using st.json directly, format the data first
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
        st.write("Details:", repr(e))

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
                        vector=[0] * 1536,  # Dummy vector for metadata-only query
                        top_k=5,
                        namespace=ns,
                        include_metadata=True
                    )
                    
                    if query_response.matches:
                        st.write(f"Found {len(query_response.matches)} records")
                        for i, match in enumerate(query_response.matches):
                            st.write(f"Record {i+1}:")
                            # Display metadata as regular text to avoid JSON formatting issues
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

# Update in streamlit_app.py - add a logging area in the "Run analysis" section
if st.button("Run analysis"):
    log_container = st.container()
    log_container.subheader("Processing Logs")
    
    for b, c in itertools.product(
            map(str.strip, brands.split(",")),
            map(str.strip, cities.split(","))):
        log_container.write(f"Processing {b} in {c}...")
        run(b, c)
    
    st.success("Data ready!")
