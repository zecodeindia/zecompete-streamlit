import os
import sys
import itertools
import pandas as pd
import streamlit as st
import time
import json

# Add the current directory to the Python path
current_dir = os.path.dirname(os.path.abspath(__file__))
if current_dir not in sys.path:
    sys.path.insert(0, current_dir)

# Create __init__.py if it doesn't exist
src_dir = os.path.join(current_dir, 'src')
init_file = os.path.join(src_dir, '__init__.py')
if not os.path.exists(init_file):
    with open(init_file, 'w') as f:
        f.write('# Auto-generated __init__.py file\n')

st.set_page_config(page_title="Competitor Mapper", layout="wide")
st.title("🗺️  Competitor Location & Demand Explorer")

# Show files in src directory
st.write("Files in src directory:", os.listdir(src_dir) if os.path.exists(src_dir) else "src directory not found")

# Try imports with error handling
try:
    from pinecone import Pinecone
    
    # Import the modules we need
    import src.config
    from src.config import secret
    
    # Initialize Pinecone with the new method
    pc = Pinecone(api_key=secret("PINECONE_API_KEY"))
    idx = pc.Index("zecompete")
    
    # Test if Pinecone connection works
    stats = idx.describe_index_stats()
    st.success("✅ Successfully connected to Pinecone!")
    
    # Now try importing the other modules
    import src.run_pipeline
    from src.run_pipeline import run
    import src.analytics
    from src.analytics import insight_question
    
    st.success("✅ Successfully imported all modules!")
    
    # Continue with the main app
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
            else:
                st.write("No namespaces found")
        except Exception as e:
            st.error(f"Error fetching index stats: {str(e)}")

    with tabs[2]:
        st.subheader("Diagnostic Information")
        st.write("System paths:", sys.path)
        st.write("Current directory:", current_dir)
        st.write("Src directory exists:", os.path.exists(src_dir))
        st.write("__init__.py exists:", os.path.exists(init_file))
        
except Exception as e:
    st.error(f"❌ Error: {str(e)}")
    st.info("Recommendation to fix the issue: Update all Pinecone initialization code to use the new API style shown in the error message.")
