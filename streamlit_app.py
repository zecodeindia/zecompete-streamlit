import os
import sys
import itertools
import pandas as pd
import streamlit as st
import time
import json

# Add the src directory to the Python path if needed
try:
    import src.run_pipeline
    from src.run_pipeline import run
    from src.analytics import insight_question
    import_success = True
except ImportError as e:
    st.error(f"Import error: {str(e)}")
    # Try to resolve path issues
    current_dir = os.path.dirname(os.path.abspath(__file__))
    src_dir = os.path.join(current_dir, 'src')
    if os.path.exists(src_dir):
        sys.path.append(current_dir)
        try:
            import src.run_pipeline
            from src.run_pipeline import run
            from src.analytics import insight_question
            import_success = True
            st.success("Successfully imported modules after path correction")
        except ImportError as e2:
            st.error(f"Still facing import issues after path correction: {str(e2)}")
            import_success = False
    else:
        st.error(f"Could not find src directory at {src_dir}")
        import_success = False

# Initialize Pinecone
try:
    import pinecone
    pinecone.init(api_key=st.secrets["PINECONE_API_KEY"], environment="us-east-1")
    idx = pinecone.Index("zecompete")
    pinecone_success = True
except Exception as e:
    st.error(f"Error initializing Pinecone: {str(e)}")
    pinecone_success = False

st.set_page_config(page_title="Competitor Mapper", layout="wide")
st.title("🗺️  Competitor Location & Demand Explorer")

# Main app content - only show if imports were successful
if import_success:
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

    if pinecone_success:
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
            
            # Display the Python path
            st.write("Python system path:")
            st.write(sys.path)
            
            # Display the current directory structure
            st.write("Directory structure:")
            try:
                dirs = os.listdir(current_dir)
                st.write(f"Files in main directory: {dirs}")
                if os.path.exists(src_dir):
                    src_files = os.listdir(src_dir)
                    st.write(f"Files in src directory: {src_files}")
            except Exception as e:
                st.write(f"Error listing directory: {str(e)}")
            
            # Check Pinecone connection
            if pinecone_success:
                try:
                    stats = idx.describe_index_stats()
                    st.write(f"Pinecone index stats: {stats}")
                except Exception as e:
                    st.error(f"Error getting Pinecone stats: {str(e)}")
else:
    st.error("Application cannot run due to import errors. Please check the logs above.")
    st.info("You may need to check the directory structure and ensure all required files are present.")
