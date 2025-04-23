import os, itertools, pandas as pd, streamlit as st
import time, json, threading, requests
from pinecone import Pinecone
from openai import OpenAI

# Set up the app
st.set_page_config(page_title="Competitor Mapper", layout="wide")
st.title("🗺️ Competitor Location & Demand Explorer")

# Initialize session state for tracking task processing
if "auto_refresh" not in st.session_state:
    st.session_state.auto_refresh = False
if "last_refresh" not in st.session_state:
    st.session_state.last_refresh = time.time()
if "last_brand" not in st.session_state:
    st.session_state.last_brand = ""
if "last_city" not in st.session_state:
    st.session_state.last_city = ""

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
    
    # Import new modules for automation
    from src.scrape_maps import run_scrape, run_apify_task, check_task_status
    from src.task_manager import add_task, process_all_tasks, get_running_tasks, get_pending_tasks
    from src.webhook_handler import get_webhook_secret, process_dataset_directly, create_apify_webhook
    
    import_success = True
    st.success("✅ Successfully imported all modules!")
except Exception as e:
    st.error(f"Import error: {str(e)}")
    import_success = False

# Define tabs
tabs = st.tabs(["Run Analysis", "Auto Integration", "Manual Upload", "Ask Questions", "Explore Data", "Diagnostic"])

# [Previous tabs remain the same...]

# Tab 5: Explore Data - Updated with namespace deletion
with tabs[4]:
    st.header("Explore Stored Data")
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
                col1, col2, col3 = st.columns([3, 2, 1])
                
                with col1:
                    st.write(f"Namespace: {ns_name}")
                    st.write(f"Vector count: {ns_data.get('vector_count', 0)}")
                
                with col2:
                    # Button to view data from this namespace
                    if st.button(f"View data", key=f"view_{ns_name}"):
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
                
                with col3:
                    # Button to delete namespace
                    if st.button(f"Delete", key=f"delete_{ns_name}"):
                        try:
                            # Confirm deletion
                            confirmation = st.checkbox(f"Confirm delete namespace '{ns_name}'", key=f"confirm_delete_{ns_name}")
                            if confirmation:
                                # Delete all vectors in the namespace
                                idx.delete(delete_all=True, namespace=ns_name)
                                st.success(f"Successfully deleted namespace '{ns_name}'")
                                
                                # Refresh the page to show updated stats
                                st.experimental_rerun()
                        except Exception as e:
                            st.error(f"Error deleting namespace {ns_name}: {str(e)}")
                
                # Add a divider between namespaces
                st.markdown("---")
        else:
            st.write("No namespaces found")
    except Exception as e:
        st.error(f"Error fetching index stats: {str(e)}")

# [Rest of the code remains the same...]

# Footer
st.markdown("---")
st.write("© 2025 Zecode - Competitor Location & Demand Explorer")
