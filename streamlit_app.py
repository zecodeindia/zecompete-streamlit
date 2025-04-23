import os, itertools, pandas as pd, streamlit as st
import time, json, threading
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

# Tab 1: Run Analysis - Original functionality with automation options
with tabs[0]:
    st.header("Run Analysis")
    
    # Standard interface for brand/city search
    brands = st.text_input("Brands (comma)", "Zudio, Max Fashion, Zara, H&M, Trends")
    cities = st.text_input("Cities (comma)", "Bengaluru, Hyderabad")
    
    col1, col2 = st.columns(2)
    
    with col1:
        if st.button("Run Manual Pipeline", key="run_manual"):
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
    
    with col2:
        # Option to run with Apify automation
        task_id = st.text_input("Apify Task ID", "avadhut.sawant~google-maps-scraper-task")
        
        if st.button("Run with Apify (Automated)", key="run_automated"):
            log_container = st.container()
            log_container.subheader("Automated Processing Logs")
            
            # Enable auto-refresh to check for task completion
            st.session_state.auto_refresh = True
            
            # Process each brand and city
            brand_list = [b.strip() for b in brands.split(",")]
            city_list = [c.strip() for c in cities.split(",")]
            
            for b, c in itertools.product(brand_list, city_list):
                # Store current brand and city for webhook use
                st.session_state.last_brand = b
                st.session_state.last_city = c
                
                log_container.write(f"Starting Apify task for {b} in {c}...")
                
                # Run the Apify task
                run_id, _ = run_apify_task(b, c)
                
                if run_id:
                    # Add to task manager
                    add_task(run_id, b, c)
                    log_container.write(f"✅ Apify task started with run ID: {run_id}")
                else:
                    log_container.error(f"❌ Failed to start Apify task for {b} in {c}")
    
    # Display task status if auto-refresh is enabled
    if st.session_state.auto_refresh:
        st.subheader("Task Status")
        
        # Show a toggle to disable auto-refresh
        if st.button("Disable Auto-Refresh"):
            st.session_state.auto_refresh = False
            st.experimental_rerun()
        
        # Check if we should refresh
        current_time = time.time()
        if current_time - st.session_state.last_refresh > 15:  # Refresh every 15 seconds
            st.session_state.last_refresh = current_time
            
            with st.spinner("Checking task status..."):
                # Process any tasks that have completed
                processed = process_all_tasks()
                if processed > 0:
                    st.success(f"✅ Processed {processed} completed tasks")
        
        # Display running tasks
        running_tasks = get_running_tasks()
        pending_tasks = get_pending_tasks()
        
        if running_tasks:
            st.write("Running tasks:")
            for task in running_tasks:
                st.write(f"• {task['brand']} in {task['city']} (Run ID: {task['run_id']})")
        else:
            st.write("No running tasks")
            
        if pending_tasks:
            st.write("Pending tasks:")
            for task in pending_tasks:
                st.write(f"• {task['brand']} in {task['city']} (Run ID: {task['run_id']})")
