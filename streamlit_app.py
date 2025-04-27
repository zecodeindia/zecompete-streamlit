# --- make local modules importable ---------------------------------
import os, sys
sys.path.insert(0, os.path.abspath(os.path.dirname(__file__)))
# -------------------------------------------------------------------

import streamlit as st
import pandas as pd
from pinecone import Pinecone
from openai import OpenAI
import time

# Import the core components
from business_keywords_tab import render_business_keywords_tab   
from openai_assistant_reporting import render_assistant_report_tab
from src.config import secret
from src.scrape_maps import run_scrape, run_apify_task
from src.task_manager import add_task, process_all_tasks, get_running_tasks
from src.webhook_handler import process_dataset_directly, create_apify_webhook
from src.keyword_pipeline import run_business_keyword_pipeline as run_enhanced_keyword_pipeline

# Basic app setup
st.set_page_config(page_title="Business Keywords & Reporting", layout="wide")
st.title("🔍 Business Keywords & Advanced Reporting")

# Initialize session state
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
    pc = Pinecone(api_key=secret("PINECONE_API_KEY"))
    idx = pc.Index("zecompete")
    st.success("✅ Connected to Pinecone!")
except Exception as e:
    st.error(f"Error connecting to Pinecone: {str(e)}")

# Create tabs
tabs = st.tabs(["Business Keywords", "Advanced Reporting", "Data Collection"])

# Render the Business Keywords tab
with tabs[0]:
    render_business_keywords_tab()

# Render the Advanced Reporting tab
with tabs[1]:
    render_assistant_report_tab()

# Add a simplified Data Collection tab for Apify integration
with tabs[2]:
    st.header("Data Collection via Apify")
    
    col1, col2 = st.columns(2)
    with col1:
        brand = st.text_input("Brand to search", "Zara")
    with col2:
        city = st.text_input("City to search", "Bengaluru")
    
    task_id = st.text_input("Apify Task ID", "zecodemedia~google-maps-scraper-task")
    
    if st.button("Run Apify Scraper"):
        with st.spinner(f"Starting Apify task to search for {brand} in {city}..."):
            st.session_state.last_brand = brand
            st.session_state.last_city = city
            run_id, _ = run_apify_task(brand, city)
            if run_id:
                add_task(run_id, brand, city)
                st.success(f"✅ Started Apify task. It will run in the background.")
                st.session_state.auto_refresh = True
    
    # Display running tasks
    st.subheader("Running Tasks")
    running_tasks = get_running_tasks()
    if running_tasks:
        for task in running_tasks:
            st.info(f"Task for {task['brand']} in {task['city']} is running...")
    else:
        st.write("No tasks currently running")
    
    # Process pending tasks
    if st.button("Process Completed Tasks"):
        with st.spinner("Processing completed tasks..."):
            processed = process_all_tasks()
            if processed > 0:
                st.success(f"✅ Processed {processed} completed tasks")
            else:
                st.info("No completed tasks to process")
    
    # Manual dataset processing
    st.subheader("Process Dataset Directly")
    dataset_id = st.text_input("Apify Dataset ID")
    if st.button("Process Dataset") and dataset_id:
        with st.spinner(f"Processing dataset {dataset_id}..."):
            success = process_dataset_directly(dataset_id, brand, city)
            if success:
                st.success(f"✅ Processed dataset for {brand} in {city}")
            else:
                st.error("❌ Failed to process dataset")

# Auto-refresh for task processing
if st.session_state.auto_refresh:
    # Only refresh every 30 seconds to avoid excessive processing
    if time.time() - st.session_state.last_refresh > 30:
        st.session_state.last_refresh = time.time()
        process_all_tasks()
        st.experimental_rerun()
