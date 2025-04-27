# streamlit_app.py
# -----------------------------------------------------------------------------#
#  Business-keyword demo – Streamlit entry point                               #
# -----------------------------------------------------------------------------#
import importlib
import os
import sys
import time

# ――― make sure the repo root is on sys.path ――――――――――――――――――――――――――――――――
ROOT_DIR = os.path.dirname(os.path.abspath(__file__))
if ROOT_DIR not in sys.path:
    sys.path.insert(0, ROOT_DIR)

# third-party
import streamlit as st
import pandas as pd
from pinecone import Pinecone
from openai import OpenAI

# -----------------------------------------------------------------------------#
#  Resilient dynamic imports (root vs. src package)                            #
# -----------------------------------------------------------------------------#
# business_keywords_tab --------------------------------------------------------
try:
    business_keywords_tab = importlib.import_module("business_keywords_tab")
except ModuleNotFoundError:
    business_keywords_tab = importlib.import_module("src.business_keywords_tab")
render_business_keywords_tab = business_keywords_tab.render_business_keywords_tab

# openai_assistant_reporting ---------------------------------------------------
try:
    oai_reporting = importlib.import_module("openai_assistant_reporting")
except ModuleNotFoundError:
    oai_reporting = importlib.import_module("src.openai_assistant_reporting")
render_assistant_report_tab = oai_reporting.render_assistant_report_tab

# internal helpers -------------------------------------------------------------
from src.config import secret
from src.scrape_maps import run_scrape, run_apify_task
from src.task_manager import add_task, process_all_tasks, get_running_tasks
from src.webhook_handler import process_dataset_directly, create_apify_webhook
from src.keyword_pipeline import (
    run_business_keyword_pipeline as run_enhanced_keyword_pipeline,
    combine_data_for_assistant,
)

# -----------------------------------------------------------------------------#
#  Streamlit page config                                                       #
# -----------------------------------------------------------------------------#
st.set_page_config(page_title="Business Keywords & Reporting", layout="wide")
st.title("🔍 Business Keywords & Advanced Reporting")

# Session-state defaults -------------------------------------------------------
if "auto_refresh" not in st.session_state:
    st.session_state.auto_refresh = False
if "last_refresh" not in st.session_state:
    st.session_state.last_refresh = time.time()
if "last_brand" not in st.session_state:
    st.session_state.last_brand = ""
if "last_city" not in st.session_state:
    st.session_state.last_city = ""

# -----------------------------------------------------------------------------#
#  Pinecone connection                                                         #
# -----------------------------------------------------------------------------#
try:
    pc = Pinecone(api_key=secret("PINECONE_API_KEY"))
    idx = pc.Index("zecompete")
    st.success("✅ Connected to Pinecone!")
except Exception as e:
    st.error(f"Error connecting to Pinecone: {e}")

# -----------------------------------------------------------------------------#
#  Tabs                                                                         #
# -----------------------------------------------------------------------------#
tabs = st.tabs(
    [
        "Business Keywords",   # 0
        "Advanced Reporting",  # 1
        "Data Collection",     # 2
        "Assistant Q&A",       # 3
    ]
)

# ── Tab 0: Keyword pipeline ───────────────────────────────────────────────────
with tabs[0]:
    render_business_keywords_tab()

# ── Tab 1: Assistant reporting (charts etc.) ──────────────────────────────────
with tabs[1]:
    render_assistant_report_tab()

# ── Tab 2: Apify data-collection helper ───────────────────────────────────────
with tabs[2]:
    st.header("Data Collection via Apify")

    col1, col2 = st.columns(2)
    with col1:
        brand = st.text_input("Brand to search", "Zara")
    with col2:
        city = st.text_input("City to search", "Bengaluru")

    task_id = st.text_input(
        "Apify Task ID",
        "zecodemedia~google-maps-scraper-task",
        help="Actor-task in the form <username>~<task-slug>",
    )

    if st.button("Run Apify Scraper"):
        with st.spinner(f"Starting Apify task to search for {brand} in {city}…"):
            st.session_state.last_brand = brand
            st.session_state.last_city = city
            run_id, _ = run_apify_task(brand, city)
            if run_id:
                add_task(run_id, brand, city)
                st.success("✅ Scraper started in the background.")
                st.session_state.auto_refresh = True

    # running tasks -----------------------------------------------------------
    st.subheader("Running Tasks")
    running_tasks = get_running_tasks()
    if running_tasks:
        for task in running_tasks:
            st.info(f"Task for {task['brand']} in {task['city']} is running…")
    else:
        st.write("No tasks currently running.")

    # manual processing -------------------------------------------------------
    if st.button("Process Completed Tasks"):
        with st.spinner("Processing completed tasks…"):
            processed = process_all_tasks()
            if processed:
                st.success(f"✅ Processed {processed} completed task(s).")
            else:
                st.info("No completed tasks to process.")

    # dataset-id processing ----------------------------------------------------
    st.subheader("Process Dataset Directly")
    dataset_id = st.text_input("Apify Dataset ID")
    if st.button("Process Dataset") and dataset_id:
        with st.spinner(f"Processing dataset {dataset_id}…"):
            success = process_dataset_directly(dataset_id, brand, city)
            st.success("✅ Dataset processed.") if success else st.error("❌ Failed.")

# ── Tab 3: Natural-language Q&A over keyword data ─────────────────────────────
with tabs[3]:
    st.subheader("Combined Data for OpenAI Assistant")

    query = st.text_input(
        "Ask a question about your keyword data",
        key="bk_nl_query",
        placeholder="e.g. Which location has the highest average search volume?",
    )

    if st.button("🤖 Ask Assistant") and query:
        with st.spinner("Thinking…"):
            try:
                answer = combine_data_for_assistant(query)
                st.write(answer)
            except Exception as e:
                st.error(f"❌ Assistant error: {e}")

# -----------------------------------------------------------------------------#
#  Background auto-refresh to poll Apify tasks                                  #
# -----------------------------------------------------------------------------#
if st.session_state.auto_refresh:
    if time.time() - st.session_state.last_refresh > 30:
        st.session_state.last_refresh = time.time()
        process_all_tasks()
        st.experimental_rerun()
