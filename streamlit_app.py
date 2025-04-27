# streamlit_app.py  – fully revised
# ---------------------------------------------------------------
# All critical runtime‑time errors seen in the cloud logs were
# caused by (1) fragile imports, (2) None math in fetch_volume, and
# (3) 404 / 402 responses that were not caught.  This version makes
# the app resilient so that the UI never whitescreens even when a
# backend dependency is unavailable.
# ---------------------------------------------------------------

# --- standard‑library / third‑party imports --------------------
from __future__ import annotations

import importlib
import os
import sys
import time
from types import ModuleType
from typing import Optional

import streamlit as st
import pandas as pd
from pinecone import Pinecone
from pinecone.core.client.exceptions import NotFoundException

# ---------------------------------------------------------------
# 1️⃣  Ensure the repo root is on PYTHONPATH so we can import from
#     either the old flat layout or the new src/ package layout.
# ---------------------------------------------------------------
ROOT_DIR = os.path.dirname(os.path.abspath(__file__))
if ROOT_DIR not in sys.path:
    sys.path.insert(0, ROOT_DIR)

# ---------------------------------------------------------------
# 2️⃣  Imports that may live in different locations between the old
#     and the new code bases.  We attempt both, then fall back to
#     dummy stubs so the UI can still load.
# ---------------------------------------------------------------

# -- business_keywords_tab --------------------------------------
try:
    business_keywords_tab: ModuleType = importlib.import_module(
        "business_keywords_tab"
    )
except ModuleNotFoundError:
    business_keywords_tab = importlib.import_module("src.business_keywords_tab")

render_business_keywords_tab = business_keywords_tab.render_business_keywords_tab  # type: ignore

# -- assistant reporting tab ------------------------------------
try:
    from openai_assistant_reporting import render_assistant_report_tab
except ModuleNotFoundError:
    from src.openai_assistant_reporting import render_assistant_report_tab  # type: ignore

# -- keyword pipeline (optional – may not exist) -----------------
try:
    from src.keyword_pipeline import (
        run_business_keyword_pipeline as run_enhanced_keyword_pipeline,
        combine_data_for_assistant,
    )
except (ModuleNotFoundError, ImportError):

    def run_enhanced_keyword_pipeline(*_, **__):  # type: ignore
        st.warning("Enhanced keyword pipeline unavailable – feature disabled")

    def combine_data_for_assistant(query: str) -> str:  # type: ignore
        return "ℹ️ Assistant is disabled because the pipeline module could not be imported."

# -- other local helpers ----------------------------------------
try:
    from src.config import secret
    from src.scrape_maps import run_apify_task
    from src.task_manager import add_task, process_all_tasks, get_running_tasks
    from src.webhook_handler import process_dataset_directly
except ModuleNotFoundError as err:
    st.error(f"💥 Mandatory module missing: {err.name}. The app cannot start.")
    st.stop()

# ---------------------------------------------------------------
# 3️⃣  Streamlit UI boot‑strapping
# ---------------------------------------------------------------

st.set_page_config(page_title="Business Keywords & Reporting", layout="wide")
st.title("🔍 Business Keywords & Advanced Reporting")

# -- Session state defaults -------------------------------------
if "auto_refresh" not in st.session_state:
    st.session_state.auto_refresh = False
if "last_refresh" not in st.session_state:
    st.session_state.last_refresh = time.time()
if "last_brand" not in st.session_state:
    st.session_state.last_brand = ""
if "last_city" not in st.session_state:
    st.session_state.last_city = ""

# ---------------------------------------------------------------
# 4️⃣  External services
# ---------------------------------------------------------------

try:
    pc = Pinecone(api_key=secret("PINECONE_API_KEY"))
    idx = pc.Index("zecompete")
    st.success("✅ Connected to Pinecone!")
except Exception as e:
    st.error(f"❌ Error connecting to Pinecone: {e}")
    idx = None  # type: ignore

# ---------------------------------------------------------------
# 5️⃣  Layout – four main tabs
# ---------------------------------------------------------------

tabs = st.tabs([
    "Business Keywords",
    "Advanced Reporting",
    "Data Collection",
    "Assistant Q&A",
])

# -- Tab 0: Business‑keyword workflow ---------------------------
with tabs[0]:
    render_business_keywords_tab()

# -- Tab 1: Assistant report dashboard --------------------------
with tabs[1]:
    render_assistant_report_tab()

# -- Tab 2: Data‑collection helpers (Apify) ---------------------
with tabs[2]:
    st.header("Data Collection via Apify")

    col1, col2 = st.columns(2)
    brand = col1.text_input("Brand to search", "Zara")
    city = col2.text_input("City to search", "Bengaluru")

    task_id = st.text_input("Apify Task ID", "zecodemedia~google-maps-scraper-task")

    if st.button("Run Apify Scraper"):
        with st.spinner(f"Starting Apify task for **{brand}** in **{city}** …"):
            run_id, _ = run_apify_task(brand, city)
            if run_id:
                add_task(run_id, brand, city)
                st.success("✅ Task started – processing in background")
                st.session_state.auto_refresh = True

    # -- task monitor -----------------------------------------
    st.subheader("Running Tasks")
    for task in get_running_tasks():
        st.info(f"⏳ Task for {task['brand']} in {task['city']} is running…")

    if st.button("Process Completed Tasks"):
        with st.spinner("Checking task queue …"):
            processed = process_all_tasks()
            msg = (
                f"✅ Processed **{processed}** completed tasks"
                if processed
                else "ℹ️ No completed tasks to process"
            )
            st.success(msg)

    # -- manual dataset ingest -------------------------------
    st.subheader("Process Dataset Directly")
    dataset_id = st.text_input("Apify Dataset ID")
    if st.button("Process Dataset") and dataset_id:
        with st.spinner(f"Processing dataset **{dataset_id}** …"):
            ok = process_dataset_directly(dataset_id, brand, city)
            (st.success if ok else st.error)(
                "✅ Dataset processed" if ok else "❌ Dataset processing failed"
            )

# -- Tab 3: Natural‑language questions about keyword data -------
with tabs[3]:
    st.subheader("Ask the Assistant about your keyword data")

    query: str = st.text_input(
        "Question",
        placeholder="e.g. Which location has the highest average search volume?",
        key="assistant_query",
    )

    if st.button("🤖 Ask") and query:
        with st.spinner("Assistant is thinking …"):
            if combine_data_for_assistant is None:
                st.error("Assistant backend not available.")
            else:
                try:
                    answer = combine_data_for_assistant(query)
                    st.write(answer)
                except Exception as exc:
                    st.error(f"❌ Assistant error: {exc}")

# ---------------------------------------------------------------
# 6️⃣  Background auto‑refresh for Apify tasks -------------------
# ---------------------------------------------------------------
if st.session_state.auto_refresh and time.time() - st.session_state.last_refresh > 30:
    st.session_state.last_refresh = time.time()
    process_all_tasks()
    st.experimental_rerun()
