import os, itertools, pandas as pd, streamlit as st
import time, json, threading, requests, secrets
from openai import OpenAI

# Set up the app
st.set_page_config(page_title="Competitor Mapper", layout="wide")
st.title("🗺️ Competitor Location & Demand Explorer")

# --- Helper function to safely clear a namespace ---
def safe_clear_namespace(index, namespace_name: str):
    """Clear a namespace from Pinecone safely if it exists."""
    try:
        namespaces = index.describe_index_stats().namespaces
        if namespace_name in namespaces:
            index.delete(delete_all=True, namespace=namespace_name)
            return True
        else:
            return False
    except Exception as e:
        print(f"Error clearing namespace {namespace_name}: {e}")
        return False

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
    from src.config import secret
    from pinecone import Pinecone
    pc = Pinecone(api_key=secret("PINECONE_API_KEY"))
    idx = pc.Index("zecompete")

    if st.button("🔄 Clear ALL Data from Pinecone", type="primary"):
        with st.spinner("Clearing all data from Pinecone..."):
            safe_clear_namespace(idx, "maps")
            safe_clear_namespace(idx, "keywords")
            st.success("✅ All data cleared from Pinecone successfully!")
            st.rerun()

    pinecone_success = True
    st.success("✅ Successfully connected to Pinecone!")
except Exception as e:
    st.error(f"Error initializing Pinecone: {str(e)}")
    pinecone_success = False
# Import required modules with error handling
import_success = True
all_modules_ok = True

try:
    from src.run_pipeline import run
    run_module_ok = True
except Exception as e:
    st.error(f"Error importing run_pipeline module: {str(e)}")
    run_module_ok = False
    all_modules_ok = False

try:
    from src.analytics import insight_question
    analytics_module_ok = True
except Exception as e:
    st.error(f"Error importing analytics module: {str(e)}")
    analytics_module_ok = False
    all_modules_ok = False

try:
    from src.embed_upsert import _embed, upsert_places
    embed_module_ok = True
except Exception as e:
    st.error(f"Error importing embed_upsert module: {str(e)}")
    embed_module_ok = False
    all_modules_ok = False

try:
    from src.scrape_maps import run_scrape, run_apify_task, check_task_status
    from src.task_manager import add_task, process_all_tasks, get_running_tasks, get_pending_tasks
    from src.webhook_handler import process_dataset_directly, create_apify_webhook
    scrape_module_ok = True
except Exception as e:
    st.error(f"Error importing scrape/task modules: {str(e)}")
    scrape_module_ok = False
    all_modules_ok = False

try:
    from src.keyword_pipeline import run_keyword_pipeline, get_business_names_from_pinecone
    keyword_module_ok = True
except Exception as e:
    st.error(f"Error importing keyword_pipeline module: {str(e)}")
    keyword_module_ok = False
    all_modules_ok = False

import_success = all_modules_ok

# API key checks
api_keys_ok = True
api_key_messages = []

try:
    pinecone_key = secret("PINECONE_API_KEY")
    if not pinecone_key:
        api_keys_ok = False
        api_key_messages.append("❌ Pinecone API key is missing")
    else:
        api_key_messages.append("✅ Pinecone API key is set")
except:
    api_keys_ok = False
    api_key_messages.append("❌ Pinecone API key is missing")

try:
    openai_key = secret("OPENAI_API_KEY")
    if not openai_key:
        api_keys_ok = False
        api_key_messages.append("❌ OpenAI API key is missing")
    else:
        api_key_messages.append("✅ OpenAI API key is set")
except:
    api_keys_ok = False
    api_key_messages.append("❌ OpenAI API key is missing")

try:
    apify_token = secret("APIFY_TOKEN")
    if not apify_token:
        api_key_messages.append("⚠️ Apify token is missing (needed for Google Maps scraping)")
    else:
        api_key_messages.append("✅ Apify token is set")
except:
    api_key_messages.append("⚠️ Apify token is missing (needed for Google Maps scraping)")

try:
    dfs_user = secret("DFS_USER")
    dfs_pass = secret("DFS_PASS")
    if not dfs_user or not dfs_pass:
        api_key_messages.append("⚠️ DataForSEO credentials are missing (search volume will be simulated)")
    else:
        api_key_messages.append("✅ DataForSEO credentials are set")
except:
    api_key_messages.append("⚠️ DataForSEO credentials are missing (search volume will be simulated)")

# Webhook secret helper
def get_webhook_secret():
    try:
        return secret("WEBHOOK_SECRET")
    except:
        if "webhook_secret" not in st.session_state:
            st.session_state.webhook_secret = secrets.token_hex(16)
        return st.session_state.webhook_secret

# Tabs
tabs = st.tabs([
    "Run Analysis", 
    "Auto Integration", 
    "Keywords & Search Volume", 
    "Manual Upload", 
    "Ask Questions", 
    "Explore Data", 
    "Diagnostic"
])
# -------------------
# Tab 1: Run Analysis
# -------------------
with tabs[0]:
    st.header("Run Analysis")

    with st.expander("API Key Status", expanded=not api_keys_ok):
        for msg in api_key_messages:
            st.write(msg)
        st.info("Add missing API keys in Streamlit secrets or .streamlit/secrets.toml file")

    if st.button("🔄 Clear Previous Data", key="clear_tab_data"):
        with st.spinner("Clearing all data from Pinecone..."):
            safe_clear_namespace(idx, "maps")
            safe_clear_namespace(idx, "keywords")
            st.success("✅ All data cleared!")
            st.cache_data.clear()

    brands = st.text_input("Brands (comma)", "Zudio, Max Fashion, Zara, H&M, Trends")
    cities = st.text_input("Cities (comma)", "Bengaluru, Hyderabad")

    col1, col2 = st.columns(2)

    with col1:
        if st.button("Run Manual Pipeline", key="run_manual"):
            if not run_module_ok:
                st.error("Run pipeline module not available.")
            else:
                log_container = st.container()
                log_container.subheader("Processing Logs")

                log_container.write("Clearing existing data...")
                safe_clear_namespace(idx, "maps")
                safe_clear_namespace(idx, "keywords")

                for b, c in itertools.product(
                        map(str.strip, brands.split(",")),
                        map(str.strip, cities.split(","))):
                    log_container.write(f"Processing {b} in {c}...")
                    try:
                        run(b, c)
                        log_container.success(f"✅ Completed {b} in {c}")
                    except Exception as e:
                        log_container.error(f"❌ Error processing {b} in {c}: {e}")
                        import traceback
                        log_container.code(traceback.format_exc())

                st.success("Data ready!")
                st.cache_data.clear()
                st.rerun()

    with col2:
        task_id = st.text_input("Apify Task ID", "zecodemedia~google-maps-scraper-task")
        if st.button("Run with Apify (Automated)", key="run_automated"):
            if not scrape_module_ok:
                st.error("Apify/scrape modules not available.")
            else:
                log_container = st.container()
                log_container.subheader("Automated Processing Logs")

                log_container.write("Clearing existing data...")
                safe_clear_namespace(idx, "maps")
                safe_clear_namespace(idx, "keywords")

                st.session_state.auto_refresh = True

                brand_list = [b.strip() for b in brands.split(",")]
                city_list = [c.strip() for c in cities.split(",")]

                for b, c in itertools.product(brand_list, city_list):
                    st.session_state.last_brand = b
                    st.session_state.last_city = c
                    run_id, _ = run_apify_task(b, c)
                    if run_id:
                        add_task(run_id, b, c)
                        log_container.success(f"✅ Started Apify task for {b} in {c}")

# --------------------------
# Tab 2: Auto Integration
# --------------------------
with tabs[1]:
    st.header("Auto Integration Setup")

    if st.button("🔄 Clear Previous Data", key="clear_auto_tab"):
        with st.spinner("Clearing all data from Pinecone..."):
            safe_clear_namespace(idx, "maps")
            safe_clear_namespace(idx, "keywords")
            st.success("✅ All data cleared!")

    app_url = st.text_input("Your app URL (for webhooks)", "https://zecompete-app.streamlit.app")
    callback_url = f"{app_url}/webhook"
    webhook_secret = get_webhook_secret()

    st.markdown("#### Webhook Configuration")
    st.code(f"URL: {callback_url}")
    st.code(f"Secret: {webhook_secret}")

    task_id = st.text_input("Apify Task ID for webhook", "zecodemedia~google-maps-scraper-task")
    if st.button("Set Up Webhook in Apify"):
        webhook_id = create_apify_webhook(task_id, callback_url)
        if webhook_id:
            st.success(f"✅ Webhook created: {webhook_id}")
        else:
            st.error("❌ Failed to create webhook.")

    dataset_id = st.text_input("Apify Dataset ID")
    col1, col2 = st.columns(2)
    with col1:
        brand = st.text_input("Brand", "Zecode")
    with col2:
        city = st.text_input("City", "Bengaluru")

    if st.button("Process Dataset") and dataset_id:
        with st.spinner(f"Processing dataset {dataset_id}..."):
            safe_clear_namespace(idx, "maps")
            safe_clear_namespace(idx, "keywords")
            success = process_dataset_directly(dataset_id, brand, city)
            if success:
                st.success(f"✅ Processed dataset for {brand} in {city}")
            else:
                st.error("❌ Failed to process dataset")
# -----------------------------------
# -----------------------------------
# Tab 3: Keywords & Search Volume
# -----------------------------------
with tabs[2]:
    st.header("Keywords & Search Volume Analysis")

    # Clear previous keywords button
    if st.button("🔄 Clear Previous Keywords", key="clear_kw_tab"):
        with st.spinner("Clearing keyword data..."):
            safe_clear_namespace(idx, "keywords")
            st.success("✅ Cleared keyword data.")

    # Input city name for keyword context
    city = st.text_input("City for keywords", "Bengaluru")

    # Check business names button
    if st.button("🔎 Check Business Names"):
        with st.spinner("Retrieving business names from Pinecone..."):
            try:
                business_names = get_business_names_from_pinecone()
                if business_names:
                    st.success(f"✅ Found {len(business_names)} business names.")
                    st.write(business_names[:10])  # Show sample
                else:
                    st.warning("⚠️ No business names found in Pinecone (maps namespace may be empty).")
            except Exception as e:
                st.error(f"❌ Error fetching business names: {e}")

    # Generate keywords button
    if st.button("🚀 Generate Keywords & Get Search Volume"):
        with st.spinner("Running keyword generation pipeline..."):
            try:
                run_keyword_pipeline(city)
                st.success("✅ Keyword pipeline completed!")
            except Exception as e:
                st.error(f"❌ Error running keyword pipeline: {e}")


# ---------------------
# Tab 4: Manual Upload
# ---------------------
with tabs[3]:
    st.header("Manual Upload (CSV)")

    if st.button("🔄 Clear Previous Upload Data", key="clear_upload_tab"):
        with st.spinner("Clearing uploaded data..."):
            safe_clear_namespace(idx, "maps")
            safe_clear_namespace(idx, "keywords")
            st.success("✅ Cleared previous upload data.")

    uploaded_file = st.file_uploader("Upload Apify CSV", type=["csv"])

    if uploaded_file is not None:
        df = pd.read_csv(uploaded_file)
        st.dataframe(df.head())

        brand = st.text_input("Brand Name", "Zecode")
        city = st.text_input("City", "Bengaluru")

        if st.button("Upload to Pinecone"):
            with st.spinner(f"Uploading data for {brand} in {city}..."):
                upsert_places(df, brand, city)
                st.success("✅ Upload completed!")

# ----------------------
# Tab 5: Ask Questions
# ----------------------
with tabs[4]:
    st.header("Ask Questions About Data")

    question = st.text_area("Your question")

    if st.button("Get Answer"):
        if question:
            try:
                answer = insight_question(question)
                st.success("✅ Answer:")
                st.write(answer)
            except Exception as e:
                st.error(f"Error answering question: {str(e)}")
# ------------------
# Tab 6: Explore Stored Data
# ------------------
with tabs[5]:
    st.header("Explore Stored Data")

    if st.button("🔄 Refresh Data View", key="refresh_explore"):
        st.cache_data.clear()
        st.rerun()

    if not pinecone_success:
        st.error("Pinecone not connected.")
    else:
        try:
            st.subheader("Pinecone Index Stats")

            # Safely load index stats
            stats = idx.describe_index_stats()

            # Try to show stats cleanly
            import json
            try:
                st.json(json.loads(json.dumps(stats)))  # Safe double-parse to handle bad src properties
            except Exception as e:
                st.warning(f"Could not render full index stats cleanly. ({e})")

            # Show available namespaces
            namespaces = stats.get("namespaces", {})
            if namespaces:
                for ns in namespaces:
                    with st.expander(f"Namespace: {ns}"):
                        dummy_vector = [0] * stats.get("dimension", 1536)
                        results = idx.query(vector=dummy_vector, top_k=10, namespace=ns, include_metadata=True)
                        if results.matches:
                            data = [match.metadata for match in results.matches if match.metadata]
                            if data:
                                df = pd.DataFrame(data)
                                st.dataframe(df)
                            else:
                                st.info("No metadata records found in this namespace.")
                        else:
                            st.info("No vectors found in this namespace.")
            else:
                st.info("No namespaces currently available.")
        except Exception as e:
            st.error(f"Error fetching Explore tab data: {e}")

# ------------------

# ---------------------
# Tab 7: Diagnostic
# ---------------------
with tabs[6]:
    st.header("Diagnostic Info")

    if st.button("🔄 Clear All Data", key="clear_diagnostic"):
        with st.spinner("Clearing all data..."):
            safe_clear_namespace(idx, "maps")
            safe_clear_namespace(idx, "keywords")
            st.success("✅ Cleared all data!")
            st.cache_data.clear()
            st.rerun()

    st.subheader("API Key Status")
    for msg in api_key_messages:
        st.write(msg)

    st.subheader("Module Import Status")
    st.write(f"Run Pipeline Module: {'✅' if run_module_ok else '❌'}")
    st.write(f"Analytics Module: {'✅' if analytics_module_ok else '❌'}")
    st.write(f"Embed Module: {'✅' if embed_module_ok else '❌'}")
    st.write(f"Scrape/Task Manager Module: {'✅' if scrape_module_ok else '❌'}")
    st.write(f"Keyword Pipeline Module: {'✅' if keyword_module_ok else '❌'}")

# Webhook Handler (for Apify)
st.markdown("---")
st.header("Webhook Handler (Testing Only)")

webhook_data = st.text_area("Paste webhook JSON payload here (testing only):", "", key="webhook_payload")

if st.button("Process Webhook Payload"):
    if webhook_data:
        try:
            with st.spinner("Processing webhook payload..."):
                payload = json.loads(webhook_data)
                process_dataset_directly(payload.get("resource", {}).get("defaultDatasetId", ""), 
                                         st.session_state.last_brand, 
                                         st.session_state.last_city)
                st.success("✅ Webhook processed successfully!")
        except Exception as e:
            st.error(f"Error processing webhook: {str(e)}")

# Footer
st.markdown("---")
st.caption("© 2025 Zecode - Competitor Mapper App")
