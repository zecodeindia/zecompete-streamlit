# src/fetch_volume.py
import os
import time
import logging
from typing import List, Dict, Any
import streamlit as st  # Add proper import here

import requests
from requests.adapters import HTTPAdapter, Retry

# ------------------------------------------------------------------
# 1️⃣  secrets come from env or st.secrets; never hard-code
def get_credentials():
    """Get DFS credentials from environment or streamlit secrets"""
    try:
        # Try to get from environment variables first
        dfs_user = os.getenv("DFS_USER")
        dfs_pass = os.getenv("DFS_PASS")
        
        # If not found, try streamlit secrets
        if not dfs_user or not dfs_pass:
            dfs_user = st.secrets.get("DFS_USER")
            dfs_pass = st.secrets.get("DFS_PASS")
            
        return dfs_user, dfs_pass
    except Exception as e:
        logging.error(f"Error getting DFS credentials: {str(e)}")
        return None, None

_ENDPOINT = (
    "https://api.dataforseo.com/v3/"
    "dataforseo_labs/google/historical_search_volume/live"
)

# ------------------------------------------------------------------
# 2️⃣  reusable session with back-off
_session = requests.Session()
_session.mount(
    "https://",
    HTTPAdapter(
        max_retries=Retry(
            total=4, backoff_factor=1.2,
            status_forcelist=[429, 500, 502, 503, 504],
        )
    ),
)
_session.headers.update({"Content-Type": "application/json"})

# ------------------------------------------------------------------
def fetch_volume(
    keywords: List[str],
    location_code: int = 2840,            # 🇮🇳 India
    language_code: str = "en",
    *,
    include_clickstream: bool = False,
    include_serp: bool = False,
) -> List[Dict[str, Any]]:
    """
    Return the `result` array from DataForSEO:
    one dict per keyword with 12-month `monthly_searches`, CPC, etc.
    """
    # Get credentials when the function is called, not at module level
    dfs_user, dfs_pass = get_credentials()
    
    if not dfs_user or not dfs_pass:
        raise ValueError("DataForSEO credentials not found. Please set DFS_USER and DFS_PASS in environment or secrets.")
    
    payload = [
        {
            "keywords": keywords,
            "location_code": location_code,
            "language_code": language_code,
            # optional flags straight from the docs
            "include_clickstream_data": include_clickstream,
            "include_serp_info": include_serp,
        }
    ]

    start = time.perf_counter()
    resp = _session.post(_ENDPOINT, json=payload, auth=(dfs_user, dfs_pass), timeout=60)
    resp.raise_for_status()

    logging.info(
        "DFS volume %s kw | %.2fs | x-ratelimit %s",
        len(keywords),
        time.perf_counter() - start,
        resp.headers.get("x-ratelimit-request-cost", "?"),
    )

    data = resp.json()
    if not data["tasks"] or not data["tasks"][0].get("result"):
        raise ValueError(f"DFS empty result – payload={payload!r}")

    return data["tasks"][0]["result"]
