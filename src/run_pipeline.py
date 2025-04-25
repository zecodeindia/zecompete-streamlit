# =============================================================================
# run_pipeline.py – Revised 25‑Apr‑2025
# =============================================================================
"""End‑to‑end competitor analysis: maps ➜ keywords ➜ volume ➜ Pinecone."""

from __future__ import annotations

import logging
from typing import List

import pandas as pd
from pinecone import Pinecone

from src.config import secret
from src.scrape_maps import run_scrape     # unchanged helper
from src.embed_upsert import upsert_places, upsert_keywords
from src.keyword_pipeline import generate_keywords_for_businesses, get_search_volumes

# -----------------------------------------------------------------------------
# Helpers
# -----------------------------------------------------------------------------

def _clean_places(raw: list[dict]) -> pd.DataFrame:
    df = pd.json_normalize(raw)
    keep_cols = [c for c in ["name", "title", "lat", "lng", "rating", "reviews"] if c in df.columns]
    return df[keep_cols].drop_duplicates("name").reset_index(drop=True)

# -----------------------------------------------------------------------------
# Main orchestrator
# -----------------------------------------------------------------------------

def run(brand: str, city: str) -> None:
    logging.info("=== Pipeline start ▸ %s | %s", brand, city)

    # Clear existing data in Pinecone
    try:
        logging.info(f"Initializing Pinecone and clearing previous data...")
        pc = Pinecone(api_key=secret("PINECONE_API_KEY"))
        index = pc.Index("zecompete")
        
        # Clear data in maps namespace 
        logging.info(f"Clearing existing data for maps namespace...")
        index.delete(delete_all=True, namespace="maps")
        logging.info("✓ Successfully cleared previous maps data")
        
        # Clear data in keywords namespace
        logging.info(f"Clearing existing data for keywords namespace...")
        index.delete(delete_all=True, namespace="keywords")
        logging.info("✓ Successfully cleared previous keywords data")
    except Exception as e:
        logging.warning(f"Warning: Could not clear previous data: {str(e)}")

    # 1️⃣  Scrape Google Maps via Apify
    logging.info(f"Scraping Google Maps data for {brand} in {city}...")
    places_raw = run_scrape(brand, city)
    if not places_raw:
        logging.warning("No places returned — aborting.")
        return

    df_places = _clean_places(places_raw)
    logging.info(f"Found {len(df_places)} unique places for {brand} in {city}")
    
    # Upload places data to Pinecone
    logging.info(f"Upserting places data to Pinecone...")
    upsert_places(df_places, brand, city)
    logging.info("✓ Places data uploaded to Pinecone")

    # 2️⃣  Keyword generation
    logging.info(f"Generating keywords for {brand} in {city}...")
    name_col = "name" if "name" in df_places.columns else df_places.columns[0]
    keywords: List[str] = generate_keywords_for_businesses(df_places[name_col].tolist(), city)
    if not keywords:
        logging.warning("Keyword generation produced zero rows; aborting.")
        return
    logging.info(f"Generated {len(keywords)} keywords")

    # 3️⃣  Search volume fetch + tidy
    logging.info(f"Fetching search volume data for {len(keywords)} keywords...")
    df_kw = get_search_volumes(keywords)
    df_kw = df_kw.assign(city=city).query("search_volume > 0")
    logging.info(f"Retrieved search volume data for {len(df_kw)} keyword-month combinations")

    # 4️⃣  Upsert keyword data
    logging.info(f"Upserting keyword data to Pinecone...")
    upsert_keywords(df_kw, city)
    logging.info("✓ Keyword data uploaded to Pinecone")

    logging.info(
        "Pipeline finished ✓  %s places → %s keywords (%s volume rows)",
        len(df_places), len(keywords), len(df_kw)
    )

# -----------------------------------------------------------------------------
if __name__ == "__main__":
    import argparse, sys
    logging.basicConfig(level=logging.INFO, format="%(levelname)s | %(message)s")

    p = argparse.ArgumentParser()
    p.add_argument("brand")
    p.add_argument("city")
    args = p.parse_args()

    run(args.brand, args.city)
