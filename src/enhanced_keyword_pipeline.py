# === FINAL enhanced_keyword_pipeline.py (Strict Business Name Locking Version) ===
"""
Enhanced Keyword Pipeline (Strict Business Name Version)
"""
import json
import logging
from typing import List, Dict
import pandas as pd
from src.openai_keyword_refiner import strict_lock_business_names
from src.fetch_volume import fetch_volume
from src.embed_upsert import upsert_keywords

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# === Core Public API ===

def generate_keywords_and_search_volume(business_entries: List[Dict[str, str]], brand_names: List[str], city: str) -> pd.DataFrame:
    """
    Full pipeline to:
    1. Lock keywords from business names
    2. Fetch search volume
    3. Return enriched dataframe
    """
    # Step 1: Generate keywords (strict locking)
    logger.info("🔵 Starting strict keyword locking...")
    keywords = strict_lock_business_names(business_entries)

    logger.info(f"✅ Locked {len(keywords)} clean keywords.")

    # Step 2: Fetch search volume using DataForSEO or similar
    logger.info("🔵 Fetching search volume data...")
    search_volume_df = fetch_volume(keywords, city)

    logger.info(f"✅ Fetched search volume for {len(search_volume_df)} keywords.")

    return search_volume_df

def push_keywords_to_pinecone(df: pd.DataFrame, brand: str, city: str):
    """
    Push keyword vectors to Pinecone
    """
    logger.info("🔵 Pushing keyword embeddings to Pinecone...")
    upsert_keywords(df, brand, city)
    logger.info("✅ Keywords successfully upserted to Pinecone.")

# === Example main run (for testing) ===
if __name__ == "__main__":
    business_entries = [
        {"name": "Zecode RR Nagar"},
        {"name": "Zecode HSR Layout"},
        {"name": "Zecode Vidyaranyapura"},
        {"name": "Zecode Kammanahalli"},
        {"name": "Zecode Hesarghatta Road"},
        {"name": "Zecode TC Palya Road"},
        {"name": "Zecode Basaveshwar Nagar"},
        {"name": "Zecode Yelahanka"},
        {"name": "Zecode Nagavara"},
        {"name": "Zecode Vignan Nagar"}
    ]

    brand_names = ["Zecode"]
    city = "Bengaluru"

    df = generate_keywords_and_search_volume(business_entries, brand_names, city)
    print(df.head())
