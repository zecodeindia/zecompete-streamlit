# === UPDATED enhanced_keyword_pipeline.py ===
"""
Enhanced Keyword Pipeline using Smart Brand+Location Refinement
"""
import os
import sys
import traceback
import logging
import streamlit as st
from typing import List, Dict, Any, Optional
import pandas as pd

# Import the existing keyword generation functions
from src.keyword_pipeline import (
    extract_businesses_from_pinecone,
    extract_location_from_business,
    extract_brand_name,
    generate_location_keywords,
    get_search_volumes,
    run_keyword_pipeline
)

# Import our new smart refiner
from src.openai_keyword_refiner import smart_batch_refine_keywords

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Fixed Assistant ID (for traceability)
FIXED_ASSISTANT_ID = "asst_aaWtxqys7xZZph6YQOSVP6Wk"

# === Main function ===

def run_enhanced_keyword_pipeline(city: str) -> bool:
    """
    Main function to run enhanced keyword generation with smart refinement.
    """
    try:
        # Step 1: Extract businesses from Pinecone
        businesses = extract_businesses_from_pinecone()
        if not businesses:
            logger.warning("No businesses found for keyword extraction.")
            return False

        brand_names = sorted(set(extract_brand_name(b.get("name", "")) for b in businesses))
        logger.info(f"Detected brands: {brand_names}")

        # Step 2: Smart refinement using business names + address + suggest fallback
        refined_keywords = smart_batch_refine_keywords(businesses, brand_names, city)

        if not refined_keywords:
            logger.error("Keyword refinement failed.")
            return False

        # Step 3: Fetch search volumes
        search_volume_df = get_search_volumes(refined_keywords)

        if search_volume_df.empty:
            logger.error("No search volume data retrieved.")
            return False

        # Step 4: Save results
        search_volume_df.to_csv("keyword_volumes.csv", index=False)
        logger.info(f"✅ Saved {len(search_volume_df)} refined keywords.")
        return True

    except Exception as e:
        logger.error(f"Error in enhanced keyword pipeline: {str(e)}")
        return False
