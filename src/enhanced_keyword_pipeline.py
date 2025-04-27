# src/enhanced_keyword_pipeline.py
"""
Enhanced keyword pipeline with OpenAI Assistant integration.
This module extends the existing keyword pipeline with advanced keyword refinement capabilities.
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

# Import our new keyword refiner
from src.openai_keyword_refiner import batch_refine_keywords

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def generate_keyword_suggestions(business_names: List[str], city: str) -> List[str]:
    """
    Generate keyword suggestions from business names
    """
    suggested_keywords = []
    
    # Get brand names
    brand_names = [extract_brand_name(name) for name in business_names if name]
    brand_names = [name for name in brand_names if name]
    
    # For each business location
    for name in business_names:
        if not name:
            continue
            
        brand = extract_brand_name(name)
        location = extract_location_from_business({"name": name})
        
        if not brand:
            continue
            
        # Generate variations - simple examples
        suggested_keywords.append(f"{brand} {location}" if location else f"{brand} {city}")
        suggested_keywords.append(f"{brand} in {city}")
        
        # Add location variations if we have them
        if location:
            suggested_keywords.append(f"{brand} {location} {city}")
            suggested_keywords.append(f"{brand} in {location}")
            suggested_keywords.append(f"{brand} {location} branch")
            
    # Remove duplicates and empty entries
    suggested_keywords = [kw for kw in suggested_keywords if kw and len(kw) > 5]
    suggested_keywords = list(set(suggested_keywords))
    
    return suggested_keywords

def run_enhanced_keyword_pipeline(city: str = "Bengaluru", refine_keywords: bool = True) -> bool:
    """
    Run the enhanced keyword pipeline with OpenAI Assistant-based refinement.
    
    Args:
        city: Target city for keywords
        refine_keywords: Whether to run the refinement step
        
    Returns:
        True if successful, False otherwise
    """
    try:
        logger.info(f"Starting enhanced keyword pipeline for {city}")
        
        # Extract business data from Pinecone
        businesses = extract_businesses_from_pinecone()
        
        if not businesses:
            logger.warning("No business data found in Pinecone")
            if 'st' in globals():
                st.warning("⚠️ No business data found in Pinecone. Please run data scraping first.")
            return False
        
        # Add city to businesses without one
        for business in businesses:
            if not business.get("city"):
                business["city"] = city
        
        # Extract brand names for refinement context
        brand_names = []
        for business in businesses:
            name = business.get("name", "")
            if name:
                brand = extract_brand_name(name)
                if brand and brand not in brand_names:
                    brand_names.append(brand)
        
        logger.info(f"Extracted {len(brand_names)} unique brand names")
        if 'st' in globals():
            st.write(f"Found {len(businesses)} businesses with {len(brand_names)} unique brands")
            st.write("Sample brands:", ", ".join(brand_names[:5]))
        
        # Generate more keyword variations first to ensure we have enough to work with
        raw_keywords = generate_keyword_suggestions(
            [business.get("name", "") for business in businesses if business.get("name")], 
            city
        )
        
        if not raw_keywords:
            logger.warning("No keywords generated")
            if 'st' in globals():
                st.warning("⚠️ No keywords generated from business data")
            return False
        
        logger.info(f"Generated {len(raw_keywords)} raw keywords")
        if 'st' in globals():
            st.write(f"Generated {len(raw_keywords)} initial keyword suggestions")
            with st.expander("View sample raw keywords"):
                for kw in raw_keywords[:20]:
                    st.write(f"- {kw}")
        
        # Refine keywords using OpenAI Assistant if enabled
        keywords = raw_keywords
        if refine_keywords:
            if 'st' in globals():
                st.write("Starting AI-powered keyword refinement...")
                
            refined_keywords = batch_refine_keywords(raw_keywords, brand_names, city)
            
            if not refined_keywords:
                logger.warning("Keyword refinement returned no results, using raw keywords")
                if 'st' in globals():
                    st.warning("⚠️ Keyword refinement returned no results, using raw keywords")
            else:
                logger.info(f"Refined {len(raw_keywords)} keywords to {len(refined_keywords)} keywords")
                if 'st' in globals():
                    st.success(f"✅ Refined {len(raw_keywords)} keywords to {len(refined_keywords)} keywords")
                    with st.expander("View sample refined keywords"):
                        for kw in refined_keywords[:20]:
                            st.write(f"- {kw}")
                keywords = refined_keywords
        
        # Get search volumes
        if 'st' in globals():
            st.write(f"Getting search volumes for {len(keywords)} keywords...")
            
        df = get_search_volumes(keywords)
        
        if df.empty:
            logger.warning("No search volume data obtained")
            if 'st' in globals():
                st.error("❌ No search volume data obtained")
            return False
        
        # Add city to DataFrame
        df = df.assign(city=city)
        
        # Upsert to Pinecone
        from src.embed_upsert import upsert_keywords
        
        if 'st' in globals():
            st.write("Uploading keyword data to Pinecone...")
            
        upsert_keywords(df, city)
        
        logger.info("Successfully uploaded keyword data to Pinecone")
        if 'st' in globals():
            st.success("✅ Successfully uploaded keyword data to Pinecone")
        
        # Export to CSV for visualization
        try:
            # Create data directory if it doesn't exist
            data_dir = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "data")
            os.makedirs(data_dir, exist_ok=True)
            
            # Save to data directory
            csv_path = os.path.join(data_dir, "keyword_volumes.csv")
            df.to_csv(csv_path, index=False)
            
            # Also save to root for easy access in Streamlit
            df.to_csv("keyword_volumes.csv", index=False)
            
            logger.info(f"Exported keyword data to CSV: {csv_path}")
            if 'st' in globals():
                st.success(f"✅ Exported keyword data to CSV")
                
                # Show dataframe
                st.subheader("Generated Keywords with Search Volumes")
                st.dataframe(
                    df[["keyword", "search_volume", "competition", "cpc"]]
                    .sort_values("search_volume", ascending=False)
                    .head(20),
                    use_container_width=True
                )
                
                # Download option
                csv = df.to_csv(index=False).encode('utf-8')
                st.download_button(
                    label="⬇️ Download Keywords as CSV",
                    data=csv,
                    file_name='generated_keywords.csv',
                    mime='text/csv',
                )
                
        except Exception as e:
            logger.warning(f"Could not export to CSV: {str(e)}")
            if 'st' in globals():
                st.warning(f"⚠️ Could not export to CSV: {str(e)}")
        
        return True
    
    except Exception as e:
        logger.error(f"Error in enhanced keyword pipeline: {str(e)}")
        traceback.print_exc()
        if 'st' in globals():
            st.error(f"❌ Error in enhanced keyword pipeline: {str(e)}")
            st.code(traceback.format_exc())
        return False

def generate_enhanced_keywords_for_businesses(business_names: List[str], city: str, refine_keywords: bool = True) -> List[str]:
    """
    Generate enhanced keywords for a list of business names in a city
    
    Args:
        business_names: List of business names
        city: Target city
        refine_keywords: Whether to refine keywords using OpenAI Assistant
        
    Returns:
        List of generated keywords
    """
    # This function can internally use the existing generate_location_keywords
    # function with the business names provided
    
    if not business_names:
        logger.warning("No business names provided")
        return []
    
    # Extract brand names for refinement context
    brand_names = [extract_brand_name(name) for name in business_names if name]
    brand_names = [brand for brand in brand_names if brand]
    
    # Generate initial keywords using the existing function
    raw_keywords = generate_keyword_suggestions(business_names, city)
    
    # Refine keywords using OpenAI Assistant if enabled
    if refine_keywords:
        logger.info("Starting keyword refinement with OpenAI Assistant")
        refined_keywords = batch_refine_keywords(raw_keywords, brand_names, city)
        
        if not refined_keywords:
            logger.warning("Keyword refinement returned no results, using raw keywords")
            return raw_keywords
        else:
            logger.info(f"Refined {len(raw_keywords)} keywords to {len(refined_keywords)} keywords")
            return refined_keywords
    else:
        return raw_keywords

# If this script is run directly, execute the enhanced pipeline
if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description="Run enhanced keyword pipeline")
    parser.add_argument("--city", default="Bengaluru", help="Target city for keywords")
    parser.add_argument("--no-refine", action="store_true", help="Skip keyword refinement")
    
    args = parser.parse_args()
    
    success = run_enhanced_keyword_pipeline(args.city, not args.no_refine)
    
    if success:
        print("✅ Enhanced keyword pipeline completed successfully")
    else:
        print("❌ Enhanced keyword pipeline failed")
        sys.exit(1)
