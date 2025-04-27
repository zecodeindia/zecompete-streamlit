# src/enhanced_keyword_pipeline.py
"""
Enhanced keyword pipeline with OpenAI Assistant integration.
This module extends the existing keyword pipeline with advanced keyword refinement capabilities.
"""

import os
import sys
import traceback
import logging
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
        
        # Generate location-focused keywords
        raw_keywords = generate_location_keywords(businesses)
        
        if not raw_keywords:
            logger.warning("No keywords generated")
            return False
        
        logger.info(f"Generated {len(raw_keywords)} raw keywords")
        
        # Refine keywords using OpenAI Assistant if enabled
        if refine_keywords:
            logger.info("Starting keyword refinement with OpenAI Assistant")
            refined_keywords = batch_refine_keywords(raw_keywords, brand_names, city)
            
            if not refined_keywords:
                logger.warning("Keyword refinement returned no results, using raw keywords")
                keywords = raw_keywords
            else:
                logger.info(f"Refined {len(raw_keywords)} keywords to {len(refined_keywords)} keywords")
                keywords = refined_keywords
        else:
            keywords = raw_keywords
        
        # Display sample keywords
        sample_size = min(10, len(keywords))
        logger.info(f"Sample keywords: {', '.join(keywords[:sample_size])}")
        
        # Get search volumes
        df = get_search_volumes(keywords)
        
        if df.empty:
            logger.warning("No search volume data obtained")
            return False
        
        # Add city to DataFrame
        df = df.assign(city=city)
        
        # Upsert to Pinecone
        from src.embed_upsert import upsert_keywords
        upsert_keywords(df, city)
        
        logger.info("Successfully uploaded keyword data to Pinecone")
        
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
        except Exception as e:
            logger.warning(f"Could not export to CSV: {str(e)}")
        
        return True
    
    except Exception as e:
        logger.error(f"Error in enhanced keyword pipeline: {str(e)}")
        traceback.print_exc()
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
    
    # Convert business names to the format expected by generate_location_keywords
    businesses = []
    for name in business_names:
        businesses.append({
            "name": name,
            "city": city,
            "location": extract_location_from_business({"name": name, "city": city}) 
        })
    
    # Use existing function to generate keywords
    raw_keywords = generate_location_keywords(businesses)
    
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
