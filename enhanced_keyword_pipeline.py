"""
enhanced_keyword_pipeline.py - Pipeline to extract business names, 
get search volumes with 12-month history, and store in Pinecone
"""
import os
import logging
import pandas as pd
from typing import List, Dict, Any
from pinecone import Pinecone
from openai import OpenAI

# Import existing components
from src.config import secret
from src.fetch_volume import fetch_volume
from src.embed_upsert import upsert_keywords

# Set up logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

def extract_business_names_from_pinecone(index_name: str = "zecompete") -> List[str]:
    """
    Extract business names from Pinecone maps namespace
    
    Args:
        index_name: Name of the Pinecone index
        
    Returns:
        List of business names
    """
    logger.info("Extracting business names from Pinecone maps namespace")
    
    try:
        # Initialize Pinecone
        pc = Pinecone(api_key=secret("PINECONE_API_KEY"))
        index = pc.Index(index_name)
        
        # Get index stats to determine dimension
        stats = index.describe_index_stats()
        dimension = stats.get("dimension", 1536)
        
        # Create dummy vector for query (all zeros)
        dummy_vector = [0.0] * dimension
        
        # Query the maps namespace
        results = index.query(
            vector=dummy_vector,
            top_k=100,  # Get up to 100 businesses
            namespace="maps",
            include_metadata=True
        )
        
        # Extract business names from metadata
        business_names = []
        if results and results.matches:
            for match in results.matches:
                if match.metadata and 'name' in match.metadata:
                    business_names.append(match.metadata['name'])
        
        logger.info(f"Extracted {len(business_names)} business names")
        return business_names
    
    except Exception as e:
        logger.error(f"Error extracting business names: {str(e)}")
        return []

def preprocess_business_names(business_names: List[str], city: str) -> List[str]:
    """
    Clean and preprocess business names to create effective keywords
    
    Args:
        business_names: Raw business names from Pinecone
        city: City name to append to keywords
        
    Returns:
        List of processed keywords
    """
    logger.info(f"Preprocessing {len(business_names)} business names with city: {city}")
    
    keywords = []
    for name in business_names:
        # Basic cleaning
        clean_name = name.strip()
        if not clean_name:
            continue
            
        # Add the raw business name
        keywords.append(clean_name)
        
        # Add business name with city if not already in the name
        if city.lower() not in clean_name.lower():
            keywords.append(f"{clean_name} {city}")
        
        # If the business name contains location, extract the brand part
        if " - " in clean_name:
            brand_part = clean_name.split(" - ")[0].strip()
            if brand_part and len(brand_part) > 2:
                keywords.append(brand_part)
                keywords.append(f"{brand_part} {city}")
        
        # If the business name contains comma, extract the brand part
        if "," in clean_name:
            brand_part = clean_name.split(",")[0].strip()
            if brand_part and len(brand_part) > 2:
                keywords.append(brand_part)
                keywords.append(f"{brand_part} {city}")
    
    # Remove duplicates and empty strings
    keywords = list(set([k for k in keywords if k]))
    
    logger.info(f"Generated {len(keywords)} keywords from business names")
    return keywords

def get_search_volume_with_history(keywords: List[str]) -> pd.DataFrame:
    """
    Get search volume data with 12-month history for keywords
    
    Args:
        keywords: List of keywords to get search volumes for
        
    Returns:
        DataFrame with keyword data including 12-month search history
    """
    logger.info(f"Fetching search volume data for {len(keywords)} keywords")
    
    try:
        # Call the existing fetch_volume function with trend data
        results = fetch_volume(keywords, include_trends=True)
        
        if not results:
            logger.warning("No results returned from search volume API")
            return pd.DataFrame()
        
        # Process the results into a DataFrame with monthly data
        rows = []
        
        for keyword, data in results.items():
            # Extract monthly trend data
            if "monthly_trends" in data and data["monthly_trends"]:
                # Add rows for each month in the trend data
                for month_data in data["monthly_trends"]:
                    rows.append({
                        "keyword": keyword,
                        "year": month_data.get("year", 0),
                        "month": month_data.get("month", 0),
                        "search_volume": month_data.get("search_volume", 0),
                        "competition": data.get("competition", 0.0),
                        "cpc": data.get("cpc", 0.0),
                        "avg_monthly_volume": data.get("search_volume", 0)  # Store average in each row
                    })
            else:
                # If no monthly data, add a single row with the overall volume
                import datetime
                now = datetime.datetime.now()
                rows.append({
                    "keyword": keyword,
                    "year": now.year,
                    "month": now.month,
                    "search_volume": data.get("search_volume", 0),
                    "competition": data.get("competition", 0.0),
                    "cpc": data.get("cpc", 0.0),
                    "avg_monthly_volume": data.get("search_volume", 0)
                })
        
        if not rows:
            logger.warning("No valid rows created from search volume results")
            return pd.DataFrame()
        
        # Create DataFrame
        df = pd.DataFrame(rows)
        
        # Ensure proper data types
        df["search_volume"] = pd.to_numeric(df["search_volume"], errors="coerce").fillna(0).astype(int)
        df["year"] = pd.to_numeric(df["year"], errors="coerce").fillna(0).astype(int)
        df["month"] = pd.to_numeric(df["month"], errors="coerce").fillna(0).astype(int)
        df["competition"] = pd.to_numeric(df["competition"], errors="coerce").fillna(0.0).astype(float)
        df["cpc"] = pd.to_numeric(df["cpc"], errors="coerce").fillna(0.0).astype(float)
        df["avg_monthly_volume"] = pd.to_numeric(df["avg_monthly_volume"], errors="coerce").fillna(0).astype(int)
        
        logger.info(f"Created DataFrame with {len(df)} rows of search volume data")
        
        # Save to CSV file for backup/inspection
        try:
            df.to_csv("keyword_volumes.csv", index=False)
            logger.info("Saved search volume data to keyword_volumes.csv")
        except Exception as e:
            logger.warning(f"Could not save CSV: {str(e)}")
        
        return df
    
    except Exception as e:
        logger.error(f"Error getting search volume data: {str(e)}")
        return pd.DataFrame()

def run_business_keyword_pipeline(city: str) -> bool:
    """
    Run the full pipeline:
    1. Extract business names from Pinecone maps namespace
    2. Preprocess business names to create keywords
    3. Get search volume data with 12-month history
    4. Store the data in Pinecone keywords namespace
    
    Args:
        city: City name to use for keywords
        
    Returns:
        True if successful, False otherwise
    """
    logger.info(f"Starting business keyword pipeline for city: {city}")
    
    try:
        # Step 1: Extract business names from Pinecone
        business_names = extract_business_names_from_pinecone()
        
        if not business_names:
            logger.warning("No business names found in Pinecone. Please ensure data exists in the maps namespace.")
            return False
        
        # Step 2: Preprocess business names to create keywords
        keywords = preprocess_business_names(business_names, city)
        
        if not keywords:
            logger.warning("No valid keywords generated from business names.")
            return False
        
        # Step 3: Get search volume data with 12-month history
        df = get_search_volume_with_history(keywords)
        
        if df.empty:
            logger.warning("No search volume data obtained.")
            return False
        
        # Add city to dataframe
        df["city"] = city
        
        # Step 4: Store the data in Pinecone keywords namespace
        logger.info(f"Storing {len(df)} rows of keyword data in Pinecone")
        upsert_keywords(df, city)
        
        logger.info("Successfully completed business keyword pipeline")
        return True
    
    except Exception as e:
        logger.error(f"Error running business keyword pipeline: {str(e)}")
        import traceback
        logger.error(traceback.format_exc())
        return False

def combine_data_for_assistant(query: str) -> Dict[str, Any]:
    """
    Combine data from both Pinecone namespaces for OpenAI Assistant
    
    Args:
        query: The query to find relevant data for
        
    Returns:
        Dictionary with combined data suitable for OpenAI Assistant
    """
    logger.info(f"Combining data from Pinecone namespaces for query: {query}")
    
    try:
        # Initialize OpenAI client
        client = OpenAI(api_key=secret("OPENAI_API_KEY"))
        
        # Initialize Pinecone
        pc = Pinecone(api_key=secret("PINECONE_API_KEY"))
        index = pc.Index("zecompete")
        
        # Generate embedding for the query
        response = client.embeddings.create(
            model="text-embedding-3-small",
            input=[query]
        )
        query_embedding = response.data[0].embedding
        
        # Query both namespaces
        maps_results = index.query(
            vector=query_embedding,
            top_k=10,
            namespace="maps",
            include_metadata=True
        )
        
        keywords_results = index.query(
            vector=query_embedding,
            top_k=10,
            namespace="keywords",
            include_metadata=True
        )
        
        # Process maps data
        business_data = []
        if maps_results and maps_results.matches:
            for match in maps_results.matches:
                if match.metadata:
                    business_data.append(match.metadata)
        
        # Process keywords data
        keyword_data = []
        if keywords_results and keywords_results.matches:
            for match in keywords_results.matches:
                if match.metadata:
                    keyword_data.append(match.metadata)
        
        # Combine data
        combined_data = {
            "query": query,
            "businesses": business_data,
            "keywords": keyword_data
        }
        
        logger.info(f"Combined data with {len(business_data)} businesses and {len(keyword_data)} keywords")
        return combined_data
    
    except Exception as e:
        logger.error(f"Error combining data for assistant: {str(e)}")
        return {"query": query, "error": str(e)}

if __name__ == "__main__":
    # Example usage
    run_business_keyword_pipeline("Bengaluru")
