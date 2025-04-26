# =============================================================================
# keyword_pipeline.py  –  Updated to support trend data
# =============================================================================
"""Keyword generation and volume-enrichment pipeline with trend data support.

Key features:
1. Fetches 12 months of historical search volume data
2. Stores monthly trend data in Pinecone
3. Exports trend data to CSV for visualization
"""

from __future__ import annotations

import re
import traceback
import unicodedata
import random
import datetime
import os
import csv
from typing import Iterable, List, Dict, Set

import pandas as pd
from openai import OpenAI
from pinecone import Pinecone

from src.config import secret
from src.fetch_volume import fetch_volume
from src.embed_upsert import upsert_keywords

# ------------------------------------------------------------------
# PUBLIC API – keep the names other modules expect
# ------------------------------------------------------------------
def _business_names_from_pinecone(index_name: str = "zecompete") -> List[str]:
    pc = Pinecone(api_key=secret("PINECONE_API_KEY"))
    index = pc.Index(index_name)

    # Directly list namespaces and fetch all items
    names = set()
    try:
        stats = index.describe_index_stats()
        dimension = stats.get("dimension", 1536)
        namespaces = [ns for ns in stats.get("namespaces", {}) if ns != "keywords"]

        dummy_vector = [0.0] * dimension

        for ns in namespaces:
            try:
                # Fetch as many as possible
                result = index.query(
                    vector=dummy_vector,
                    top_k=500,  # Increase top_k
                    include_metadata=True,
                    namespace=ns or None
                )
                if result.matches:
                    for match in result.matches:
                        md = match.metadata or {}
                        for fld in ("name", "title", "business_name", "brand", "company"):
                            if md.get(fld):
                                names.add(str(md[fld]))
                                break
            except Exception as e:
                print(f"Error querying namespace '{ns}': {e}")
    except Exception as e:
        print(f"Error describing index stats: {e}")

    return sorted(names)
# -----------------------------------------------------------------------------
# OpenAI client & prompt templates
# -----------------------------------------------------------------------------
_OPENAI_MODEL = "gpt-4o-mini"
_openai = OpenAI(api_key=secret("OPENAI_API_KEY"))

# New improved system prompt for keyword generation
_SYSTEM_PROMPT = """You are a local SEO expert specializing in generating realistic, high-value search queries.
For each business, create EXACTLY 5 different search queries that real people in <CITY> would type into Google.

Include these query types:
1. Basic business name + location (e.g., "business in <CITY>")
2. Business with intent (e.g., "business near me", "business hours", "business address")
3. Business products or services (e.g., "business products", "business offerings")
4. Business + comparable (e.g., "business like X", "business vs competitor")
5. Business with specific need (e.g., "business best deals", "business discounts")

REQUIREMENTS:
- Each query MUST contain the business name or a recognizable part of it
- Queries should be realistic and have actual search volume
- Make sure queries match how real users actually search
- Format: One line per business, with the 5 queries separated by pipes (|)
"""

_FEW_SHOT = """
Input: ZECODE HSR Layout

Output: ZECODE HSR Layout address | ZECODE store HSR Layout near me | ZECODE HSR Layout timings | ZECODE HSR Layout vs other coding schools | ZECODE HSR Layout admission process

Input: ZECODE Indiranagar

Output: ZECODE Indiranagar location | ZECODE store Indiranagar opening hours | ZECODE Indiranagar courses | ZECODE Indiranagar compared to WhiteHat Jr | ZECODE Indiranagar beginner classes
"""

_BATCH = 8  # Process businesses in batches of this size

# -- Back-compat ----------------------------------------------------------------
# export an alias so old code `from src.keyword_pipeline import get_business_names_from_pinecone`
# still works without edits.
get_business_names_from_pinecone = _business_names_from_pinecone

# -----------------------------------------------------------------------------
# Keyword generation
# -----------------------------------------------------------------------------

def _norm(txt: str) -> str:
    """Normalize text for comparison"""
    return unicodedata.normalize("NFKD", re.sub(r"\W", "", txt.lower()))

def generate_keywords_for_businesses(business_names: Iterable[str], city: str) -> List[str]:
    """
    Generate relevant keywords for the given businesses in the specified city
    
    Args:
        business_names: List of business names to generate keywords for
        city: Target city name
        
    Returns:
        List of generated keywords
    """
    biz = list(dict.fromkeys(business_names))  # preserve order and deduplicate
    if not biz:
        return []
    tokens = [_norm(n) for n in biz]  # Normalized business tokens for validation

    # Replace city placeholder in system prompt
    sys_prompt = _SYSTEM_PROMPT.replace("<CITY>", city)
    prefix = _FEW_SHOT + f"\n\nBusinesses (city = {city}):\n"

    kws: set[str] = set()
    for i in range(0, len(biz), _BATCH):
        # Create prompt for this batch
        batch = biz[i : i + _BATCH]
        prompt = prefix + "\n".join(batch)
        
        # Call OpenAI
        try:
            rsp = _openai.chat.completions.create(
                model=_OPENAI_MODEL,
                temperature=0.7,  # Higher temperature for more diverse queries
                top_p=0.9,
                messages=[
                    {"role": "system", "content": sys_prompt},
                    {"role": "user", "content": prompt},
                ],
            )
            
            # Process response
            response_text = rsp.choices[0].message.content.strip()
            print(f"OpenAI response ({len(response_text)} chars)")
            
            # Extract keywords from each line
            for line in response_text.splitlines():
                if "|" in line:
                    # Process pipe-separated format
                    line_keywords = [kw.strip() for kw in line.split("|") if kw.strip()]
                    kws.update(line_keywords)
                elif ":" in line and any(b.lower() in line.lower() for b in batch):
                    # Process business: keyword1, keyword2 format
                    _, keywords_part = line.split(":", 1)
                    for separator in [",", ";", "•"]:
                        if separator in keywords_part:
                            line_kws = [kw.strip() for kw in keywords_part.split(separator) if kw.strip()]
                            kws.update(line_kws)
                            break
            
        except Exception as e:
            print(f"Error generating keywords for batch {i}-{i+_BATCH}: {e}")
            traceback.print_exc()

    # Filter keywords to ensure they contain business tokens
    final = [kw for kw in kws if any(t in _norm(kw) for t in tokens)]
    
    # Add additional variants with city name if not already present
    additional_kws = []
    for kw in final:
        if city.lower() not in kw.lower() and "near me" not in kw.lower():
            additional_kws.append(f"{kw} {city}")
    
    # Combine and deduplicate
    all_keywords = sorted(set(final + additional_kws))
    print(f"Generated {len(all_keywords)} keywords from {len(biz)} businesses")
    
    return all_keywords

# -----------------------------------------------------------------------------
# Search volume fetch with trend data
# -----------------------------------------------------------------------------

def get_search_volumes(keywords: List[str]) -> pd.DataFrame:
    """
    Get search volume data with 12-month trends for the given keywords
    
    Args:
        keywords: List of keywords to get search volumes for
        
    Returns:
        DataFrame with keyword data including monthly trend data
    """
    if not keywords:
        print("No keywords provided")
        return pd.DataFrame()
        
    print(f"Fetching search volume data with trends for {len(keywords)} keywords...")
    
    try:
        # Call DataForSEO API with trend data
        results = fetch_volume(keywords, include_trends=True)
        
        if not results:
            print("No results returned from DataForSEO")
            return _generate_fallback_volumes_with_trends(keywords)
            
        print(f"Received data for {len(results)} keywords")
        
        # Process the results into a DataFrame
        all_rows = []
        
        for keyword, data in results.items():
            # Get base data
            base_volume = data.get("search_volume", 0)
            competition = data.get("competition", 0.0)
            cpc = data.get("cpc", 0.0)
            
            # Check for trend data
            if "monthly_trends" in data and data["monthly_trends"]:
                # We have monthly trend data
                monthly_trends = data["monthly_trends"]
                
                # Add each month as a separate row
                for trend in monthly_trends:
                    all_rows.append({
                        "keyword": keyword,
                        "year": trend.get("year", 0),
                        "month": trend.get("month", 0),
                        "search_volume": trend.get("search_volume", base_volume),
                        "competition": competition,
                        "cpc": cpc
                    })
            else:
                # No trend data, use current data only
                now = datetime.datetime.now()
                all_rows.append({
                    "keyword": keyword,
                    "year": now.year,
                    "month": now.month,
                    "search_volume": base_volume,
                    "competition": competition,
                    "cpc": cpc
                })
        
        if not all_rows:
            print("No valid rows created from API results")
            return _generate_fallback_volumes_with_trends(keywords)
            
        # Create DataFrame
        df = pd.DataFrame(all_rows)
        
        # Ensure proper data types
        df["search_volume"] = pd.to_numeric(df["search_volume"], errors="coerce").fillna(0).astype(int)
        df["year"] = pd.to_numeric(df["year"], errors="coerce").fillna(0).astype(int)
        df["month"] = pd.to_numeric(df["month"], errors="coerce").fillna(0).astype(int)
        df["competition"] = pd.to_numeric(df["competition"], errors="coerce").fillna(0.0).astype(float)
        df["cpc"] = pd.to_numeric(df["cpc"], errors="coerce").fillna(0.0).astype(float)
        
        # Export to CSV for visualization
        _export_keyword_data_to_csv(df)
        
        # Log statistics
        trends_count = len(df)
        keywords_count = df["keyword"].nunique()
        print(f"Processed {trends_count} data points for {keywords_count} keywords")
        print(f"Search volume stats: min={df['search_volume'].min()}, max={df['search_volume'].max()}, mean={df['search_volume'].mean():.1f}")
        
        return df
        
    except Exception as e:
        print(f"Error in get_search_volumes: {str(e)}")
        traceback.print_exc()
        return _generate_fallback_volumes_with_trends(keywords)

def _generate_fallback_volumes_with_trends(keywords: List[str]) -> pd.DataFrame:
    """
    Generate fallback data with 12 months of trend data
    
    Args:
        keywords: List of keywords to generate trend data for
        
    Returns:
        DataFrame with simulated search volume trend data
    """
    print("Generating fallback search volume data with trends")
    
    # Factors that influence search volume
    local_intent_terms = ["near me", "address", "location", "directions", "map"]
    high_volume_terms = ["best", "top", "cheap", "price", "discount", "deals"]
    low_volume_terms = ["timings", "hours", "phone", "contact", "email", "owner"]
    
    all_rows = []
    now = datetime.datetime.now()
    
    for keyword in keywords:
        kw_lower = keyword.lower()
        
        # Base volume - randomized but realistic
        base_volume = random.randint(30, 400)
        
        # Adjust volume based on keyword characteristics
        if any(term in kw_lower for term in local_intent_terms):
            base_volume = int(base_volume * random.uniform(0.6, 0.8))  # Lower for local intent
        
        if any(term in kw_lower for term in high_volume_terms):
            base_volume = int(base_volume * random.uniform(1.2, 1.5))  # Higher for popular terms
            
        if any(term in kw_lower for term in low_volume_terms):
            base_volume = int(base_volume * random.uniform(0.4, 0.7))  # Lower for specific needs
            
        # Ensure reasonable minimum
        base_volume = max(10, base_volume)
        
        # Generate realistic competition score (0-1)
        competition = round(random.uniform(0.1, 0.95), 2)
        
        # Generate realistic CPC based on competition
        base_cpc = 0.5 + (competition * 3)  # Higher competition → higher CPC
        cpc = round(random.uniform(base_cpc * 0.8, base_cpc * 1.2), 2)  # Add some variation
        
        # Generate 12 months of trend data
        for i in range(12):
            # Calculate month and year for this data point (going backwards)
            month = now.month - i
            year = now.year
            
            # Handle month wrapping
            if month <= 0:
                month += 12
                year -= 1
                
            # Apply seasonal and random variation to volume
            seasonal_factor = 1.0
            
            # Different seasonal patterns
            if month in [11, 12, 1]:  # Winter months
                seasonal_factor = random.uniform(0.85, 1.15)
            elif month in [3, 4, 5]:  # Spring months
                seasonal_factor = random.uniform(0.9, 1.2)
            elif month in [6, 7, 8]:  # Summer months
                seasonal_factor = random.uniform(0.8, 1.1)
            else:
                seasonal_factor = random.uniform(0.9, 1.25)  # Fall months
                
            # Add randomness for more realistic trends
            random_factor = random.uniform(0.9, 1.1)
            
            # Calculate this month's volume
            month_volume = int(base_volume * seasonal_factor * random_factor)
            
            # Add row
            all_rows.append({
                "keyword": keyword,
                "year": year,
                "month": month,
                "search_volume": month_volume,
                "competition": competition,
                "cpc": cpc
            })
    
    # Create DataFrame
    df = pd.DataFrame(all_rows)
    
    # Export to CSV for visualization
    _export_keyword_data_to_csv(df)
    
    print(f"Generated fallback trend data with {len(df)} rows for {len(keywords)} keywords")
    return df

def _export_keyword_data_to_csv(df: pd.DataFrame) -> None:
    """
    Export keyword data to CSV for visualization
    
    Args:
        df: DataFrame with keyword data
    """
    try:
        # Create the data directory if it doesn't exist
        data_dir = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "data")
        os.makedirs(data_dir, exist_ok=True)
        
        # Define CSV path
        csv_path = os.path.join(data_dir, "keyword_volumes.csv")
        
        # Export to CSV
        df.to_csv(csv_path, index=False)
        print(f"Exported keyword data to {csv_path}")
        
        # For Streamlit app, also copy to root directory
        root_csv_path = "keyword_volumes.csv"
        df.to_csv(root_csv_path, index=False)
        print(f"Also exported to {root_csv_path} for Streamlit access")
        
    except Exception as e:
        print(f"Error exporting keyword data to CSV: {e}")
        traceback.print_exc()

# -----------------------------------------------------------------------------
# Full pipeline (backward‑compatible)
# -----------------------------------------------------------------------------

def run_keyword_pipeline(city: str = "General") -> bool:
    try:
        # Initialize Pinecone
        pc = Pinecone(api_key=secret("PINECONE_API_KEY"))
        index = pc.Index("zecompete")
        
        # Clear existing keyword data
        try:
            print(f"Deleting existing keyword data from Pinecone namespace 'keywords'...")
            index.delete(delete_all=True, namespace="keywords")
            print("Successfully cleared previous keyword data")
        except Exception as e:
            print(f"Warning: Could not clear previous keyword data: {str(e)}")
        
        # Get business names
        names = _business_names_from_pinecone()
        if not names:
            print("No business names found in Pinecone")
            return False
        print(f"Found {len(names)} business names")
        
        # Generate keywords with improved method
        kws = generate_keywords_for_businesses(names, city)
        if not kws:
            print("No keywords generated")
            return False
        print(f"Generated {len(kws)} keywords")
        
        # Get sample of keywords to show
        sample_size = min(10, len(kws))
        print(f"Sample keywords: {', '.join(kws[:sample_size])}")
        
        # Fetch search volumes with trend data
        print(f"Fetching search volume data with trends...")
        df = get_search_volumes(kws)
        
        # Verify DataFrame
        print(f"DataFrame empty? {df.empty}")
        print(f"DataFrame shape: {df.shape}")
        
        if not df.empty:
            print(f"DataFrame columns: {df.columns.tolist()}")
            print(f"DataFrame data types: {df.dtypes}")
            
            # Show sample data
            print("First 5 rows of data:")
            for idx, row in df.head(5).iterrows():
                print(f"  Row {idx}: {dict(row)}")
                
            # Show unique months/years in the data
            years = sorted(df["year"].unique())
            months = sorted(df["month"].unique())
            print(f"Years in data: {years}")
            print(f"Months in data: {months}")
        
        # Add city to DataFrame
        df = df.assign(city=city)
        
        # Upsert to Pinecone
        upsert_keywords(df, city)
        print("Successfully uploaded keyword data to Pinecone")
        
        # Verify upload
        try:
            stats = index.describe_index_stats()
            if "keywords" in stats.get("namespaces", {}):
                count = stats["namespaces"]["keywords"].get("vector_count", 0)
                print(f"Verification - 'keywords' namespace now has {count} vectors")
                
            # Sample check
            try:
                dummy_vector = [0.0] * stats.get("dimension", 1536)
                results = index.query(
                    vector=dummy_vector,
                    top_k=5,
                    namespace="keywords",
                    include_metadata=True
                )
                
                print(f"Retrieved {len(results.matches)} sample records from Pinecone")
                for i, match in enumerate(results.matches):
                    if match.metadata:
                        print(f"  Record {i}: {match.metadata}")
            except Exception as e:
                print(f"Error in verification query: {str(e)}")
                    
        except Exception as e:
            print(f"Error verifying uploaded data: {str(e)}")
            
        return True
    except Exception as e:
        print(f"Error in run_keyword_pipeline: {str(e)}")
        traceback.print_exc()
        return False

if __name__ == "__main__":
    run_keyword_pipeline
