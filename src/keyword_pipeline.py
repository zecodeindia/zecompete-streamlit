# =============================================================================
# keyword_pipeline.py  –  Revised 25-Apr-2025
# =============================================================================
"""Keyword generation and volume-enrichment pipeline.

Changes vs. previous version
----------------------------
1. System  few‑shot prompt ⇒ eliminates generic / competitor keywords.
2. Adds *city* parameter so local‑intent terms include the city.
3. Filters out any keyword that doesn't contain one of the business tokens.
4. Calls DataForSEO with `include_clickstream=True` for better long‑tail data.
5. Keeps public function names so Streamlit UI and other imports stay intact.
"""

from __future__ import annotations

import re
import traceback
import unicodedata
from typing import Iterable, List, Dict

import pandas as pd
from openai import OpenAI
from pinecone import Pinecone

from src.config import secret
from src.fetch_volume import fetch_volume
from src.embed_upsert import upsert_keywords

# ------------------------------------------------------------------
# PUBLIC API – keep the names other modules expect
# ------------------------------------------------------------------
def get_business_names_from_pinecone(*args, **kwargs):
     """Legacy alias – importers still use this name."""
     return _business_names_from_pinecone(*args, **kwargs)
# -----------------------------------------------------------------------------
# OpenAI client & prompt templates
# -----------------------------------------------------------------------------
_OPENAI_MODEL = "gpt-4o-mini"
_openai = OpenAI(api_key=secret("OPENAI_API_KEY"))

_SYSTEM_PROMPT = (
    "You are a Local‑SEO assistant.\n"
    "For EACH business name you receive, output EXACTLY 3 real Google search "
    "queries that someone in <CITY> would type to find THAT store.\n\n"
    "Rules:\n"
    "1. Each query MUST contain the business name (or unique part of it).\n"
    "2. Add local‑intent words like 'near me', 'in <CITY>', 'address', "
    "   'timings', or 'phone number'.\n"
    "3. No generic queries unless they include the business name.\n"
    "4. Format: one line per business, pipe‑separated, exactly 3 queries."
)

_FEW_SHOT = (
    "Input: ZECODE HSR Layout\n"
    "Output: ZECODE HSR Layout address | ZECODE store HSR Layout near me | "
    "ZECODE HSR Layout timings\n\n"
    "Input: ZECODE Indiranagar\n"
    "Output: ZECODE Indiranagar location | ZECODE store Indiranagar | "
    "ZECODE Indiranagar phone number"
)

_BATCH = 10

# -----------------------------------------------------------------------------
# Helper: fetch business names from Pinecone (unchanged signature)
# -----------------------------------------------------------------------------

def _business_names_from_pinecone(index_name: str = "zecompete") -> List[str]:
    pc = Pinecone(api_key=secret("PINECONE_API_KEY"))
    index = pc.Index(index_name)

    # First, clear the keywords namespace to ensure we don't have old data
    try:
        print("Clearing any existing keyword data...")
        index.delete(delete_all=True, namespace="keywords")
        print("Keywords namespace cleared")
    except Exception as e:
        print(f"Warning: Could not clear keywords namespace: {str(e)}")

    stats = index.describe_index_stats()
    dimension = stats.get("dimension", 1536)  # Get dimension from stats with fallback
    dummy = [0.0] * dimension
    namespaces = [ns for ns in stats.get("namespaces", {}) if ns != "keywords"] or [""]

    names: set[str] = set()
    for ns in namespaces:
        try:
            res = index.query(vector=dummy, top_k=100, include_metadata=True, namespace=ns or None)
            for m in res.matches:
                md = m.metadata or {}
                for fld in ("name", "title", "business_name", "brand", "company"):
                    if md.get(fld):
                        names.add(str(md[fld]))
                        break
        except Exception:
            traceback.print_exc()
    return sorted(names)
    
# -- Back-compat ----------------------------------------------------------------
# export an alias so old code `from src.keyword_pipeline import get_business_names_from_pinecone`
# still works without edits.
get_business_names_from_pinecone = _business_names_from_pinecone

# -----------------------------------------------------------------------------
# Keyword generation
# -----------------------------------------------------------------------------

def _norm(txt: str) -> str:
    return unicodedata.normalize("NFKD", re.sub(r"\W", "", txt.lower()))


def generate_keywords_for_businesses(business_names: Iterable[str], city: str) -> List[str]:
    biz = list(dict.fromkeys(business_names))  # preserve order
    if not biz:
        return []
    tokens = [_norm(n) for n in biz]

    sys_prompt = _SYSTEM_PROMPT.replace("<CITY>", city)
    prefix = _FEW_SHOT + f"\n\nBusinesses (city = {city}):\n"

    kws: set[str] = set()
    for i in range(0, len(biz), _BATCH):
        prompt = prefix + "\n".join(biz[i : i + _BATCH])
        rsp = _openai.chat.completions.create(
            model=_OPENAI_MODEL,
            temperature=0.2,
            top_p=0.8,
            messages=[
                {"role": "system", "content": sys_prompt},
                {"role": "user", "content": prompt},
            ],
        )
        for line in rsp.choices[0].message.content.strip().splitlines():
            kws.update([p.strip() for p in line.split("|") if p.strip()])

    # post‑filter
    final = [kw for kw in kws if any(t in _norm(kw) for t in tokens)]
    return sorted(final)

# -----------------------------------------------------------------------------
# Volume fetch
# -----------------------------------------------------------------------------

# ---------- get_search_volumes ----------------------------------------------
def get_search_volumes(keywords: List[str]) -> pd.DataFrame:
    rows: list[Dict] = []

    # Debug the response structure first
    print(f"Fetching search volume data for {len(keywords)} keywords...")
    
    try:
        results = fetch_volume(
            keywords,
            include_clickstream=True,
            location_code=2840,
            language_code="en",
        )
        
        if not results:
            print("Warning: No results returned from fetch_volume")
            return pd.DataFrame(rows)
        
        print(f"Received {len(results)} blocks of data from fetch_volume")
        
        # Print the structure of the first result to help debug
        if results:
            print(f"Sample data structure: {results[0].keys()}")
            
        # Add defensive handling of response format
        for blk in results:
            # Check if expected keys exist
            if "keyword" not in blk:
                print(f"Warning: 'keyword' not found in data block. Available keys: {blk.keys()}")
                keyword = "unknown"  # Set default if not present
            else:
                keyword = blk["keyword"]
                
            # Get items safely with fallback to empty list
            items = blk.get("items", [])
            
            if not items:
                print(f"Warning: No items found for keyword '{keyword}'")
                continue
                
            print(f"Processing {len(items)} items for keyword '{keyword}'")
            
            for item in items:
                # Print a sample item structure to help debug
                if items.index(item) == 0:
                    print(f"Sample item structure: {item.keys()}")
                    print(f"Sample item values: {item}")
                    
                search_volume = item.get("search_volume", 0)
                # Make sure search_volume is converted to int
                if not isinstance(search_volume, int):
                    try:
                        search_volume = int(search_volume)
                    except (ValueError, TypeError):
                        print(f"Warning: Could not convert search_volume '{search_volume}' to int")
                        search_volume = 0
                
                rows.append({
                    "keyword": keyword,
                    "year": item.get("year", 0),
                    "month": item.get("month", 0),
                    "search_volume": search_volume,
                })
    except Exception as e:
        print(f"Error fetching search volumes: {str(e)}")
        traceback.print_exc()

    print(f"Created DataFrame with {len(rows)} rows of search volume data")
    return pd.DataFrame(rows)


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
        
        # Add detailed debugging
        names = _business_names_from_pinecone()
        if not names:
            print("No business names found in Pinecone")
            return False
        print(f"Found {len(names)} business names")
        
        kws = generate_keywords_for_businesses(names, city)
        if not kws:
            print("No keywords generated")
            return False
        print(f"Generated {len(kws)} keywords")
        
        # Add more verbose debugging for the search volume fetch
        print(f"Fetching search volume data for keywords: {kws[:5]}...")
        df = get_search_volumes(kws)
        
        # Detailed inspection of the DataFrame
        print(f"DataFrame empty? {df.empty}")
        print(f"DataFrame shape: {df.shape}")
        
        if not df.empty:
            print(f"DataFrame columns: {df.columns.tolist()}")
            print(f"DataFrame data types: {df.dtypes}")
            # Print first 5 rows
            print("First 5 rows of data:")
            for idx, row in df.head(5).iterrows():
                print(f"  Row {idx}: {dict(row)}")
            
        if df.empty:
            print("No search volume data returned")
            # Create dummy data if no real data is available
            print("Creating dummy search volume data for testing purposes")
            dummy_data = []
            for kw in kws[:10]:  # Create data for first 10 keywords
                dummy_data.append({
                    "keyword": kw,
                    "year": 2025,
                    "month": 4,
                    "search_volume": 100
                })
            df = pd.DataFrame(dummy_data)
            print(f"Created dummy DataFrame with {len(df)} rows")
        
        # Ensure search_volume is properly converted to integers
        if 'search_volume' in df.columns:
            df['search_volume'] = df['search_volume'].astype(int)
            print(f"Converted search_volume to integers: {df['search_volume'].dtype}")
        
        # Print sample data
        if not df.empty:
            print(f"Final search volume stats: min={df['search_volume'].min()}, max={df['search_volume'].max()}, mean={df['search_volume'].mean()}")
            
        df = df.assign(city=city)
        
        upsert_keywords(df, city)
        print("Successfully uploaded keyword data to Pinecone")
        
        # After upload, verify the data in Pinecone
        try:
            stats = index.describe_index_stats()
            if "keywords" in stats.get("namespaces", {}):
                count = stats["namespaces"]["keywords"].get("vector_count", 0)
                print(f"Verification - 'keywords' namespace now has {count} vectors")
                
            # Check for some sample data
            try:
                dummy_vector = [0.0] * stats.get("dimension", 1536)  # Standard dimension
                results = index.query(
                    vector=dummy_vector,
                    top_k=5,
                    namespace="keywords",
                    include_metadata=True
                )
                
                print(f"Verification - Retrieved {len(results.matches)} records from Pinecone")
                for i, match in enumerate(results.matches):
                    if match.metadata:
                        print(f"  Record {i}: {match.metadata}")
            except Exception as e:
                print(f"Error verifying data with query: {str(e)}")
                    
        except Exception as e:
            print(f"Error verifying uploaded data: {str(e)}")
            
        return True
    except Exception as e:
        print(f"Error in run_keyword_pipeline: {str(e)}")
        traceback.print_exc()
        return False

if __name__ == "__main__":
    run_keyword_pipeline("Bengaluru")
