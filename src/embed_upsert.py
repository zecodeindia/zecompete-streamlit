from typing import Iterable, Dict, List
from pinecone import Pinecone  # Updated import
import pandas as pd
from openai import OpenAI
from src.config import secret

# Updated Pinecone initialization
pc = Pinecone(api_key=secret("PINECONE_API_KEY"))
INDEX = pc.Index("zecompete")

client = OpenAI(api_key=secret("OPENAI_API_KEY"))
EMBED_MODEL = "text-embedding-3-small"  # 1536‑dim

# --- helpers -----------------------------------------------------
def _embed(texts: List[str]) -> List[List[float]]:
    res = client.embeddings.create(model=EMBED_MODEL, input=texts)
    return [d.embedding for d in res.data]

def upsert_places(df: pd.DataFrame, brand: str, city: str) -> None:
    # First, clear existing data from all maps namespace
    try:
        print(f"Clearing ALL existing data from 'maps' namespace in Pinecone...")
        
        # Delete all data in the maps namespace
        INDEX.delete(delete_all=True, namespace="maps")
        
        print(f"Successfully cleared all previous data from 'maps' namespace")
    except Exception as e:
        print(f"Warning: Could not clear previous data: {str(e)}")
    
    # Check if 'name' exists or try alternative column names
    if 'name' in df.columns:
        name_column = 'name'
    elif 'title' in df.columns:
        name_column = 'title'
    else:
        # If neither exists, create a placeholder
        df['name'] = f"{brand} location in {city}"
        name_column = 'name'
    
    print(f"Using column '{name_column}' for place names")
    
    # Create embeddings
    print(f"Generating embeddings for {len(df)} place names...")
    vecs = _embed(df[name_column].tolist())
    print(f"Generated {len(vecs)} embeddings")
    
    # Create records with flexible field mapping
    records = []
    for i, (_, row) in enumerate(df.iterrows()):
        # Create a unique ID even if placeId is missing
        if 'placeId' in row:
            record_id = f"place-{row['placeId']}"
        else:
            record_id = f"place-{brand}-{city}-{i}"
            
        # Create metadata with required fields
        metadata = {
            "brand": brand,
            "city": city,
            "name": row[name_column]
        }
        
        # Add optional fields if available
        try:
            if 'totalScore' in row and pd.notna(row['totalScore']):
                metadata["rating"] = float(row['totalScore'])
            elif 'rating' in row and pd.notna(row['rating']):
                metadata["rating"] = float(row['rating'])
        except Exception as e:
            print(f"Warning: Could not convert rating for {row[name_column]}: {str(e)}")
            
        try:
            if 'reviewsCount' in row and pd.notna(row['reviewsCount']):
                metadata["reviews"] = int(row['reviewsCount'])
            elif 'reviews' in row and pd.notna(row['reviews']):
                metadata["reviews"] = int(row['reviews'])
        except Exception as e:
            print(f"Warning: Could not convert reviews for {row[name_column]}: {str(e)}")
            
        try:
            if 'gpsCoordinates' in row and isinstance(row['gpsCoordinates'], dict):
                if 'lat' in row['gpsCoordinates'] and 'lng' in row['gpsCoordinates']:
                    metadata["lat"] = row['gpsCoordinates']['lat']
                    metadata["lng"] = row['gpsCoordinates']['lng']
            elif all(coord in row for coord in ['latitude', 'longitude']):
                metadata["lat"] = row['latitude']
                metadata["lng"] = row['longitude']
        except Exception as e:
            print(f"Warning: Could not process coordinates for {row[name_column]}: {str(e)}")
            
        records.append((record_id, vecs[i], metadata))
    
    # Upsert to Pinecone
    if records:
        print(f"Upserting {len(records)} records to Pinecone...")
        INDEX.upsert(vectors=records, namespace="maps")
        print(f"Successfully upserted {len(records)} records to Pinecone")
    else:
        print(f"Warning: No records to upsert for {brand} in {city}")
    
    # Verify upsert
    try:
        stats = INDEX.describe_index_stats()
        if "maps" in stats.get("namespaces", {}):
            count = stats["namespaces"]["maps"].get("vector_count", 0)
            print(f"Verification: 'maps' namespace now has {count} vectors")
    except Exception as e:
        print(f"Warning: Could not verify upsert: {str(e)}")

def upsert_keywords(df: pd.DataFrame, city: str) -> None:
    # First, clear existing keyword data
    try:
        print(f"Clearing existing keyword data from Pinecone...")
        
        # Delete all data in the keywords namespace
        INDEX.delete(delete_all=True, namespace="keywords")
        
        print(f"Successfully cleared previous keyword data")
    except Exception as e:
        print(f"Warning: Could not clear previous keyword data: {str(e)}")
    
    # df: keyword, year, month, search_volume
    unique = df["keyword"].unique().tolist()
    print(f"Generating embeddings for {len(unique)} unique keywords...")
    vecs = _embed(unique)
    vec_map = dict(zip(unique, vecs))
    print(f"Generated embeddings for {len(vec_map)} unique keywords")
    
    try:
        # Check data types before creating records
        print("Checking data types in DataFrame...")
        print(f"DataFrame columns: {df.columns.tolist()}")
        print(f"DataFrame data types: {df.dtypes}")
        
        # Convert columns to appropriate types
        if 'year' in df.columns:
            df['year'] = pd.to_numeric(df['year'], errors='coerce').fillna(0).astype(int)
        
        if 'month' in df.columns:
            df['month'] = pd.to_numeric(df['month'], errors='coerce').fillna(0).astype(int)
            
        if 'search_volume' in df.columns:
            df['search_volume'] = pd.to_numeric(df['search_volume'], errors='coerce').fillna(0).astype(int)
        
        print("After conversion:")
        print(f"DataFrame data types: {df.dtypes}")
        
        # Create records
        records = []
        for row in df.itertuples(index=False):
            if row.keyword not in vec_map:
                print(f"Warning: No embedding found for keyword '{row.keyword}'")
                continue
                
            try:
                record_id = f"kw-{row.keyword}-{row.year}{row.month:02}"
                
                # Ensure all metadata fields are of proper types
                year = int(row.year) if hasattr(row, 'year') else 0
                month = int(row.month) if hasattr(row, 'month') else 0
                search_volume = int(row.search_volume) if hasattr(row, 'search_volume') else 0
                
                metadata = {
    "keyword": row.keyword,
    "year": year,
    "month": month,
    "search_volume": search_volume,
    "competition": getattr(row, 'competition', 0.0),
    "cpc": getattr(row, 'cpc', 0.0),
    "city": city
}

                records.append((record_id, vec_map[row.keyword], metadata))
            except Exception as e:
                print(f"Error creating record for keyword '{row.keyword}': {str(e)}")
        
        print(f"Created {len(records)} keyword records for upsert")
        
        # Upsert in smaller batches if there are many records
        if records:
            batch_size = 100
            for i in range(0, len(records), batch_size):
                batch = records[i:i+batch_size]
                print(f"Upserting batch {i//batch_size + 1}/{(len(records)//batch_size) + 1} ({len(batch)} records)...")
                INDEX.upsert(vectors=batch, namespace="keywords")
            
            print(f"Successfully upserted {len(records)} keyword records to Pinecone")
        else:
            print("Warning: No keyword records to upsert")
            
        # Verify upsert
        try:
            stats = INDEX.describe_index_stats()
            if "keywords" in stats.get("namespaces", {}):
                count = stats["namespaces"]["keywords"].get("vector_count", 0)
                print(f"Verification: 'keywords' namespace now has {count} vectors")
        except Exception as e:
            print(f"Warning: Could not verify upsert: {str(e)}")
            
    except Exception as e:
        print(f"Error in upsert_keywords: {str(e)}")
        import traceback
        traceback.print_exc()
