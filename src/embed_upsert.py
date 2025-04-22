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
    # Print column names to help with debugging
    print(f"Available columns: {df.columns.tolist()}")
    
    # Check if 'name' exists or try alternative column names
    if 'name' in df.columns:
        name_column = 'name'
    elif 'title' in df.columns:
        name_column = 'title'
    else:
        # If neither exists, create a placeholder
        df['name'] = f"{brand} location in {city}"
        name_column = 'name'
        
    # Try to get place ID safely
    place_id_col = 'placeId' if 'placeId' in df.columns else 'id' if 'id' in df.columns else None
    
    # Proceed with embedding
    vecs = _embed(df[name_column].tolist())
    
    # Create records for upsert
    records = []
    for i, (_, row) in enumerate(df.iterrows()):
        # Create safe ID
        if place_id_col:
            record_id = f"place-{row[place_id_col]}"
        else:
            record_id = f"place-{brand}-{city}-{i}"
            
        # Create metadata with safe access to fields
        metadata = {
            "brand": brand,
            "city": city,
            "name": row.get(name_column, f"{brand} location")
        }
        
        # Add rating if available
        if 'totalScore' in row:
            metadata["rating"] = float(row['totalScore'])
        elif 'rating' in row:
            metadata["rating"] = float(row['rating'])
            
        # Add reviews if available
        if 'reviewsCount' in row:
            metadata["reviews"] = int(row['reviewsCount'])
        elif 'reviews' in row:
            metadata["reviews"] = int(row['reviews'])
            
        # Add coordinates if available
        if 'gpsCoordinates' in row and isinstance(row['gpsCoordinates'], dict):
            if 'lat' in row['gpsCoordinates'] and 'lng' in row['gpsCoordinates']:
                metadata["lat"] = row['gpsCoordinates']['lat']
                metadata["lng"] = row['gpsCoordinates']['lng']
        elif 'latitude' in row and 'longitude' in row:
            metadata["lat"] = row['latitude']
            metadata["lng"] = row['longitude']
            
        records.append((record_id, vecs[i], metadata))
    
    # Upsert to Pinecone
    if records:
        INDEX.upsert(records, namespace="maps")
    else:
        print(f"Warning: No records to upsert for {brand} in {city}")

def upsert_keywords(df: pd.DataFrame, city: str) -> None:
    # df: keyword, year, month, search_volume
    unique = df["keyword"].unique().tolist()
    vecs   = _embed(unique)
    vec_map = dict(zip(unique, vecs))
    INDEX.upsert([
        (f"kw-{row.keyword}-{row.year}{row.month:02}",
         vec_map[row.keyword],
         {"keyword": row.keyword,
          "year": int(row.year), "month": int(row.month),
          "search_volume": int(row.search_volume),
          "city": city})
        for row in df.itertuples(index=False)
    ], namespace="keywords")