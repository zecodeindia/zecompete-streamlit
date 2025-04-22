from typing import Iterable, Dict, List
from pinecone import Pinecone
import pandas as pd
from openai import OpenAI
from src.config import secret

# Updated Pinecone initialization
pc = Pinecone(api_key=secret("PINECONE_API_KEY"))
INDEX = pc.Index("zecompete")

client = OpenAI(api_key=secret("OPENAI_API_KEY"))
EMBED_MODEL = "text-embedding-3-small"

# --- helpers -----------------------------------------------------
def _embed(texts: List[str]) -> List[List[float]]:
    print(f"Generating embeddings for {len(texts)} texts")
    res = client.embeddings.create(model=EMBED_MODEL, input=texts)
    embeddings = [d.embedding for d in res.data]
    print(f"Successfully generated {len(embeddings)} embeddings")
    return embeddings

def upsert_places(df: pd.DataFrame, brand: str, city: str) -> None:
    print(f"Starting upsert_places for {brand} in {city} with DataFrame of shape {df.shape}")
    print(f"Available columns: {df.columns.tolist()}")
    
    # Check if 'name' exists or try alternative column names
    if 'name' in df.columns:
        name_column = 'name'
    elif 'title' in df.columns:
        name_column = 'title'
    else:
        # If neither exists, create a placeholder
        print("WARNING: No name or title column found, creating placeholder")
        df['name'] = f"{brand} location in {city}"
        name_column = 'name'
        
    # Get place ID safely
    place_id_col = None
    for possible_id in ['placeId', 'id', 'place_id']:
        if possible_id in df.columns:
            place_id_col = possible_id
            break
    
    # Proceed with embedding
    texts = df[name_column].tolist()
    print(f"Generating embeddings for {len(texts)} places")
    vecs = _embed(texts)
    
    # Create records for upsert
    records = []
    for i, (_, row) in enumerate(df.iterrows()):
        # Create safe ID
        if place_id_col and place_id_col in row:
            record_id = f"place-{row[place_id_col]}"
        else:
            record_id = f"place-{brand}-{city}-{i}"
            
        # Create metadata with safe access
        metadata = {
            "brand": brand,
            "city": city,
            "name": row.get(name_column, f"{brand} location")
        }
        
        # Add rating if available
        try:
            if 'totalScore' in row and pd.notna(row['totalScore']):
                metadata["rating"] = float(row['totalScore'])
            elif 'rating' in row and pd.notna(row['rating']):
                metadata["rating"] = float(row['rating'])
        except:
            pass
            
        # Add reviews if available
        try:
            if 'reviewsCount' in row and pd.notna(row['reviewsCount']):
                metadata["reviews"] = int(row['reviewsCount'])
            elif 'reviews' in row and pd.notna(row['reviews']):
                metadata["reviews"] = int(row['reviews'])
        except:
            pass
            
        # Add coordinates if available
        try:
            if 'gpsCoordinates' in row and isinstance(row['gpsCoordinates'], dict):
                if 'lat' in row['gpsCoordinates'] and 'lng' in row['gpsCoordinates']:
                    metadata["lat"] = row['gpsCoordinates']['lat']
                    metadata["lng"] = row['gpsCoordinates']['lng']
            elif all(coord in row for coord in ['latitude', 'longitude']):
                metadata["lat"] = row['latitude']
                metadata["lng"] = row['longitude']
        except:
            pass
                
        records.append((record_id, vecs[i], metadata))
    
    # Upsert to Pinecone
    if records:
        print(f"Upserting {len(records)} records to Pinecone 'maps' namespace")
        result = INDEX.upsert(vectors=records, namespace="maps")
        print(f"Upsert result: {result}")
    else:
        print(f"Warning: No records to upsert for {brand} in {city}")

def upsert_keywords(df: pd.DataFrame, city: str) -> None:
    print(f"Starting upsert_keywords for {city} with DataFrame of shape {df.shape}")
    
    # df: keyword, year, month, search_volume
    unique = df["keyword"].unique().tolist()
    print(f"Generating embeddings for {len(unique)} unique keywords")
    vecs = _embed(unique)
    vec_map = dict(zip(unique, vecs))
    
    records = []
    for row in df.itertuples(index=False):
        try:
            record_id = f"kw-{row.keyword}-{row.year}{row.month:02}"
            vector = vec_map[row.keyword]
            metadata = {
                "keyword": row.keyword,
                "year": int(row.year), 
                "month": int(row.month),
                "search_volume": int(row.search_volume),
                "city": city
            }
            records.append((record_id, vector, metadata))
        except Exception as e:
            print(f"Error processing keyword row: {e}")
    
    if records:
        print(f"Upserting {len(records)} keyword records to Pinecone 'keywords' namespace")
        result = INDEX.upsert(vectors=records, namespace="keywords")
        print(f"Keyword upsert result: {result}")
    else:
        print("Warning: No keyword records to upsert")
