from typing import Iterable, Dict, List
from pinecone import Pinecone
import pandas as pd
from openai import OpenAI
from src.config import secret
import traceback

# Updated Pinecone initialization
pc = Pinecone(api_key=secret("PINECONE_API_KEY"))
INDEX = pc.Index("zecompete")

client = OpenAI(api_key=secret("OPENAI_API_KEY"))
EMBED_MODEL = "text-embedding-3-small"

# --- helpers -----------------------------------------------------
def _embed(texts: List[str]) -> List[List[float]]:
    try:
        print(f"Generating embeddings for {len(texts)} texts")
        # Print a sample of the first text
        if texts and len(texts) > 0:
            print(f"Sample text for embedding: '{texts[0]}'")
            
        res = client.embeddings.create(model=EMBED_MODEL, input=texts)
        embeddings = [d.embedding for d in res.data]
        print(f"Successfully generated {len(embeddings)} embeddings")
        return embeddings
    except Exception as e:
        print(f"Error in _embed: {str(e)}")
        traceback.print_exc()
        raise

def upsert_places(df: pd.DataFrame, brand: str, city: str) -> None:
    try:
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
            
        # Try to get place ID safely
        place_id_col = 'placeId' if 'placeId' in df.columns else 'id' if 'id' in df.columns else None
        
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
                try:
                    if 'lat' in row['gpsCoordinates'] and 'lng' in row['gpsCoordinates']:
                        metadata["lat"] = row['gpsCoordinates']['lat']
                        metadata["lng"] = row['gpsCoordinates']['lng']
                except:
                    print(f"Error accessing gpsCoordinates for row {i}")
            elif 'latitude' in row and 'longitude' in row:
                metadata["lat"] = row['latitude']
                metadata["lng"] = row['longitude']
                
            records.append((record_id, vecs[i], metadata))
        
        # Upsert to Pinecone
        if records:
            print(f"Upserting {len(records)} records to Pinecone 'maps' namespace")
            INDEX.upsert(vectors=records, namespace="maps")
            print("Upsert to Pinecone successful")
        else:
            print(f"Warning: No records to upsert for {brand} in {city}")
            
    except Exception as e:
        print(f"Error in upsert_places: {str(e)}")
        traceback.print_exc()

def upsert_keywords(df: pd.DataFrame, city: str) -> None:
    try:
        print(f"Starting upsert_keywords for {city} with DataFrame of shape {df.shape}")
        
        # df: keyword, year, month, search_volume
        unique = df["keyword"].unique().tolist()
        print(f"Generating embeddings for {len(unique)} unique keywords")
        vecs = _embed(unique)
        vec_map = dict(zip(unique, vecs))
        
        records = [
            (f"kw-{row.keyword}-{row.year}{row.month:02}",
             vec_map[row.keyword],
             {"keyword": row.keyword,
              "year": int(row.year), "month": int(row.month),
              "search_volume": int(row.search_volume),
              "city": city})
            for row in df.itertuples(index=False)
        ]
        
        if records:
            print(f"Upserting {len(records)} keyword records to Pinecone 'keywords' namespace")
            INDEX.upsert(vectors=records, namespace="keywords")
            print("Keyword upsert to Pinecone successful")
        else:
            print("Warning: No keyword records to upsert")
            
    except Exception as e:
        print(f"Error in upsert_keywords: {str(e)}")
        traceback.print_exc()
