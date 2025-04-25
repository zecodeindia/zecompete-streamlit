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
    # First, clear existing data for this brand and city
    try:
        print(f"Clearing existing data for {brand} in {city} from Pinecone...")
        
        # Option 1: Delete all data in the maps namespace (most aggressive)
        INDEX.delete(delete_all=True, namespace="maps")
        
        # Option 2: Delete data with specific IDs (more targeted)
        # This is commented out as Option 1 is used for a clean slate
        # ids_to_delete = [f"place-{brand}-{city}-{i}" for i in range(100)]  # Adjust range as needed
        # if ids_to_delete:
        #     INDEX.delete(ids=ids_to_delete, namespace="maps")
        
        print(f"Successfully cleared previous data for {brand} in {city}")
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
    vecs = _embed(df[name_column].tolist())
    
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
        except:
            pass
            
        try:
            if 'reviewsCount' in row and pd.notna(row['reviewsCount']):
                metadata["reviews"] = int(row['reviewsCount'])
            elif 'reviews' in row and pd.notna(row['reviews']):
                metadata["reviews"] = int(row['reviews'])
        except:
            pass
            
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
        INDEX.upsert(vectors=records, namespace="maps")
        print(f"Successfully upserted {len(records)} records to Pinecone")
    else:
        print(f"Warning: No records to upsert for {brand} in {city}")

def upsert_keywords(df: pd.DataFrame, city: str) -> None:
    # First, clear existing keyword data
    try:
        print(f"Clearing existing keyword data for {city} from Pinecone...")
        
        # Delete all data in the keywords namespace
        INDEX.delete(delete_all=True, namespace="keywords")
        
        print(f"Successfully cleared previous keyword data for {city}")
    except Exception as e:
        print(f"Warning: Could not clear previous keyword data: {str(e)}")
    
    # df: keyword, year, month, search_volume
    unique = df["keyword"].unique().tolist()
    print(f"Generating embeddings for {len(unique)} unique keywords...")
    vecs = _embed(unique)
    vec_map = dict(zip(unique, vecs))
    
    try:
        records = [
            (f"kw-{row.keyword}-{row.year}{row.month:02}",
             vec_map[row.keyword],
             {"keyword": row.keyword,
              "year": int(row.year), 
              "month": int(row.month),
              "search_volume": int(row.search_volume),
              "city": city})
            for row in df.itertuples(index=False)
            if row.keyword in vec_map  # Safety check
        ]
        
        print(f"Created {len(records)} keyword records for upsert")
        
        if records:
            INDEX.upsert(vectors=records, namespace="keywords")
            print(f"Successfully upserted {len(records)} keyword records to Pinecone")
        else:
            print("Warning: No keyword records to upsert")
    except Exception as e:
        print(f"Error in upsert_keywords: {str(e)}")
