import pandas as pd
import traceback
from src.scrape_maps import run_scrape
from src.fetch_volume import fetch_volume
from src.embed_upsert import upsert_places, upsert_keywords
from src.config import secret
from openai import OpenAI

client = OpenAI(api_key=secret("OPENAI_API_KEY"))

def suggest_keywords(names: list[str]) -> list[str]:
    print(f"Generating keywords for {len(names)} place names")
    prompt = ("Give comma‑separated search phrases (≤3 words) "
              "to find these stores:\n\n" + "\n".join(names))
    rsp = client.chat.completions.create(
        model="gpt-4o-mini",
        messages=[{"role": "user", "content": prompt}],
        temperature=0.3
    )
    keywords = [kw.strip() for kw in rsp.choices[0].message.content.split(",") if kw.strip()]
    print(f"Generated {len(keywords)} keywords: {keywords}")
    return keywords

def tidy_volume(vol_result: dict) -> pd.DataFrame:
    print(f"Processing volume data")
    rows = []
    for kw in vol_result:
        for m in kw["keyword_info"]["monthly_searches"]:
            rows.append({
                "keyword": kw["keyword"],
                "year": m["year"],
                "month": m["month"],
                "search_volume": m["search_volume"]
            })
    df = pd.DataFrame(rows)
    print(f"Created DataFrame with {len(df)} rows")
    return df

def run(brand: str, city: str) -> None:
    try:
        print(f"======= Starting pipeline for {brand} in {city} =======")
        
        # Step 1: Scrape places from Google Maps via Apify
        print(f"Step 1: Scraping places from Google Maps...")
        places = run_scrape(brand, city)
        
        if not places:
            print(f"Warning: No places found for {brand} in {city}")
            return
            
        print(f"Received {len(places)} places from Apify")
        if places:
            print(f"First place sample: {places[0]}")
        
        # Step 2: Convert to DataFrame and upsert to Pinecone
        print(f"Step 2: Converting to DataFrame and upserting to Pinecone...")
        df_places = pd.json_normalize(places)
        print(f"Created DataFrame with shape {df_places.shape}")
        print(f"DataFrame columns: {df_places.columns.tolist()}")
        
        if not df_places.empty:
            print(f"First row: {df_places.iloc[0].to_dict()}")
        
        upsert_places(df_places, brand, city)
        print("Places upsert completed")

        # Step 3: Generate keywords and fetch search volumes
        print(f"Step 3: Generating keywords and fetching search volumes...")
        
        # Only proceed with keywords if we have place names
        name_col = None
        for col in ['name', 'title']:
            if col in df_places.columns:
                name_col = col
                break
                
        if name_col and not df_places.empty:
            print(f"Using '{name_col}' column for keyword generation")
            keywords = suggest_keywords(df_places[name_col].tolist())
        else:
            # Fallback to using brand name
            print(f"No place names found, using brand name for keywords")
            keywords = suggest_keywords([f"{brand} in {city}"])
        
        # Step 4: Fetch search volume data
        print(f"Step 4: Fetching search volume data for {len(keywords)} keywords...")
        try:
            vol_raw = fetch_volume(keywords)
            print(f"Received volume data for {len(vol_raw)} keywords")
            df_kw = tidy_volume(vol_raw)
            
            # Step 5: Upsert keyword data to Pinecone
            print(f"Step 5: Upserting keyword data to Pinecone...")
            upsert_keywords(df_kw, city)
            print("Keywords upsert completed")
        except Exception as e:
            print(f"Error in keyword processing: {str(e)}")
            traceback.print_exc()
        
        print(f"======= Pipeline completed for {brand} in {city} =======")
        
    except Exception as e:
        print(f"Error in run pipeline for {brand} in {city}: {str(e)}")
        traceback.print_exc()
