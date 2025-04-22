import pandas as pd
import traceback
from src.scrape_maps    import run_scrape
from src.fetch_volume   import fetch_volume
from src.embed_upsert   import upsert_places, upsert_keywords
from src.config         import secret
from openai             import OpenAI

client = OpenAI(api_key=secret("OPENAI_API_KEY"))

def suggest_keywords(names: list[str]) -> list[str]:
    prompt = ("Give comma‑separated search phrases (≤3 words) "
              "to find these stores:\n\n" + "\n".join(names))
    rsp = client.chat.completions.create(
        model="gpt-4o-mini",
        messages=[{"role": "user", "content": prompt}],
        temperature=0.3
    )
    return [kw.strip() for kw in rsp.choices[0].message.content.split(",") if kw.strip()]

def tidy_volume(vol_result: dict) -> pd.DataFrame:
    rows = []
    for kw in vol_result:
        for m in kw["keyword_info"]["monthly_searches"]:
            rows.append({
                "keyword": kw["keyword"],
                "year": m["year"],
                "month": m["month"],
                "search_volume": m["search_volume"]
            })
    return pd.DataFrame(rows)

def run(brand: str, city: str) -> None:
    try:
        print(f"Starting scrape for {brand} in {city}...")
        places = run_scrape(brand, city)
        print(f"Scrape complete, received {len(places)} places")
        
        if not places:
            print(f"Warning: No places found for {brand} in {city}")
            return
            
        print(f"Converting to DataFrame...")
        df_places = pd.json_normalize(places)
        print(f"Scraped data columns: {df_places.columns.tolist()}")
        print(f"First row sample: {df_places.iloc[0].to_dict()}")
        
        print(f"Upserting {len(df_places)} places to Pinecone...")
        upsert_places(df_places, brand, city)
        print("Places upsert complete")

        # Only proceed with keywords if we have place names
        name_col = 'name' if 'name' in df_places.columns else 'title' if 'title' in df_places.columns else None
        
        if name_col and len(df_places[name_col]) > 0:
            print(f"Generating keywords based on {len(df_places[name_col])} place names...")
            keywords = suggest_keywords(df_places[name_col].tolist())
            print(f"Generated {len(keywords)} keywords: {keywords}")
        else:
            # Fallback to using brand name
            print("No place names found, using brand name for keywords...")
            keywords = suggest_keywords([f"{brand} in {city}"])
            print(f"Generated {len(keywords)} keywords: {keywords}")
            
        print("Fetching search volume data...")
        vol_raw = fetch_volume(keywords)
        print("Processing volume data...")
        df_kw = tidy_volume(vol_raw)
        print(f"Upserting {len(df_kw)} keyword records to Pinecone...")
        upsert_keywords(df_kw, city)
        print("Keywords upsert complete")
        
        print(f"Pipeline complete for {brand} in {city}")
        
    except Exception as e:
        print(f"Error in run pipeline for {brand} in {city}: {str(e)}")
        print("Traceback:")
        traceback.print_exc()
