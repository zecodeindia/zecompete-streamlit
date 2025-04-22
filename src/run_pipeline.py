import pandas as pd
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
        places = run_scrape(brand, city)
        if not places:
            print(f"Warning: No places found for {brand} in {city}")
            return
            
        df_places = pd.json_normalize(places)
        print(f"Scraped data columns: {df_places.columns.tolist()}")
        upsert_places(df_places, brand, city)

        # Only proceed with keywords if we have place names
        if 'name' in df_places.columns and len(df_places['name']) > 0:
            keywords = suggest_keywords(df_places['name'].tolist())
        else:
            # Fallback to using brand name
            keywords = suggest_keywords([f"{brand} in {city}"])
            
        vol_raw = fetch_volume(keywords)
        df_kw = tidy_volume(vol_raw)
        upsert_keywords(df_kw, city)
    except Exception as e:
        print(f"Error in run pipeline for {brand} in {city}: {str(e)}")
        # Continue execution rather than failing completely