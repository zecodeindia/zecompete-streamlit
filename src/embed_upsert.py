from typing import Iterable, Dict, List
import pinecone, pandas as pd
from openai import OpenAI
from src.config import secret

# --- init Pinecone & OpenAI once ---------------------------------
pinecone.init(api_key=secret("PINECONE_API_KEY"), environment=secret("PINECONE_ENV"))
INDEX = pinecone.Index("zecompete")

client = OpenAI(api_key=secret("OPENAI_API_KEY"))
EMBED_MODEL = "text-embedding-3-small"          # 1536‑dim

# --- helpers -----------------------------------------------------
def _embed(texts: List[str]) -> List[List[float]]:
    res = client.embeddings.create(model=EMBED_MODEL, input=texts)
    return [d.embedding for d in res.data]

def upsert_places(df: pd.DataFrame, brand: str, city: str) -> None:
    vecs = _embed(df["name"].tolist())
    INDEX.upsert([
        (f"place-{r.placeId}", vec, {
            "brand": brand,
            "city": city,
            "name": r.name,
            "rating": float(r.totalScore),
            "reviews": int(r.reviewsCount),
            "lat": r.gpsCoordinates["lat"],
            "lng": r.gpsCoordinates["lng"]
        })
        for r, vec in zip(df.itertuples(), vecs)
    ], namespace="maps")

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
