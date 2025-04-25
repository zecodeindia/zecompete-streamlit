# =============================================================================
# run_pipeline.py – Revised 25‑Apr‑2025
# =============================================================================
"""End‑to‑end competitor analysis: maps ➜ keywords ➜ volume ➜ Pinecone."""

from __future__ import annotations

import logging
from typing import List

import pandas as pd

from src.scrape_maps import run_scrape     # unchanged helper
from src.embed_upsert import upsert_places, upsert_keywords
from src.keyword_pipeline import generate_keywords_for_businesses, get_search_volumes

# -----------------------------------------------------------------------------
# Helpers
# -----------------------------------------------------------------------------

def _clean_places(raw: list[dict]) -> pd.DataFrame:
    df = pd.json_normalize(raw)
    keep_cols = [c for c in ["name", "title", "lat", "lng", "rating", "reviews"] if c in df.columns]
    return df[keep_cols].drop_duplicates("name").reset_index(drop=True)

# -----------------------------------------------------------------------------
# Main orchestrator
# -----------------------------------------------------------------------------

def run(brand: str, city: str) -> None:
    logging.info("=== Pipeline start ▸ %s | %s", brand, city)

    # 1️⃣  Scrape Google Maps via Apify
    places_raw = run_scrape(brand, city)
    if not places_raw:
        logging.warning("No places returned — aborting.")
        return

    df_places = _clean_places(places_raw)
    upsert_places(df_places, brand, city)

    # 2️⃣  Keyword generation
    name_col = "name" if "name" in df_places.columns else df_places.columns[0]
    keywords: List[str] = generate_keywords_for_businesses(df_places[name_col].tolist(), city)
    if not keywords:
        logging.warning("Keyword generation produced zero rows; aborting.")
        return

    # 3️⃣  Search volume fetch + tidy
    df_kw = get_search_volumes(keywords)
    df_kw = df_kw.assign(city=city).query("search_volume > 0")

    # 4️⃣  Upsert keyword data
    upsert_keywords(df_kw, city)

    logging.info(
        "Pipeline finished ✓  %s places → %s keywords (%s volume rows)",
        len(df_places), len(keywords), len(df_kw)
    )

# -----------------------------------------------------------------------------
if __name__ == "__main__":
    import argparse, sys
    logging.basicConfig(level=logging.INFO, format="%(levelname)s | %(message)s")

    p = argparse.ArgumentParser()
    p.add_argument("brand")
    p.add_argument("city")
    args = p.parse_args()

    run(args.brand, args.city)
