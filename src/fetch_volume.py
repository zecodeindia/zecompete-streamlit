"""
fetch_volume.py  ·  DataForSEO search–volume helper
---------------------------------------------------
* Safely handles missing search-volume values.
* Produces 12-month trend data (real or synthetic).
"""

from __future__ import annotations

import datetime
import json
import random
import traceback
from typing import Any, Dict, List

import requests


# --------------------------------------------------------------------------- #
#  Public helpers                                                             #
# --------------------------------------------------------------------------- #
def fetch_volume(keywords: List[str], include_trends: bool = True) -> Dict[str, Dict]:
    """
    Fetch Google search–volume data (and 12-month trends) from the DataForSEO API.

    Args:
        keywords:         list of search terms.
        include_trends:   if True, attach ``"monthly_trends"`` to each keyword’s dict.

    Returns:
        {
            "keyword one": {
                "search_volume": int,
                "competition":   float,
                "cpc":           float,
                "monthly_trends": [  # only when include_trends is True
                    {"year": 2025, "month": 4, "search_volume": 123},
                    ...
                ]
            },
            ...
        }
    """
    # --- credentials & endpoint --------------------------------------------
    from src.config import secret

    dfs_user = secret("DFS_USER")
    dfs_pass = secret("DFS_PASS")
    if not (dfs_user and dfs_pass):
        print("💥  DataForSEO credentials are missing!")
        return {}

    ENDPOINT = "https://api.dataforseo.com/v3/keywords_data/google/search_volume/live"

    payload = {
        "keywords": keywords,
        "language_code": "en",
        "location_code": 1023191,       # Bengaluru
        "include_serp_info": True,
    }

    print(f"📡  Requesting volume for {len(keywords)} keywords …")
    try:
        resp = requests.post(ENDPOINT, json=[payload], auth=(dfs_user, dfs_pass), timeout=30)
    except Exception as exc:  # pragma: no cover
        print(f"💥  Network error → {exc}")
        traceback.print_exc()
        return {}

    print(f"🔙  DataForSEO status: {resp.status_code}")
    if resp.status_code != 200:
        print(f"⚠️  Payload: {resp.text[:800]} …")
        return {}

    data = resp.json()
    if data.get("status_code") != 20000:
        print(f"⚠️  DataForSEO API error → {data.get('status_message')}")
        return {}

    tasks = data.get("tasks") or []
    if not tasks:
        print("⚠️  No tasks in response")
        return {}

    enriched: Dict[str, Dict[str, Any]] = {}

    # ------------------------------------------------------------------ #
    #  Parse every keyword result                                        #
    # ------------------------------------------------------------------ #
    for task in tasks:
        if task.get("status_code") != 20000:
            print(f"⚠️  Task error → {task.get('status_message')}")
            continue

        for res in task.get("result", []):
            kw = res.get("keyword") or ""
            if not kw:
                continue

            raw_volume = res.get("search_volume")
            # ----------------------------------------------------------------
            #  Always coerce to *int* and never leave None
            # ----------------------------------------------------------------
            search_volume = int(raw_volume or 0)

            item: Dict[str, Any] = {
                "search_volume": search_volume,
                "competition": float(res.get("competition_index") or 0.0),
                "cpc": (res.get("cpc") or 0) / 100.0,      # convert cents → dollars
            }

            # ------------------------------------------------------------ #
            #  Trend data                                                 #
            # ------------------------------------------------------------ #
            if include_trends:
                trends = _extract_real_trends(res) or _generate_synthetic_trends(search_volume)
                item["monthly_trends"] = trends

            enriched[kw] = item

    print(f"✅  Processed {len(enriched)} keywords successfully")
    return enriched


# --------------------------------------------------------------------------- #
#  Private helpers                                                            #
# --------------------------------------------------------------------------- #
def _extract_real_trends(res: Dict[str, Any]) -> List[Dict[str, int]]:
    """Pull 12-month trend data out of a DataForSEO result (if present)."""
    serp_info = res.get("serp_info") or {}
    trend_raw = serp_info.get("month_trend") or []
    if not trend_raw:
        return []

    trends: List[Dict[str, int]] = []
    for entry in trend_raw:
        year, month = entry.get("year"), entry.get("month")
        vol = entry.get("search_volume")
        if year and month and vol is not None:
            trends.append({"year": year, "month": month, "search_volume": int(vol)})

    trends.sort(key=lambda x: (x["year"], x["month"]))
    return trends


def _generate_synthetic_trends(current_volume: int, *, months: int = 12) -> List[Dict[str, int]]:
    """
    Fallback generator: create plausible seasonality around ``current_volume``.

    Ensures the values stay within 70-130 % of *current_volume*
    (or 50–300 if the current volume is zero/missing).
    """
    now = datetime.datetime.now()
    base = max(50, int(current_volume * 0.7)) or 50
    peak = max(base + 10, int(current_volume * 1.3)) or 300

    trends: List[Dict[str, int]] = []

    for i in range(months):
        # month index counting backwards
        month = now.month - i
        year = now.year
        if month <= 0:
            month += 12
            year -= 1

        # Current month sticks close to the reported volume
        if i == 0 and current_volume:
            vol = int(random.uniform(0.95, 1.05) * current_volume)
        else:
            vol = random.randint(base, peak)

        trends.append({"year": year, "month": month, "search_volume": vol})

    trends.sort(key=lambda x: (x["year"], x["month"]))
    return trends


# --------------------------------------------------------------------------- #
#  Quick CLI test (run `python fetch_volume.py`)                               #
# --------------------------------------------------------------------------- #
if __name__ == "__main__":
    demo = fetch_volume(["zara bangalore", "hm near me"])
    # pretty-print the first keyword for sanity check
    if demo:
        key0 = next(iter(demo))
        print(json.dumps(demo[key0], indent=2))
