# Re-creating the full corrected openai_keyword_refiner.py after environment reset

corrected_code = """
\"\"\"
OpenAI Assistant + Smart Keyword Refiner (Strict Matching Version)
\"\"\"
import json, time, logging
from typing import List, Dict, Any

FIXED_ASSISTANT_ID = "asst_aaWtxqys7xZZph6YQOSVP6Wk"

# === Helpers ===

def is_brand_location_pair(business_name: str) -> bool:
    return len(business_name.strip().split()) >= 2

def extract_location_from_address(address: str) -> str:
    if not address:
        return ""
    parts = address.split(",")
    return parts[0].strip() if parts else ""

def get_google_suggest_keywords(brand: str) -> List[str]:
    return [f"{brand} Whitefield", f"{brand} Indiranagar", f"{brand} Jayanagar"]

def extract_brand_from_name(name: str, brand_names: List[str]) -> str:
    for brand in brand_names:
        if brand.lower() in name.lower():
            return brand
    return brand_names[0] if brand_names else "Unknown"

def post_validate_keywords(keywords: List[str], brand_names: List[str]) -> List[str]:
    cleaned_keywords = set()
    blacklist = ["timing", "phone", "contact", "location", "store", "reviews", "open", "near me"]

    for kw in keywords:
        kw_clean = kw.replace(",", "").replace("  ", " ").strip()
        if not kw_clean:
            continue
        if any(bad_word in kw_clean.lower() for bad_word in blacklist):
            continue
        if len(kw_clean.split()) < 2:
            continue
        if not any(brand.lower() in kw_clean.lower() for brand in brand_names):
            continue
        cleaned_keywords.add(kw_clean)

    return list(cleaned_keywords)

# === Core Functions ===

def smart_batch_refine_keywords(business_entries: List[Dict[str, str]], brand_names: List[str], city: str) -> List[str]:
    allowed_keywords = set()
    
    for entry in business_entries:
        name = entry.get("name", "").strip()
        address = entry.get("address", "").strip()

        if is_brand_location_pair(name):
            cleaned_name = name.replace(",", "").replace("  ", " ").strip()
            allowed_keywords.add(cleaned_name)
        else:
            location_guess = extract_location_from_address(address)
            if location_guess:
                brand = extract_brand_from_name(name, brand_names)
                allowed_keywords.add(f"{brand} {location_guess}".strip())
            else:
                brand = extract_brand_from_name(name, brand_names)
                for suggest in get_google_suggest_keywords(brand):
                    allowed_keywords.add(suggest.strip())

    final_keywords = post_validate_keywords(list(allowed_keywords), brand_names)
    return final_keywords

# === Main (for testing) ===
if __name__ == "__main__":
    entries = [
        {"name": "Zecode RR Nagar", "address": "RR Nagar, Bengaluru"},
        {"name": "Zecode HSR Layout", "address": "HSR Layout, Bengaluru"},
        {"name": "Zecode", "address": "Indiranagar, Bengaluru"}
    ]
    brands = ["Zecode"]
    city = "Bengaluru"
    refined = smart_batch_refine_keywords(entries, brands, city)
    print(json.dumps(refined, indent=2))
"""

# Save to file
output_path = "/mnt/data/openai_keyword_refiner_corrected.py"
with open(output_path, "w") as f:
    f.write(corrected_code)

output_path
