# Building the fully corrected openai_keyword_refiner.py based on strict business name locking
corrected_final_code = """
\"\"\"
OpenAI-Free Strict Keyword Refiner (Final Clean Version)
\"\"\"
import json
from typing import List, Dict

# === Helpers ===

def is_brand_location_pair(business_name: str) -> bool:
    return len(business_name.strip().split()) >= 2

def clean_business_name(name: str) -> str:
    \"\"\"Clean the business name by removing commas, fixing spaces, and trimming.\"\"\"
    return name.replace(",", "").replace("  ", " ").strip()

def post_validate_keywords(keywords: List[str], brand_names: List[str]) -> List[str]:
    \"\"\"\n    Only keep keywords that:\n    - Contain a known brand name\n    - Have at least 2 words (brand + location)\n    - Do not contain blacklisted junk words\n    \"\"\"\n    cleaned_keywords = set()\n    blacklist = ["timing", "phone", "contact", "location", "store", "reviews", "open", "near me"]

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

# === Core Function ===

def smart_batch_refine_keywords(business_entries: List[Dict[str, str]], brand_names: List[str], city: str) -> List[str]:
    \"\"\"Strict keyword locking from business names only.\"\"\"
    allowed_keywords = set()

    for entry in business_entries:
        name = entry.get("name", "").strip()

        if is_brand_location_pair(name):
            cleaned_name = clean_business_name(name)
            allowed_keywords.add(cleaned_name)
        else:
            continue  # ❌ Do not fallback to address guessing or Google suggest

    final_keywords = post_validate_keywords(list(allowed_keywords), brand_names)
    return final_keywords

# === Main (for testing) ===
if __name__ == "__main__":
    entries = [
        {\"name\": \"Zecode RR Nagar\"},
        {\"name\": \"Zecode HSR Layout\"},
        {\"name\": \"Zecode Vignan Nagar\"},
        {\"name\": \"Zecode Kammanahalli\"},
        {\"name\": \"Zecode Vidyaranyapura\"},
        {\"name\": \"Zecode Hesarghatta Road\"},
        {\"name\": \"Zecode TC Palya Road\"},
        {\"name\": \"Zecode Basaveshwar Nagar\"},
        {\"name\": \"Zecode Yelahanka\"},
        {\"name\": \"Zecode Nagavara\"}
    ]

    brand_names = [\"Zecode\"]
    city = \"Bengaluru\"

    refined = smart_batch_refine_keywords(entries, brand_names, city)
    print(json.dumps(refined, indent=2))
"""

# Save corrected file
final_output_path = "/mnt/data/openai_keyword_refiner_final_corrected.py"
with open(final_output_path, "w") as f:
    f.write(corrected_final_code)

final_output_path
