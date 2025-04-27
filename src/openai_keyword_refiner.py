# === FINAL ULTIMATE Corrected openai_keyword_refiner.py ===
"""
OpenAI Assistant + Smart Keyword Refiner with Post-Validation (Fully Cleaned Version)
"""
import json, time, logging
from typing import List, Dict, Any
import streamlit as st
from openai import OpenAI
from src.config import secret

client = OpenAI(api_key=secret("OPENAI_API_KEY"))
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

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
    # Placeholder — implement actual API later
    return [f"{brand} Whitefield", f"{brand} Indiranagar", f"{brand} Jayanagar"]

def post_validate_keywords(keywords: List[str], brand_names: List[str]) -> List[str]:
    """
    Post-process keywords after OpenAI cleaning:
    1. Remove junk entries
    2. Ensure brand + location exist
    3. Clean spaces
    4. Deduplicate
    """
    cleaned_keywords = set()
    blacklist = ["timing", "phone", "contact", "location", "store", "reviews", "open"]

    for kw in keywords:
        kw = kw.replace(",", "").replace("  ", " ").strip()
        if not kw:
            continue
        if any(bad_word in kw.lower() for bad_word in blacklist):
            continue
        if len(kw.split()) < 2:
            continue
        if not any(brand.lower() in kw.lower() for brand in brand_names):
            continue
        cleaned_keywords.add(kw)

    logger.info(f"✅ Post-validation complete. Final keywords: {len(cleaned_keywords)}")
    return list(cleaned_keywords)

# === Core Functions ===

def get_assistant_id() -> str:
    return FIXED_ASSISTANT_ID

def refine_keywords_openai(keywords: List[str], brand_names: List[str], city: str) -> List[str]:
    """Call OpenAI Assistant only if needed."""
    try:
        assistant_id = FIXED_ASSISTANT_ID
        thread = client.beta.threads.create()

        message_content = f"""Here are the brand names: {', '.join(brand_names)}\nCity: {city}\n\nRaw keywords to refine:\n{json.dumps(keywords, indent=2)}\n\nPlease clean these keywords to focus only on brand+location pairs, removing keywords with intents like \"timings\", \"phone number\", \"store\", \"location\", etc.\nPreserve locality suffixes like Road, Layout, Nagar.\nReturn ONLY a clean JSON array of brand + locality keywords."""

        client.beta.threads.messages.create(thread_id=thread.id, role="user", content=message_content)
        run = client.beta.threads.runs.create(thread_id=thread.id, assistant_id=assistant_id)

        progress_placeholder = st.empty() if 'st' in globals() else None

        while run.status in ["queued", "in_progress"]:
            if progress_placeholder:
                progress_placeholder.write(f"Assistant status: {run.status}")
            time.sleep(1)
            run = client.beta.threads.runs.retrieve(thread_id=thread.id, run_id=run.id)

        if run.status != "completed":
            logger.error(f"Assistant run failed: {run.status}")
            return keywords

        messages = client.beta.threads.messages.list(thread_id=thread.id)
        assistant_messages = [msg for msg in messages.data if msg.role == "assistant"]

        if assistant_messages:
            response_text = assistant_messages[0].content[0].text.value
            if "[" in response_text and "]" in response_text:
                start_idx = response_text.find("[")
                end_idx = response_text.rfind("]") + 1
                raw_keywords = json.loads(response_text[start_idx:end_idx])
                return post_validate_keywords(raw_keywords, brand_names)
        return post_validate_keywords(keywords, brand_names)

    except Exception as e:
        logger.error(f"OpenAI refinement error: {str(e)}")
        return post_validate_keywords(keywords, brand_names)

def smart_batch_refine_keywords(business_entries: List[Dict[str, str]], brand_names: List[str], city: str) -> List[str]:
    """
    Final smart refinement:
    1. Protect business names
    2. Cleaning commas and spaces
    3. Deduplicating
    4. Skipping OpenAI unless needed
    5. Post-validation after OpenAI
    """
    initial_keywords = set()

    for entry in business_entries:
        name = entry.get("name", "").strip()
        address = entry.get("address", "").strip()

        if is_brand_location_pair(name):
            cleaned_name = name.replace(",", "").replace("  ", " ").strip()
            initial_keywords.add(cleaned_name)
        else:
            location_guess = extract_location_from_address(address)
            if location_guess:
                brand = extract_brand_from_name(name, brand_names)
                initial_keywords.add(f"{brand} {location_guess}".strip())
            else:
                brand = extract_brand_from_name(name, brand_names)
                for suggest in get_google_suggest_keywords(brand):
                    initial_keywords.add(suggest.strip())

    keywords = list(initial_keywords)

    logger.info(f"✅ Final deduplicated keyword count: {len(keywords)}")

    # Check if refinement is needed
    needs_refinement = False
    for kw in keywords:
        if ("location" in kw.lower()) or (len(kw.strip().split()) < 2):
            needs_refinement = True
            break

    if not needs_refinement:
        logger.info("✅ Keywords look clean. Skipping OpenAI refinement.")
        return post_validate_keywords(keywords, brand_names)
    else:
        logger.info("🔵 Keywords need refinement. Sending to OpenAI.")
        return refine_keywords_openai(keywords, brand_names, city)

# === Helper to guess brand ===
def extract_brand_from_name(name: str, brand_names: List[str]) -> str:
    for brand in brand_names:
        if brand.lower() in name.lower():
            return brand
    return brand_names[0] if brand_names else "Unknown"

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
