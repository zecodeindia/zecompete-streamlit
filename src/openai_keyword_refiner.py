# === NEW openai_keyword_refiner.py ===
"""
OpenAI Assistant + Smart Keyword Refiner
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

# === Core Functions ===

def get_assistant_id() -> str:
    return FIXED_ASSISTANT_ID

def refine_keywords_openai(keywords: List[str], brand_names: List[str], city: str) -> List[str]:
    """Call OpenAI Assistant only if needed."""
    try:
        assistant_id = FIXED_ASSISTANT_ID
        thread = client.beta.threads.create()

        message_content = f"""Here are the brand names: {', '.join(brand_names)}\nCity: {city}\n\nRaw keywords to refine:\n{json.dumps(keywords, indent=2)}\n\nPlease clean these keywords to focus only on brand+location pairs, removing keywords with intents like \"timings\", \"phone number\", etc.\nReturn ONLY the JSON array of clean keywords."""

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
                return json.loads(response_text[start_idx:end_idx])
        return keywords

    except Exception as e:
        logger.error(f"OpenAI refinement error: {str(e)}")
        return keywords

def smart_batch_refine_keywords(business_entries: List[Dict[str, str]], brand_names: List[str], city: str) -> List[str]:
    """
    New smarter refinement that checks:
    1. Business name first
    2. Then address fallback
    3. Then Google suggest
    """
    initial_keywords = []

    for entry in business_entries:
        name = entry.get("name", "").strip()
        address = entry.get("address", "").strip()

        if is_brand_location_pair(name):
            initial_keywords.append(name)
        else:
            location_guess = extract_location_from_address(address)
            if location_guess:
                brand = extract_brand_from_name(name, brand_names)
                initial_keywords.append(f"{brand} {location_guess}")
            else:
                brand = extract_brand_from_name(name, brand_names)
                initial_keywords.extend(get_google_suggest_keywords(brand))

    logger.info(f"Initial keyword count after brand+location logic: {len(initial_keywords)}")

    # Refine slightly using OpenAI if needed
    return refine_keywords_openai(initial_keywords, brand_names, city)

# === Helper to guess brand ===
def extract_brand_from_name(name: str, brand_names: List[str]) -> str:
    for brand in brand_names:
        if brand.lower() in name.lower():
            return brand
    return brand_names[0] if brand_names else "Unknown"

# === Main (for testing) ===
if __name__ == "__main__":
    entries = [
        {"name": "Zecode", "address": "Indiranagar, Bengaluru"},
        {"name": "Zecode Whitefield", "address": "Whitefield, Bengaluru"},
        {"name": "Zecode", "address": ""},
    ]
    brands = ["Zecode"]
    city = "Bengaluru"
    refined = smart_batch_refine_keywords(entries, brands, city)
    print(json.dumps(refined, indent=2))
