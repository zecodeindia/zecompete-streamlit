# Modified src/openai_keyword_refiner.py
"""
OpenAI Assistant integration for keyword refinement.
This module provides functions to clean and refine keywords using a specific OpenAI Assistant.
"""

import json
import time
from typing import List, Dict, Any
import logging
import streamlit as st  # Import Streamlit for UI feedback
from openai import OpenAI
from src.config import secret

# Initialize OpenAI client
client = OpenAI(api_key=secret("OPENAI_API_KEY"))

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Fixed Assistant ID - use this specific Assistant instead of creating new ones
FIXED_ASSISTANT_ID = "asst_aaWtxqys7xZZph6YQOSVP6Wk"

def get_assistant_id() -> str:
    """
    Return the fixed Assistant ID.
    
    Returns:
        The ID of the Assistant to use
    """
    return FIXED_ASSISTANT_ID

def refine_keywords(keywords: List[str], brand_names: List[str], city: str) -> List[str]:
    """
    Refine keywords using the fixed OpenAI Assistant.
    
    Args:
        keywords: List of raw keywords to refine
        brand_names: List of brand names for context
        city: City name for context
        
    Returns:
        List of refined keywords
    """
    try:
        # Use the fixed assistant ID
        assistant_id = FIXED_ASSISTANT_ID
        logger.info(f"Using fixed Assistant ID: {assistant_id}")
            
        # Create a thread
        thread = client.beta.threads.create()
        
        # Prepare the message content
        message_content = f"""Here are the brand names: {', '.join(brand_names)}
City: {city}

Raw keywords to refine:
{json.dumps(keywords, indent=2)}

Please clean these keywords to focus only on brand+location pairs, removing keywords with intents like "timings", "phone number", etc.
Return ONLY the JSON array of clean keywords."""
        
        # Add a message to the thread
        client.beta.threads.messages.create(
            thread_id=thread.id,
            role="user",
            content=message_content
        )
        
        # Run the assistant
        run = client.beta.threads.runs.create(
            thread_id=thread.id,
            assistant_id=assistant_id
        )
        
        # Show status in UI if running in Streamlit
        progress_placeholder = st.empty() if 'st' in globals() else None
        
        # Wait for the run to complete
        while run.status in ["queued", "in_progress"]:
            if progress_placeholder:
                progress_placeholder.write(f"Assistant status: {run.status}")
            time.sleep(1)
            run = client.beta.threads.runs.retrieve(
                thread_id=thread.id,
                run_id=run.id
            )
        
        if progress_placeholder:
            progress_placeholder.write(f"Assistant finished with status: {run.status}")
        
        if run.status != "completed":
            logger.error(f"Assistant run failed with status: {run.status}")
            return keywords  # Return original keywords if refinement fails
        
        # Get the response
        messages = client.beta.threads.messages.list(
            thread_id=thread.id
        )
        
        # Extract the last assistant message
        assistant_messages = [msg for msg in messages.data if msg.role == "assistant"]
        if not assistant_messages:
            logger.warning("No response from assistant")
            return keywords
        
        # Parse the JSON response
        try:
            response_text = assistant_messages[0].content[0].text.value
            # Try to extract JSON array from the response
            if "[" in response_text and "]" in response_text:
                start_idx = response_text.find("[")
                end_idx = response_text.rfind("]") + 1
                json_str = response_text[start_idx:end_idx]
                refined_keywords = json.loads(json_str)
                logger.info(f"Refined {len(keywords)} keywords to {len(refined_keywords)} keywords")
                return refined_keywords
            else:
                logger.warning("Response did not contain a JSON array")
                return keywords
        except Exception as e:
            logger.error(f"Error parsing assistant response: {str(e)}")
            return keywords
    
    except Exception as e:
        logger.error(f"Error refining keywords: {str(e)}")
        return keywords  # Return original keywords if an error occurs

def batch_refine_keywords(keywords: List[str], brand_names: List[str], city: str, batch_size: int = 100) -> List[str]:
    """
    Refine a large list of keywords in batches.
    
    Args:
        keywords: List of raw keywords to refine
        brand_names: List of brand names for context
        city: City name for context
        batch_size: Number of keywords to process in each batch
        
    Returns:
        List of refined keywords
    """
    if len(keywords) <= batch_size:
        return refine_keywords(keywords, brand_names, city)
    
    # Show batch processing in UI if running in Streamlit
    progress_bar = st.progress(0) if 'st' in globals() else None
    status_text = st.empty() if 'st' in globals() else None
    
    # Use the fixed Assistant ID for all batches
    assistant_id = FIXED_ASSISTANT_ID
    
    refined_keywords = []
    total_batches = (len(keywords) // batch_size) + (1 if len(keywords) % batch_size > 0 else 0)
    
    for i in range(0, len(keywords), batch_size):
        batch = keywords[i:i+batch_size]
        batch_num = i // batch_size + 1
        
        if status_text:
            status_text.write(f"Processing batch {batch_num}/{total_batches} ({len(batch)} keywords)")
        logger.info(f"Processing batch {batch_num}/{total_batches} ({len(batch)} keywords)")
        
        batch_refined = refine_keywords(batch, brand_names, city)
        refined_keywords.extend(batch_refined)
        
        if progress_bar:
            progress_bar.progress(batch_num / total_batches)
    
    # Remove duplicates
    refined_keywords = list(set(refined_keywords))
    
    if status_text:
        status_text.write(f"✅ Completed - refined {len(keywords)} keywords to {len(refined_keywords)} keywords")
    
    return refined_keywords

# Example usage
if __name__ == "__main__":
    test_keywords = [
        "Zecode Indiranagar timings",
        "Zecode RR Nagar address",
        "Zecode Whitefield phone number",
        "Zecode Bengaluru",
        "Zecode in Electronic City",
        "Zecode store directions"
    ]
    
    test_brand_names = ["Zecode"]
    test_city = "Bengaluru"
    
    refined = refine_keywords(test_keywords, test_brand_names, test_city)
    print(json.dumps(refined, indent=2))
