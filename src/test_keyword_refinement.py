# test_keyword_refinement.py
"""
Test script to verify the OpenAI Assistant integration for keyword refinement.
Run this script directly to test the functionality.
"""

import json
import time
import pandas as pd
import streamlit as st
from openai import OpenAI

# Add an option to run in the Streamlit environment or standalone
try:
    from src.config import secret
    def get_api_key():
        return secret("OPENAI_API_KEY")
except ImportError:
    import os
    def get_api_key():
        return os.environ.get("OPENAI_API_KEY", st.secrets.get("OPENAI_API_KEY", ""))

# Initialize OpenAI client
client = OpenAI(api_key=get_api_key())

def create_test_assistant():
    """Create a test assistant and return its ID"""
    print("Creating test assistant...")
    
    assistant = client.beta.assistants.create(
        name="Keyword Refiner Test",
        instructions="""You are an expert in SEO and local search marketing.
Your task is to clean and refine keyword suggestions related to local businesses.
Given a list of raw keyword suggestions, you must:
Focus ONLY on keywords that include a recognizable brand name and a location (like "Zecode Indiranagar", "Zecode RR Nagar", "Zecode Whitefield Bengaluru").
If location is missing but can be inferred from context (e.g., nearby localities in the city), suggest the corrected form.
Remove any keywords that focus on unrelated intents such as "timings", "open hours", "phone number", "contact", "review", "ratings", "directions", unless explicitly instructed otherwise.
Ensure each final keyword is natural, realistic, and something a real user would type into Google when searching for a business outlet.
Format the output as a clean JSON array, like:
[
  "Zecode Indiranagar",
  "Zecode RR Nagar",
  "Zecode Whitefield Bengaluru"
]
Do not explain or add extra text. Return only the JSON list.""",
        model="gpt-4o-mini",
        tools=[],
    )
    
    print(f"Created assistant with ID: {assistant.id}")
    return assistant.id

def test_keyword_refinement(assistant_id):
    """Test the keyword refinement with a sample list of keywords"""
    print("Testing keyword refinement...")
    
    # Create a thread
    thread = client.beta.threads.create()
    
    # Sample test data
    test_keywords = [
        "Zara Indiranagar timings",
        "Zara Koramangala opening hours",
        "H&M Phoenix Mall phone number",
        "Max Fashion JP Nagar store",
        "Trends Commercial Street directions",
        "Levi's Forum Mall reviews",
        "FabIndia Jayanagar address",
        "Park Avenue HSR Layout",
        "Zudio Majestic contact",
        "Manyavar Whitefield wedding collection"
    ]
    
    brand_names = ["Zara", "H&M", "Max Fashion", "Trends", "Levi's", "FabIndia", "Park Avenue", "Zudio", "Manyavar"]
    city = "Bengaluru"
    
    # Prepare the message content
    message_content = f"""Here are the brand names: {', '.join(brand_names)}
City: {city}

Raw keywords to refine:
{json.dumps(test_keywords, indent=2)}

Please clean these keywords to focus only on brand+location pairs, removing keywords with intents like "timings", "phone number", etc.
Return ONLY the JSON array of clean keywords."""
    
    # Add a message to the thread
    client.beta.threads.messages.create(
        thread_id=thread.id,
        role="user",
        content=message_content
    )
    
    # Run the assistant
    print("Running the assistant...")
    run = client.beta.threads.runs.create(
        thread_id=thread.id,
        assistant_id=assistant_id
    )
    
    # Wait for the run to complete
    while run.status in ["queued", "in_progress"]:
        print(f"Current status: {run.status}...")
        time.sleep(2)
        run = client.beta.threads.runs.retrieve(
            thread_id=thread.id,
            run_id=run.id
        )
    
    print(f"Run completed with status: {run.status}")
    
    if run.status != "completed":
        print(f"Error: Run failed with status {run.status}")
        return
    
    # Get the response
    messages = client.beta.threads.messages.list(
        thread_id=thread.id
    )
    
    # Extract the last assistant message
    assistant_messages = [msg for msg in messages.data if msg.role == "assistant"]
    if not assistant_messages:
        print("Error: No response from assistant")
        return
    
    # Parse the JSON response
    try:
        response_text = assistant_messages[0].content[0].text.value
        print("\nResponse from assistant:")
        print(response_text)
        
        # Try to extract JSON array from the response
        if "[" in response_text and "]" in response_text:
            start_idx = response_text.find("[")
            end_idx = response_text.rfind("]") + 1
            json_str = response_text[start_idx:end_idx]
            refined_keywords = json.loads(json_str)
            
            print("\nRefined keywords:")
            for kw in refined_keywords:
                print(f"- {kw}")
            
            # Show before and after comparison
            print("\nBefore and After Comparison:")
            comparison = pd.DataFrame({
                "Original Keywords": test_keywords,
                "Refined Keywords": refined_keywords + [""] * (len(test_keywords) - len(refined_keywords)) if len(test_keywords) > len(refined_keywords) else refined_keywords[:len(test_keywords)]
            })
            print(comparison)
            
        else:
            print("Error: Response did not contain a JSON array")
    except Exception as e:
        print(f"Error parsing assistant response: {str(e)}")

def run_test():
    """Run the complete test"""
    try:
        assistant_id = create_test_assistant()
        test_keyword_refinement(assistant_id)
        print("\nTest completed successfully!")
    except Exception as e:
        print(f"Test failed with error: {str(e)}")
        import traceback
        print(traceback.format_exc())

# If running this script directly
if __name__ == "__main__":
    run_test()

# Streamlit interface for the test
def streamlit_test():
    """Interface for running the test in Streamlit"""
    st.title("OpenAI Assistant Keyword Refinement Test")
    
    # Display description
    st.markdown("""
    This tool tests the OpenAI Assistant integration for keyword refinement.
    It will:
    1. Create a test assistant
    2. Submit sample keywords for refinement
    3. Display the results
    """)
    
    # Custom keywords option
    custom_keywords = st.text_area(
        "Custom keywords to test (one per line, leave empty to use default examples):",
        placeholder="Zara Indiranagar timings\nH&M Phoenix Mall phone number\nMax Fashion JP Nagar store"
    )
    
    brands = st.text_input("Brand names (comma-separated):", "Zara, H&M, Max Fashion, Trends, Levi's")
    city = st.text_input("City:", "Bengaluru")
    
    if st.button("Run Test"):
        with st.spinner("Creating test assistant..."):
            try:
                assistant_id = create_test_assistant()
                st.success(f"Created test assistant with ID: {assistant_id}")
                
                # Get keywords (custom or default)
                if custom_keywords:
                    test_keywords = [kw.strip() for kw in custom_keywords.split("\n") if kw.strip
