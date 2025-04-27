# Modified src/test_keyword_refinement.py
"""
Test script to verify the OpenAI Assistant integration for keyword refinement.
Uses a fixed Assistant ID instead of creating a new one.
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

# Fixed Assistant ID - use this specific Assistant
FIXED_ASSISTANT_ID = "asst_aaWtxqys7xZZph6YQOSVP6Wk"

def test_keyword_refinement():
    """Test the keyword refinement with a sample list of keywords"""
    print("Testing keyword refinement with fixed Assistant ID...")
    
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
    print(f"Running the assistant with fixed ID: {FIXED_ASSISTANT_ID}...")
    run = client.beta.threads.runs.create(
        thread_id=thread.id,
        assistant_id=FIXED_ASSISTANT_ID
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
        print(f"Testing with fixed Assistant ID: {FIXED_ASSISTANT_ID}")
        test_keyword_refinement()
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
    st.markdown(f"""
    This tool tests the OpenAI Assistant integration for keyword refinement.
    It uses a fixed Assistant ID: `{FIXED_ASSISTANT_ID}`
    
    It will:
    1. Submit sample keywords for refinement
    2. Display the results
    """)
    
    # Custom keywords option
    custom_keywords = st.text_area(
        "Custom keywords to test (one per line, leave empty to use default examples):",
        placeholder="Zara Indiranagar timings\nH&M Phoenix Mall phone number\nMax Fashion JP Nagar store"
    )
    
    brands = st.text_input("Brand names (comma-separated):", "Zara, H&M, Max Fashion, Trends, Levi's")
    city = st.text_input("City:", "Bengaluru")
    
    if st.button("Run Test"):
        with st.spinner(f"Testing keyword refinement with Assistant {FIXED_ASSISTANT_ID}..."):
            try:
                # Get keywords (custom or default)
                if custom_keywords:
                    test_keywords = [kw.strip() for kw in custom_keywords.split("\n") if kw.strip()]
                else:
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
                
                # Get brand names
                brand_names = [b.strip() for b in brands.split(",") if b.strip()]
                
                # Prepare the message content
                message_content = f"""Here are the brand names: {', '.join(brand_names)}
City: {city}

Raw keywords to refine:
{json.dumps(test_keywords, indent=2)}

Please clean these keywords to focus only on brand+location pairs, removing keywords with intents like "timings", "phone number", etc.
Return ONLY the JSON array of clean keywords."""
                
                # Create a thread
                thread = client.beta.threads.create()
                
                # Add a message to the thread
                client.beta.threads.messages.create(
                    thread_id=thread.id,
                    role="user",
                    content=message_content
                )
                
                # Run the assistant
                with st.spinner(f"Running the assistant with ID {FIXED_ASSISTANT_ID}..."):
                    run = client.beta.threads.runs.create(
                        thread_id=thread.id,
                        assistant_id=FIXED_ASSISTANT_ID
                    )
                    
                    # Create a progress indicator
                    progress_bar = st.progress(0)
                    progress_text = st.empty()
                    
                    # Wait for the run to complete
                    while run.status in ["queued", "in_progress"]:
                        if run.status == "queued":
                            progress_text.text("Queued... waiting to start")
                            progress_bar.progress(10)
                        else:
                            progress_text.text("In progress... generating refined keywords")
                            progress_bar.progress(50)
                        
                        time.sleep(1)
                        run = client.beta.threads.runs.retrieve(
                            thread_id=thread.id,
                            run_id=run.id
                        )
                    
                    if run.status == "completed":
                        progress_bar.progress(100)
                        progress_text.text("Completed!")
                    else:
                        progress_bar.progress(100)
                        progress_text.text(f"Finished with status: {run.status}")
                
                if run.status != "completed":
                    st.error(f"❌ Run failed with status: {run.status}")
                    return
                
                # Get the response
                messages = client.beta.threads.messages.list(
                    thread_id=thread.id
                )
                
                # Extract the last assistant message
                assistant_messages = [msg for msg in messages.data if msg.role == "assistant"]
                if not assistant_messages:
                    st.error("❌ No response from assistant")
                    return
                
                # Parse the JSON response
                try:
                    response_text = assistant_messages[0].content[0].text.value
                    st.subheader("Response from OpenAI Assistant:")
                    st.code(response_text)
                    
                    # Try to extract JSON array from the response
                    if "[" in response_text and "]" in response_text:
                        start_idx = response_text.find("[")
                        end_idx = response_text.rfind("]") + 1
                        json_str = response_text[start_idx:end_idx]
                        refined_keywords = json.loads(json_str)
                        
                        # Show comparison
                        st.subheader("Before and After Comparison:")
                        
                        # Create columns for before/after
                        col1, col2 = st.columns(2)
                        
                        with col1:
                            st.markdown("**Original Keywords:**")
                            for kw in test_keywords:
                                st.write(f"- {kw}")
                        
                        with col2:
                            st.markdown("**Refined Keywords:**")
                            for kw in refined_keywords:
                                st.write(f"- {kw}")
                        
                        # Create a dataframe for more detailed comparison
                        comparison_data = []
                        for i, orig_kw in enumerate(test_keywords):
                            # Find if this keyword was kept in the refined list
                            kept = any(orig_kw.lower() in ref_kw.lower() for ref_kw in refined_keywords)
                            
                            comparison_data.append({
                                "Original Keyword": orig_kw,
                                "Kept?": "✅" if kept else "❌",
                                "Reason": "Intent words removed" if not kept and any(word in orig_kw.lower() for word in ["timings", "hours", "phone", "number", "contact", "review", "ratings", "directions", "address"]) else ""
                            })
                        
                        st.subheader("Detailed Analysis:")
                        st.dataframe(pd.DataFrame(comparison_data))
                        
                        # Summary
                        st.success(f"✅ Successfully refined {len(test_keywords)} keywords to {len(refined_keywords)} keywords")
                    else:
                        st.error("❌ Response did not contain a valid JSON array")
                except Exception as e:
                    st.error(f"❌ Error parsing assistant response: {str(e)}")
            except Exception as e:
                st.error(f"❌ Test failed with error: {str(e)}")
                import traceback
                st.code(traceback.format_exc())
