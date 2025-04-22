import os
import sys
import itertools
import pandas as pd
import streamlit as st
import time
import json
import pinecone

# Instead of importing from src, try to load the code directly
# This is a workaround for the import issues

def load_module_code(file_path):
    """Load a Python module's code as a string"""
    if os.path.exists(file_path):
        with open(file_path, 'r') as f:
            return f.read()
    return None

# Set up the basic app
st.set_page_config(page_title="Competitor Mapper", layout="wide")
st.title("🗺️  Competitor Location & Demand Explorer")

# Check if required files exist
current_dir = os.path.dirname(os.path.abspath(__file__))
src_dir = os.path.join(current_dir, 'src')
files_exist = True

if not os.path.exists(src_dir):
    st.error(f"src directory not found at {src_dir}")
    files_exist = False
else:
    # List files in src directory
    src_files = os.listdir(src_dir)
    st.write(f"Files in src directory: {src_files}")

# Initialize Pinecone
try:
    pinecone.init(api_key=st.secrets["PINECONE_API_KEY"], environment="us-east-1")
    idx = pinecone.Index("zecompete")
    
    # Test if Pinecone is working
    stats = idx.describe_index_stats()
    st.write("Successfully connected to Pinecone index")
    
    # Display index stats
    st.subheader("Pinecone Index Stats")
    st.write("Dimension:", stats.get("dimension", "N/A"))
    st.write("Total vector count:", stats.get("total_vector_count", 0))
    
    # Handle namespaces
    namespaces = stats.get("namespaces", {})
    if namespaces:
        st.success(f"Found {len(namespaces)} namespaces: {', '.join(namespaces.keys())}")
        
        # Show sample data from each namespace
        for ns in namespaces:
            st.subheader(f"Sample data from '{ns}' namespace")
            try:
                # Fetch a few vectors to verify content
                query_response = idx.query(
                    vector=[0] * 1536,  # Dummy vector for metadata-only query
                    top_k=5,
                    namespace=ns,
                    include_metadata=True
                )
                
                if query_response.matches:
                    st.write(f"Found {len(query_response.matches)} records")
                    for i, match in enumerate(query_response.matches):
                        st.write(f"Record {i+1}:")
                        st.json(match.metadata)
                else:
                    st.warning(f"No records found in namespace '{ns}'")
            except Exception as e:
                st.error(f"Error querying namespace '{ns}': {str(e)}")
    else:
        st.warning("No namespaces found in the index.")
except Exception as e:
    st.error(f"Error connecting to Pinecone: {str(e)}")

# Diagnostic information section
st.subheader("Diagnostic Information")
st.write("Python Path:")
st.code("\n".join(sys.path))

st.write("Environment Variables:")
env_vars = {k: v for k, v in os.environ.items() if not k.startswith('_')}
st.code(json.dumps(env_vars, indent=2))

# Solution recommendation
st.subheader("Recommendation to Fix Import Issues")
st.write("""
It appears there's an issue with importing modules from the src directory. Here are steps to fix this:

1. Create an empty `__init__.py` file in the src directory (if it doesn't exist)
2. Simplify the import structure in your files
3. Make sure all required packages are installed in requirements.txt

For a long-term solution, consider reorganizing your code to avoid complex import structures.
""")
