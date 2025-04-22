import streamlit as st
from pinecone import Pinecone
from openai import OpenAI
import json
import time  # Add this import

st.set_page_config(page_title="Pinecone Debug", layout="wide")
st.title("Pinecone Troubleshooter")

# Show which API keys are available
st.subheader("API Keys")
for key in ["PINECONE_API_KEY", "OPENAI_API_KEY", "APIFY_TOKEN", "DFS_USER", "DFS_PASS"]:
    if key in st.secrets:
        st.write(f"✅ {key} is available (starts with '{st.secrets[key][:3]}...')")
    else:
        st.write(f"❌ {key} is not available")

# Test OpenAI
if st.button("Test OpenAI Connection", key="openai_test"):
    try:
        client = OpenAI(api_key=st.secrets["OPENAI_API_KEY"])
        response = client.embeddings.create(
            model="text-embedding-3-small",
            input=["Test"]
        )
        embedding = response.data[0].embedding
        st.success(f"OpenAI connection successful! Embedding dimension: {len(embedding)}")
    except Exception as e:
        st.error(f"OpenAI connection failed: {str(e)}")

# Test Pinecone
if st.button("Test Pinecone Connection", key="pinecone_test"):
    try:
        pc = Pinecone(api_key=st.secrets["PINECONE_API_KEY"])
        st.success("Pinecone client initialized successfully!")
        
        # Test index connection
        index = pc.Index("zecompete")
        st.success("Connected to Pinecone index 'zecompete'!")
        
        # Get index stats with error handling
        try:
            stats = index.describe_index_stats()
            # Display raw stats first
            st.write("Raw index stats response:")
            st.write(repr(stats))
            
            # Then try to display it as JSON
            st.write("Index stats:")
            if isinstance(stats, dict):
                st.write("Dimension:", stats.get("dimension", "Not available"))
                st.write("Total vectors:", stats.get("total_vector_count", "Not available"))
                st.write("Namespaces:", list(stats.get("namespaces", {}).keys()))
            else:
                st.warning(f"Stats not in expected format. Type: {type(stats)}")
                
        except Exception as e:
            st.error(f"Error getting index stats: {str(e)}")

    except Exception as e:
        st.error(f"Pinecone test failed: {str(e)}")
        import traceback
        st.code(traceback.format_exc())

# Add a new button for the test upsert
if st.button("Try Test Upsert", key="upsert_test"):
    try:
        # Initialize connections
        pc = Pinecone(api_key=st.secrets["PINECONE_API_KEY"])
        index = pc.Index("zecompete")
        client = OpenAI(api_key=st.secrets["OPENAI_API_KEY"])
        
        # Generate an embedding
        response = client.embeddings.create(
            model="text-embedding-3-small",
            input=["Test Store"]
        )
        embedding = response.data[0].embedding
        
        # Create a test record
        test_record = ("test-id-1", embedding, {"name": "Test Store"})
        
        # Log what we're about to upsert
        st.write(f"Upserting record with ID: test-id-1")
        st.write(f"Embedding dimension: {len(embedding)}")
        st.write(f"Metadata: {{'name': 'Test Store'}}")
        
        # Upsert to Pinecone
        result = index.upsert(
            vectors=[test_record], 
            namespace="test"
        )
        
        # Show the upsert result
        st.write("Upsert result:")
        st.write(result)
        
        # Try to verify the upsert with a query instead of stats
        st.write("Waiting 2 seconds for indexing...")
        time.sleep(2)  # Give a moment for indexing
        
        st.write("Querying for the inserted record...")
        query_result = index.query(
            vector=embedding,
            top_k=1,
            namespace="test",
            include_metadata=True
        )
        
        st.write("Query result:")
        st.write(query_result)
        
        st.success("Test upsert completed!")
    except Exception as e:
        st.error(f"Test upsert failed: {str(e)}")
        import traceback
        st.code(traceback.format_exc())
