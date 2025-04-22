import streamlit as st
from pinecone import Pinecone
from openai import OpenAI

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
        
        # Get index stats
        stats = index.describe_index_stats()
        st.write("Index stats:")
        st.json(stats)
        
        # Try simple upsert
        if st.button("Try Test Upsert", key="upsert_test"):
            # First get an embedding
            client = OpenAI(api_key=st.secrets["OPENAI_API_KEY"])
            response = client.embeddings.create(
                model="text-embedding-3-small",
                input=["Test Store"]
            )
            embedding = response.data[0].embedding
            
            # Create a test record
            test_record = ("test-id", embedding, {"name": "Test Store"})
            
            # Upsert to Pinecone
            index.upsert(vectors=[test_record], namespace="test")
            
            # Verify upsert
            updated_stats = index.describe_index_stats()
            st.write("Updated stats:")
            st.json(updated_stats)
            
            st.success("Test upsert successful!")
    except Exception as e:
        st.error(f"Pinecone test failed: {str(e)}")
        import traceback
        st.code(traceback.format_exc())
