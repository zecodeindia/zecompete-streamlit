import streamlit as st
import time
import pandas as pd
from openai import OpenAI

st.set_page_config(page_title="Pinecone Debug", layout="wide")
st.title("🔍 Pinecone Connection Debugger")

# Display all secrets (masking the actual values for security)
st.subheader("Secret Keys Status")
secrets_to_check = ["PINECONE_API_KEY", "OPENAI_API_KEY", "APIFY_TOKEN", "DFS_USER", "DFS_PASS"]
for key in secrets_to_check:
    if key in st.secrets:
        st.write(f"✅ {key}: Available (starts with '{st.secrets[key][:3]}...')")
    else:
        st.write(f"❌ {key}: Missing")

# Test OpenAI connection
st.subheader("1. Testing OpenAI Connection")
if st.button("Test OpenAI", key="test_openai"):
    try:
        client = OpenAI(api_key=st.secrets["OPENAI_API_KEY"])
        response = client.embeddings.create(
            model="text-embedding-3-small",
            input=["Test text"]
        )
        embedding = response.data[0].embedding
        st.success(f"✅ OpenAI connection successful - embedding dimension: {len(embedding)}")
    except Exception as e:
        st.error(f"❌ OpenAI connection failed: {str(e)}")

# Test Pinecone connection (just initialization)
st.subheader("2. Testing Pinecone Initialization")
if st.button("Test Pinecone Init", key="test_pinecone_init"):
    try:
        from pinecone import Pinecone
        pc = Pinecone(api_key=st.secrets["PINECONE_API_KEY"])
        st.success("✅ Pinecone initialization successful")
    except Exception as e:
        st.error(f"❌ Pinecone initialization failed: {str(e)}")

# Test Pinecone index connection
st.subheader("3. Testing Pinecone Index Connection")
if st.button("Test Index Connection", key="test_index"):
    try:
        from pinecone import Pinecone
        pc = Pinecone(api_key=st.secrets["PINECONE_API_KEY"])
        index = pc.Index("zecompete")
        st.success("✅ Pinecone index connection successful")
    except Exception as e:
        st.error(f"❌ Pinecone index connection failed: {str(e)}")

# Test Pinecone index stats
st.subheader("4. Testing Pinecone Index Stats")
if st.button("Test Index Stats", key="test_stats"):
    try:
        from pinecone import Pinecone
        pc = Pinecone(api_key=st.secrets["PINECONE_API_KEY"])
        index = pc.Index("zecompete")
        stats = index.describe_index_stats()
        st.success("✅ Pinecone stats retrieval successful")
        st.json(stats)
    except Exception as e:
        st.error(f"❌ Pinecone stats retrieval failed: {str(e)}")

# Test complete pipeline (OpenAI + Pinecone upsert)
st.subheader("5. Testing Complete Upsert Pipeline")
if st.button("Test Upsert", key="test_upsert"):
    try:
        # Step 1: Initialize connections
        st.write("Initializing connections...")
        from pinecone import Pinecone
        pc = Pinecone(api_key=st.secrets["PINECONE_API_KEY"])
        index = pc.Index("zecompete")
        client = OpenAI(api_key=st.secrets["OPENAI_API_KEY"])
        
        # Step 2: Generate embedding
        st.write("Generating embedding...")
        test_text = "Test store for debugging"
        response = client.embeddings.create(
            model="text-embedding-3-small",
            input=[test_text]
        )
        embedding = response.data[0].embedding
        st.write(f"Embedding generated with dimension: {len(embedding)}")
        
        # Step 3: Prepare record
        st.write("Preparing record for upsert...")
        test_id = f"debug-test-{int(time.time())}"
        test_metadata = {"name": test_text, "source": "debug_test"}
        record = (test_id, embedding, test_metadata)
        
        # Step 4: Upsert to Pinecone
        st.write("Upserting to Pinecone...")
        index.upsert(vectors=[record], namespace="debug")
        
        # Step 5: Verify upsert
        st.write("Verifying upsert...")
        time.sleep(2)  # Give Pinecone time to process
        stats = index.describe_index_stats()
        st.json(stats)
        
        st.success("✅ Complete pipeline test successful!")
    except Exception as e:
        st.error(f"❌ Pipeline test failed: {str(e)}")
        import traceback
        st.code(traceback.format_exc())
