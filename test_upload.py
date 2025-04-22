# test_upload.py
import streamlit as st
from pinecone import Pinecone
from openai import OpenAI
import pandas as pd

st.title("Pinecone Test Upload")

# Initialize connections
pc = Pinecone(api_key=st.secrets["PINECONE_API_KEY"])
index = pc.Index("zecompete")

client = OpenAI(api_key=st.secrets["OPENAI_API_KEY"])

# Display current index stats
st.subheader("Current Index Stats")
stats = index.describe_index_stats()
st.write(stats)

# Test data
test_data = [
    {"name": "Test Store 1", "city": "Test City", "brand": "Test Brand"},
    {"name": "Test Store 2", "city": "Test City", "brand": "Test Brand"}
]

if st.button("Upload Test Data", key="test_upload"):
    st.write("Generating embeddings...")
    texts = [item["name"] for item in test_data]
    
    try:
        # Create embeddings
        response = client.embeddings.create(
            model="text-embedding-3-small",
            input=texts
        )
        embeddings = [d.embedding for d in response.data]
        st.write(f"Generated {len(embeddings)} embeddings")
        
        # Create records for Pinecone
        records = []
        for i, (item, embedding) in enumerate(zip(test_data, embeddings)):
            record_id = f"test-{i}"
            records.append((record_id, embedding, item))
        
        # Upsert to Pinecone
        st.write(f"Upserting {len(records)} records to Pinecone...")
        index.upsert(vectors=records, namespace="test")
        
        # Verify upload
        st.write("Checking updated stats...")
        updated_stats = index.describe_index_stats()
        st.write(updated_stats)
        
        st.success("Test upload complete!")
    except Exception as e:
        st.error(f"Error during test upload: {str(e)}")
        st.write("Error details:", repr(e))

# Add a query test
st.subheader("Test Query")
if st.button("Run Test Query", key="test_query"):
    try:
        # Create a test embedding
        response = client.embeddings.create(
            model="text-embedding-3-small",
            input=["Test Store"]
        )
        query_embedding = response.data[0].embedding
        
        # Query Pinecone
        st.write("Querying Pinecone...")
        results = index.query(
            vector=query_embedding,
            top_k=5,
            namespace="test",
            include_metadata=True
        )
        
        st.write("Query results:")
        st.write(results)
    except Exception as e:
        st.error(f"Error during test query: {str(e)}")
