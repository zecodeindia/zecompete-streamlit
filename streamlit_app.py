# Simplified streamlit_app.py
import streamlit as st
from pinecone import Pinecone
from openai import OpenAI
import time

st.set_page_config(page_title="Pinecone Test", layout="wide")
st.title("🗂️ Pinecone Direct Test")

# Initialize API clients
api_key = st.secrets["PINECONE_API_KEY"]
pc = Pinecone(api_key=api_key)
index = pc.Index("zecompete")

openai_key = st.secrets["OPENAI_API_KEY"]
client = OpenAI(api_key=openai_key)

st.write("This is a direct test of Pinecone functionality")

# Simple upsert test
st.subheader("Test Data Upload")
test_text = st.text_input("Enter text to store", "Test location in Bengaluru")
test_namespace = st.text_input("Namespace", "simple_test")

if st.button("Store in Pinecone"):
    with st.spinner("Generating embedding and storing..."):
        try:
            # Generate embedding
            response = client.embeddings.create(
                model="text-embedding-3-small",
                input=[test_text]
            )
            embedding = response.data[0].embedding
            st.write(f"✅ Generated embedding with {len(embedding)} dimensions")
            
            # Create record
            record_id = f"test-{int(time.time())}"
            metadata = {"text": test_text, "timestamp": time.time()}
            
            # Upsert to Pinecone
            result = index.upsert(
                vectors=[(record_id, embedding, metadata)],
                namespace=test_namespace
            )
            
            st.success(f"✅ Data stored successfully! Result: {result}")
        except Exception as e:
            st.error(f"❌ Error: {str(e)}")

# View data
st.subheader("View Stored Data")
if st.button("View All Data"):
    try:
        stats = index.describe_index_stats()
        
        st.write(f"Total vectors: {stats.get('total_vector_count', 0)}")
        st.write(f"Dimension: {stats.get('dimension', 'unknown')}")
        
        namespaces = stats.get("namespaces", {})
        if namespaces:
            st.write("Namespaces:")
            for ns_name, ns_data in namespaces.items():
                st.write(f"- {ns_name}: {ns_data.get('vector_count', 0)} vectors")
                
                # Query sample from this namespace
                if ns_data.get('vector_count', 0) > 0:
                    st.write(f"Sample from '{ns_name}':")
                    # Use a dummy vector for metadata-only retrieval
                    dummy_vector = [0.0] * stats.get('dimension', 1536)
                    results = index.query(
                        vector=dummy_vector,
                        top_k=3,
                        namespace=ns_name,
                        include_metadata=True
                    )
                    
                    if results.matches:
                        for i, match in enumerate(results.matches):
                            st.write(f"Item {i+1}:")
                            st.write(f"ID: {match.id}")
                            st.write(f"Metadata: {match.metadata}")
                            st.write("---")
                    else:
                        st.write("No matches found")
        else:
            st.write("No namespaces found")
    except Exception as e:
        st.error(f"❌ Error retrieving data: {str(e)}")

# Simple search
st.subheader("Search Data")
search_text = st.text_input("Enter text to search", "Bengaluru")
search_namespace = st.text_input("Namespace to search", "simple_test")

if st.button("Search"):
    try:
        # Generate embedding for search
        response = client.embeddings.create(
            model="text-embedding-3-small",
            input=[search_text]
        )
        search_embedding = response.data[0].embedding
        
        # Query Pinecone
        results = index.query(
            vector=search_embedding,
            top_k=5,
            namespace=search_namespace,
            include_metadata=True
        )
        
        if results.matches:
            st.success(f"Found {len(results.matches)} matches")
            for i, match in enumerate(results.matches):
                st.write(f"Match {i+1}:")
                st.write(f"ID: {match.id}")
                st.write(f"Score: {match.score}")
                st.write(f"Metadata: {match.metadata}")
                st.write("---")
        else:
            st.warning("No matches found")
    except Exception as e:
        st.error(f"❌ Error searching: {str(e)}")
