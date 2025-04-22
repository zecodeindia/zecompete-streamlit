import os, itertools, pandas as pd, streamlit as st
import time, json
from src.run_pipeline import run
from src.analytics import insight_question
import pinecone

st.set_page_config(page_title="Competitor Mapper", layout="wide")
st.title("🗺️  Competitor Location & Demand Explorer")

brands = st.text_input("Brands (comma)", "Zudio, Max Fashion, Zara, H&M, Trends")
cities = st.text_input("Cities (comma)", "Bengaluru, Hyderabad")

if st.button("Run analysis", key="run_analysis_button"):
    log_container = st.container()
    log_container.subheader("Processing Logs")
    
    # Add this note about potential fallback data
    st.info("Note: If Google Maps data can't be accessed via Apify, the app will create sample data to demonstrate functionality.")
    
    for b, c in itertools.product(
            map(str.strip, brands.split(",")),
            map(str.strip, cities.split(","))):
        log_container.write(f"Processing {b} in {c}...")
        try:
            run(b, c)
            log_container.write(f"✅ Completed processing {b} in {c}")
        except Exception as e:
            log_container.error(f"❌ Error processing {b} in {c}: {str(e)}")
    
    st.success("Data ready!")

# Initialize Pinecone directly (old style API)
pinecone.init(api_key=st.secrets["PINECONE_API_KEY"], environment="us-east-1")
idx = pinecone.Index("zecompete")

tabs = st.tabs(["Ask", "Explore Data", "Diagnostic"])

with tabs[0]:
    q = st.text_area("Ask a question about the data")
    if st.button("Answer", key="answer_button") and q:
        try:
            answer = insight_question(q)
            st.write(answer)
        except Exception as e:
            st.error(f"Error: {str(e)}")
            st.write("Please try a simpler question or check the Diagnostic tab to verify data exists.")

with tabs[1]:
    try:
        res = idx.describe_index_stats()
        
        # Display as text, not JSON
        st.subheader("Index Statistics")
        st.write("Dimension:", res.get("dimension", "N/A"))
        st.write("Total vector count:", res.get("total_vector_count", 0))
        st.write("Index fullness:", res.get("index_fullness", 0))
        
        # Handle namespaces specifically
        st.subheader("Namespaces")
        namespaces = res.get("namespaces", {})
        if namespaces:
            for ns_name, ns_data in namespaces.items():
                st.write(f"Namespace: {ns_name}")
                st.write(f"Vector count: {ns_data.get('vector_count', 0)}")
                
                # Add a button to view sample data from this namespace
                if st.button(f"View sample data from {ns_name}", key=f"view_{ns_name}"):
                    st.write(f"Sample data from '{ns_name}' namespace:")
                    try:
                        # Fetch a few vectors
                        query_response = idx.query(
                            vector=[0] * 1536,  # Dummy vector for metadata-only query
                            top_k=5,
                            namespace=ns_name,
                            include_metadata=True
                        )
                        
                        if query_response.matches:
                            for i, match in enumerate(query_response.matches):
                                st.write(f"Record {i+1}:")
                                st.json(match.metadata)
                        else:
                            st.warning(f"No records found in namespace '{ns_name}'")
                    except Exception as e:
                        st.error(f"Error querying namespace '{ns_name}': {str(e)}")
        else:
            st.write("No namespaces found")
    except Exception as e:
        st.error(f"Error fetching index stats: {str(e)}")

with tabs[2]:
    st.subheader("Diagnostic Information")
    
    # Check namespaces and count
    try:
        stats = idx.describe_index_stats()
        
        # Display basic stats as text
        st.write(f"Dimension: {stats.get('dimension')}")
        st.write(f"Total vectors: {stats.get('total_vector_count')}")
        st.write(f"Index fullness: {stats.get('index_fullness')}")
        
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
                            # Display metadata as regular text 
                            for key, value in match.metadata.items():
                                st.write(f"{key}: {value}")
                            st.write("---")
                    else:
                        st.warning(f"No records found in namespace '{ns}'")
                except Exception as e:
                    st.error(f"Error querying namespace '{ns}': {str(e)}")
        else:
            st.warning("No namespaces found in the index. Data may not have been uploaded successfully.")
        
        # Add a section for testing individual brand/city searches
        st.subheader("Test Brand/City Search")
        test_brand = st.text_input("Test brand", "Zara")
        test_city = st.text_input("Test city", "Bengaluru")
        
        if st.button("Test Search", key="test_search"):
            from src.scrape_maps import run_scrape
            
            with st.spinner(f"Searching for {test_brand} in {test_city}..."):
                results = run_scrape(test_brand, test_city)
                
                if results:
                    st.success(f"Found {len(results)} results for {test_brand} in {test_city}")
                    for i, result in enumerate(results[:5]):  # Show only first 5
                        st.write(f"Result {i+1}:")
                        # Clean up the result for display
                        display_result = {
                            "name": result.get("name", ""),
                            "address": result.get("address", ""),
                            "rating": result.get("totalScore", "N/A"),
                            "reviews": result.get("reviewsCount", "N/A")
                        }
                        st.json(display_result)
                else:
                    st.error(f"No results found for {test_brand} in {test_city}")
        
    except Exception as e:
        st.error(f"Error accessing Pinecone: {str(e)}")
        import traceback
        st.code(traceback.format_exc())

    # Add a direct test for Pinecone and OpenAI
    st.subheader("Test API Connections")
    
    col1, col2 = st.columns(2)
    
    with col1:
        if st.button("Test Pinecone Connection", key="test_pinecone"):
            try:
                # Check if we can connect to Pinecone
                pinecone.whoami()
                st.success("✅ Pinecone connection successful")
            except Exception as e:
                st.error(f"❌ Pinecone connection failed: {str(e)}")
    
    with col2:
        if st.button("Test OpenAI Connection", key="test_openai"):
            try:
                from openai import OpenAI
                client = OpenAI(api_key=st.secrets["OPENAI_API_KEY"])
                
                # Simple completion to test the connection
                response = client.chat.completions.create(
                    model="gpt-4o-mini",
                    messages=[{"role": "user", "content": "Say hello!"}],
                    max_tokens=10
                )
                
                st.success(f"✅ OpenAI connection successful: {response.choices[0].message.content}")
            except Exception as e:
                st.error(f"❌ OpenAI connection failed: {str(e)}")
                
    # Add Apify test
    if st.button("Test Apify Connection", key="test_apify"):
        try:
            import requests
            
            headers = {
                "Content-Type": "application/json",
                "Authorization": f"Bearer {st.secrets['APIFY_TOKEN']}"
            }
            
            # Simple API call to check token
            response = requests.get(
                "https://api.apify.com/v2/user/me",
                headers=headers
            )
            
            if response.status_code == 200:
                user_data = response.json()
                st.success(f"✅ Apify connection successful. Username: {user_data.get('username')}")
            else:
                st.error(f"❌ Apify connection failed. Status code: {response.status_code}")
                st.write(response.text)
                
        except Exception as e:
            st.error(f"❌ Apify test failed: {str(e)}")
