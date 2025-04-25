import os, itertools, pandas as pd, streamlit as st
import time, json, threading, requests, secrets
from pinecone import Pinecone
from openai import OpenAI

# Set up the app
st.set_page_config(page_title="Competitor Mapper", layout="wide")
st.title("🗺️ Competitor Location & Demand Explorer")

# Initialize session state for tracking task processing
if "auto_refresh" not in st.session_state:
    st.session_state.auto_refresh = False
if "last_refresh" not in st.session_state:
    st.session_state.last_refresh = time.time()
if "last_brand" not in st.session_state:
    st.session_state.last_brand = ""
if "last_city" not in st.session_state:
    st.session_state.last_city = ""

# Initialize Pinecone
try:
    from src.config import secret
    pc = Pinecone(api_key=secret("PINECONE_API_KEY"))
    idx = pc.Index("zecompete")
    
    # Add a data clearing button at the top of the app
    if st.button("🔄 Clear ALL Data from Pinecone", type="primary"):
        with st.spinner("Clearing all data from Pinecone..."):
            try:
                # Clear maps namespace
                idx.delete(delete_all=True, namespace="maps")
                # Clear keywords namespace
                idx.delete(delete_all=True, namespace="keywords")
                st.success("✅ All data cleared from Pinecone successfully!")
                st.experimental_rerun()  # Rerun the app to reflect the cleared data
            except Exception as e:
                st.error(f"Error clearing data: {str(e)}")

    pinecone_success = True
    st.success("✅ Successfully connected to Pinecone!")
except Exception as e:
    st.error(f"Error initializing Pinecone: {str(e)}")
    pinecone_success = False

# Import the modules we need
try:
    from src.run_pipeline import run
    from src.analytics import insight_question
    from src.embed_upsert import _embed, upsert_places
    
    # Import new modules for automation
    from src.scrape_maps import run_scrape, run_apify_task, check_task_status
    from src.task_manager import add_task, process_all_tasks, get_running_tasks, get_pending_tasks
    from src.webhook_handler import process_dataset_directly, create_apify_webhook
    
    # Import keyword pipeline module
    from src.keyword_pipeline import run_keyword_pipeline, get_business_names_from_pinecone
    
    import_success = True
    st.success("✅ Successfully imported all modules!")
except Exception as e:
    st.error(f"Import error: {str(e)}")
    import_success = False

# Define get_webhook_secret function here since it's missing or not properly imported
def get_webhook_secret():
    """Get or create a webhook secret"""
    # Try to get from Streamlit secrets
    try:
        return secret("WEBHOOK_SECRET")
    except:
        # Generate a new one
        if "webhook_secret" not in st.session_state:
            st.session_state.webhook_secret = secrets.token_hex(16)
        return st.session_state.webhook_secret

# Define tabs
tabs = st.tabs(["Run Analysis", "Auto Integration", "Keywords & Search Volume", "Manual Upload", "Ask Questions", "Explore Data", "Diagnostic"])

# Tab 1: Run Analysis
with tabs[0]:
    st.header("Run Analysis")
    
    # Add another clear data button specific to this tab
    if st.button("🔄 Clear Previous Data", key="clear_tab_data"):
        with st.spinner("Clearing all data from Pinecone..."):
            try:
                # Clear maps namespace
                idx.delete(delete_all=True, namespace="maps")
                # Clear keywords namespace
                idx.delete(delete_all=True, namespace="keywords")
                st.success("✅ All data cleared from Pinecone successfully!")
                # Force cache clear
                st.cache_data.clear()
            except Exception as e:
                st.error(f"Error clearing data: {str(e)}")
    
    # Standard interface for brand/city search
    brands = st.text_input("Brands (comma)", "Zudio, Max Fashion, Zara, H&M, Trends")
    cities = st.text_input("Cities (comma)", "Bengaluru, Hyderabad")
    
    col1, col2 = st.columns(2)
    
    with col1:
        if st.button("Run Manual Pipeline", key="run_manual"):
            log_container = st.container()
            log_container.subheader("Processing Logs")
            
            # Clear existing data first
            log_container.write("Clearing existing data from Pinecone...")
            try:
                idx.delete(delete_all=True, namespace="maps")
                idx.delete(delete_all=True, namespace="keywords")
                log_container.write("✅ Previous data cleared successfully")
            except Exception as e:
                log_container.error(f"❌ Error clearing data: {str(e)}")
            
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
            # Force cache clear and rerun
            st.cache_data.clear()
            st.experimental_rerun()
    
    with col2:
        # Option to run with Apify automation
        task_id = st.text_input("Apify Task ID", "zecodemedia~google-maps-scraper-task")  # Updated task ID
        
        if st.button("Run with Apify (Automated)", key="run_automated"):
            log_container = st.container()
            log_container.subheader("Automated Processing Logs")
            
            # Clear existing data first
            log_container.write("Clearing existing data from Pinecone...")
            try:
                idx.delete(delete_all=True, namespace="maps")
                idx.delete(delete_all=True, namespace="keywords")
                log_container.write("✅ Previous data cleared successfully")
            except Exception as e:
                log_container.error(f"❌ Error clearing data: {str(e)}")
            
            # Enable auto-refresh to check for task completion
            st.session_state.auto_refresh = True
            
            # Process each brand and city
            brand_list = [b.strip() for b in brands.split(",")]
            city_list = [c.strip() for c in cities.split(",")]
            
            for b, c in itertools.product(brand_list, city_list):
                # Store current brand and city for webhook use
                st.session_state.last_brand = b
                st.session_state.last_city = c
                
                log_container.write(f"Starting Apify task for {b} in {c}...")
                
                # Run the Apify task
                run_id, _ = run_apify_task(b, c)
                
                if run_id:
                    # Add to task manager
                    add_task(run_id, b, c)
                    log_container.write(f"✅ Apify task started with run ID: {run_id}")
                else:
                    log_container.error(f"❌ Failed to start Apify task for {b} in {c}")
    
    # Display task status if auto-refresh is enabled
    if st.session_state.auto_refresh:
        st.subheader("Task Status")
        
        # Show a toggle to disable auto-refresh
        if st.button("Disable Auto-Refresh"):
            st.session_state.auto_refresh = False
            st.experimental_rerun()  # Use st.experimental_rerun() instead of rerun
        
        # Check if we should refresh
        current_time = time.time()
        if current_time - st.session_state.last_refresh > 15:  # Refresh every 15 seconds
            st.session_state.last_refresh = current_time
            
            with st.spinner("Checking task status..."):
                # Process any tasks that have completed
                processed = process_all_tasks()
                if processed > 0:
                    st.success(f"✅ Processed {processed} completed tasks")
        
        # Display running tasks
        running_tasks = get_running_tasks()
        pending_tasks = get_pending_tasks()
        
        if running_tasks:
            st.write("Running tasks:")
            for task in running_tasks:
                st.write(f"• {task['brand']} in {task['city']} (Run ID: {task['run_id']})")
        else:
            st.write("No running tasks")
            
        if pending_tasks:
            st.write("Pending tasks:")
            for task in pending_tasks:
                st.write(f"• {task['brand']} in {task['city']} (Run ID: {task['run_id']})")
        
        # Safer approach using a timer
        if running_tasks and st.session_state.auto_refresh:
            # Update the last refresh time
            st.session_state.last_refresh = time.time()
            # Show a countdown timer instead of auto-refreshing
            remaining = 15  # seconds until next refresh
            refresh_placeholder = st.empty()
            refresh_placeholder.info(f"Auto-refreshing in {remaining} seconds... Click 'Disable Auto-Refresh' to stop.")
            # Add a manual refresh button
            if st.button("Refresh Now"):
                st.experimental_rerun()  # Use st.experimental_rerun() instead of rerun

# Tab 2: Auto Integration
with tabs[1]:
    st.header("Auto Integration Setup")
    st.markdown("""
    Set up automatic processing of Apify results when tasks complete. This can be done via:
    1. **Webhook Integration** - Automatically process data when Apify tasks complete
    2. **Direct Dataset Processing** - Manually trigger processing for specific Apify datasets
    """)
    
    # Add clear button
    if st.button("🔄 Clear Previous Data", key="clear_auto_tab"):
        with st.spinner("Clearing all data from Pinecone..."):
            try:
                idx.delete(delete_all=True, namespace="maps")
                idx.delete(delete_all=True, namespace="keywords")
                st.success("✅ All data cleared from Pinecone successfully!")
            except Exception as e:
                st.error(f"Error clearing data: {str(e)}")
    
    # Webhook integration section
    st.subheader("Webhook Integration")
    
    # Get app URL for webhook callback
    app_url = st.text_input("Your app URL (for webhooks)", "https://zecompete-app.streamlit.app")
    callback_url = f"{app_url}/webhook"
    
    # Display webhook information
    webhook_secret = get_webhook_secret()
    
    st.markdown("#### Webhook Configuration")
    st.code(f"URL: {callback_url}")
    st.code(f"Secret: {webhook_secret}")
    
    # Setup webhook button
    task_id = st.text_input("Apify Task ID for webhook", "zecodemedia~google-maps-scraper-task")  # Updated task ID
    
    if st.button("Set Up Webhook in Apify"):
        with st.spinner("Creating webhook..."):
            webhook_id = create_apify_webhook(task_id, callback_url)
            
            if webhook_id:
                st.success(f"✅ Webhook created with ID: {webhook_id}")
                st.markdown("""
                ### Next Steps:
                1. Run your Apify tasks from the "Run Analysis" tab
                2. When tasks complete, Apify will call your app's webhook
                3. Tasks will be automatically processed and data uploaded to Pinecone
                """)
            else:
                st.error("❌ Failed to create webhook")
                st.markdown("""
                ### Troubleshooting:
                - Make sure your Apify API token has the correct permissions
                - Verify that the Task ID is correct
                - If webhook creation fails, use the direct dataset processing method below
                """)
    
    # Direct dataset processing section
    st.markdown("---")
    st.subheader("Direct Dataset Processing")
    st.markdown("""
    If webhook integration isn't feasible (e.g., in Streamlit Cloud where incoming webhooks aren't supported),
    you can manually process Apify datasets after tasks complete.
    """)
    
    dataset_id = st.text_input("Apify Dataset ID")
    col1, col2 = st.columns(2)
    
    with col1:
        brand = st.text_input("Brand", "Zecode")
    
    with col2:
        city = st.text_input("City", "Bengaluru")
    
    if st.button("Process Dataset") and dataset_id:
        with st.spinner(f"Processing dataset {dataset_id}..."):
            # Clear existing data first
            try:
                idx.delete(delete_all=True, namespace="maps")
                idx.delete(delete_all=True, namespace="keywords") 
                st.write("✅ Previous data cleared successfully")
            except Exception as e:
                st.error(f"❌ Error clearing data: {str(e)}")
                
            success = process_dataset_directly(dataset_id, brand, city)
            
            if success:
                st.success(f"✅ Successfully processed dataset for {brand} in {city}")
            else:
                st.error(f"❌ Failed to process dataset {dataset_id}")
    
    # Instructions for webhook forwarding
    st.markdown("---")
    st.subheader("Webhook Forwarding Services")
    st.markdown("""
    Since Streamlit Cloud doesn't support direct incoming webhooks, you can use a webhook forwarding service:
    
    1. **[Hookdeck](https://hookdeck.com)** - Receive webhooks and forward them to your app
    2. **[Pipedream](https://pipedream.com)** - Create workflows triggered by webhooks
    3. **[webhook.site](https://webhook.site)** - For testing and debugging webhooks
    
    **Basic setup:**
    1. Create an account with a forwarding service
    2. Configure it to forward Apify webhooks to your app's `/webhook` endpoint
    3. Use the forwarding URL when creating the webhook in Apify
    """)

# Tab 3: Keywords & Search Volume
with tabs[2]:
    st.header("Keywords & Search Volume Analysis")
    st.markdown("""
    This tab helps you generate relevant keywords based on business names in your Pinecone index,
    fetch search volume data from DataForSEO, and store everything back in Pinecone.
    """)
    
    # Add clear button
    if st.button("🔄 Clear Previous Data", key="clear_kw_tab"):
        with st.spinner("Clearing all data from Pinecone..."):
            try:
                idx.delete(delete_all=True, namespace="maps")
                idx.delete(delete_all=True, namespace="keywords")
                st.success("✅ All data cleared from Pinecone successfully!")
                # Force refresh
                st.cache_data.clear()
                st.experimental_rerun()
            except Exception as e:
                st.error(f"Error clearing data: {str(e)}")
    
    # Option to specify city for the keywords
    city = st.text_input("City for keyword data", "General")
    
    # Display business count
    if st.button("Check Business Names"):
        with st.spinner("Retrieving business names from Pinecone..."):
            try:
                # Clear keywords first
                idx.delete(delete_all=True, namespace="keywords")
                st.write("✅ Cleared previous keyword data")
                
                business_names = get_business_names_from_pinecone()
                if business_names:
                    st.success(f"✅ Found {len(business_names)} unique business names in Pinecone")
                    # Show sample of business names
                    st.write("Sample business names:")
                    st.write(", ".join(business_names[:10]))
                    if len(business_names) > 10:
                        st.write(f"...and {len(business_names) - 10} more")
                else:
                    st.warning("No business names found in Pinecone. Please upload business data first.")
            except Exception as e:
                st.error(f"Error retrieving business names: {str(e)}")
    
    # Generate keywords and get search volume
    if st.button("Generate Keywords & Get Search Volume"):
        with st.spinner("Running keyword pipeline..."):
            try:
                # Clear keywords first
                idx.delete(delete_all=True, namespace="keywords")
                st.write("✅ Cleared previous keyword data")
                
                success = run_keyword_pipeline(city)
                if success:
                    st.success("✅ Successfully generated keywords and retrieved search volume data")
                    
                    # Show the new data in Pinecone
                    try:
                        # Check for the keywords namespace
                        stats = idx.describe_index_stats()
                        namespaces = stats.get("namespaces", {})
                        
                        if "keywords" in namespaces:
                            count = namespaces["keywords"].get("vector_count", 0)
                            st.write(f"You now have {count} keyword vectors in the 'keywords' namespace")
                            
                            # Show sample keywords
                            dummy_vector = [0] * stats.get("dimension", 1536)
                            results = idx.query(
                                vector=dummy_vector,
                                top_k=5,
                                namespace="keywords",
                                include_metadata=True
                            )
                            
                            if results.matches:
                                st.write("Sample keywords:")
                                keyword_data = []
                                for match in results.matches:
                                    if match.metadata:
                                        keyword_data.append(match.metadata)
                                
                                if keyword_data:
                                    kw_df = pd.DataFrame(keyword_data)
                                    st.dataframe(kw_df)
                    except Exception as e:
                        st.error(f"Error displaying keyword data: {str(e)}")
                else:
                    st.error("❌ Keyword pipeline failed. Check the logs for details.")
            except Exception as e:
                st.error(f"Error running keyword pipeline: {str(e)}")
                import traceback
                st.code(traceback.format_exc())
    
    # Add explanation of the process
    st.markdown("""
    ### How It Works
    
    1. **Extract Business Names**: We query your Pinecone index to retrieve all business names from the "maps" namespace.
    
    2. **Generate Keywords**: We use OpenAI to generate relevant search terms based on the business names.
    
    3. **Get Search Volume**: We fetch search volume data for these keywords from DataForSEO.
    
    4. **Store in Pinecone**: We embed the keywords and store them with their search volume data in the "keywords" namespace.
    
    This data can then be used to understand search demand and trends related to the businesses in your index.
    """)
    
    # Add information about how to use this data
    st.markdown("""
    ### Using Keyword Data
    
    You can access this keyword data in several ways:
    
    1. Use the "Explore Data" tab to see all keywords and their search volumes.
    
    2. Ask questions about search trends in the "Ask Questions" tab, such as:
       - "Which keywords have the highest search volume?"
       - "What are the trending search terms for clothing stores?"
       - "Compare search volumes for different retail brands"
    
    3. Create custom analyses by exporting the data and using your preferred analytics tools.
    """)

# Tab 4: Manual Upload
with tabs[3]:
    st.header("Upload Apify CSV (Optional)")
    
    # Add clear button
    if st.button("🔄 Clear Previous Data", key="clear_upload_tab"):
        with st.spinner("Clearing all data from Pinecone..."):
            try:
                idx.delete(delete_all=True, namespace="maps")
                idx.delete(delete_all=True, namespace="keywords")
                st.success("✅ All data cleared from Pinecone successfully!")
            except Exception as e:
                st.error(f"Error clearing data: {str(e)}")
                
    uploaded_file = st.file_uploader("Upload the Apify CSV file", type="csv")

    if uploaded_file is not None:
        st.write("Processing uploaded CSV file...")
        df = pd.read_csv(uploaded_file)
        st.write(f"CSV contains {len(df)} rows and {len(df.columns)} columns")
        
        # Sample of the data
        st.write("Sample of the data:")
        st.dataframe(df.head(3))
        
        # Upload to Pinecone button
        if st.button("Upload CSV data to Pinecone"):
            if pinecone_success and import_success:
                try:
                    # Clear existing data first
                    st.write("Clearing previous data...")
                    idx.delete(delete_all=True, namespace="maps")
                    idx.delete(delete_all=True, namespace="keywords")
                    st.write("✅ Previous data cleared successfully")
                    
                    # Extract brand from searchString if available
                    if 'searchString' in df.columns:
                        brand = df['searchString'].iloc[0] if not df.empty else "Unknown"
                    else:
                        brand = st.text_input("Brand name (not found in CSV)", "Zecode")
                    
                    # Extract city or use default
                    if 'city' in df.columns:
                        city = df['city'].iloc[0] if not df.empty else "Bengaluru"
                    else:
                        city = st.text_input("City (not found in CSV)", "Bengaluru")
                    
                    st.write(f"Uploading to Pinecone for brand: {brand}, city: {city}...")
                    # Generate embeddings and upload to Pinecone
                    upsert_places(df, brand, city)
                    st.success("✅ CSV data uploaded to Pinecone successfully!")
                    # Force refresh
                    st.cache_data.clear()
                    st.experimental_rerun()
                except Exception as e:
                    st.error(f"Error uploading to Pinecone: {str(e)}")
            else:
                st.error("Cannot upload to Pinecone due to connection or import issues")

# Tab 5: Ask Questions
with tabs[4]:
    st.header("Ask Questions About Your Data")
    
    # Add refresh data notice
    st.info("⚠️ Make sure you have run the data collection process or check the 'Explore Data' tab to verify you have data to query.")
    
    q = st.text_area("Enter your question about the competitor data")
    if st.button("Answer", key="answer_button") and q:
        try:
            answer = insight_question(q)
            st.write(answer)
        except Exception as e:
            st.error(f"Error: {str(e)}")
            st.write("Please try a simpler question or check the Diagnostic tab to verify data exists.")

# Tab 6: Explore Data
with tabs[5]:
    st.header("Explore Stored Data")
    
    # Add refresh button
    if st.button("🔄 Refresh Data View", key="refresh_explore"):
        st.cache_data.clear()
        st.experimental_rerun()
        
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
                col1, col2, col3 = st.columns([3, 2, 1])
                
                with col1:
                    st.write(f"Namespace: {ns_name}")
                    st.write(f"Vector count: {ns_data.get('vector_count', 0)}")
                
                with col2:
                    # Button to view data from this namespace
                    if st.button(f"View data", key=f"view_{ns_name}"):
                        try:
                            # Create a dummy vector for search
                            dummy_vector = [0] * res.get("dimension", 1536)
                            
                            # Query to get records
                            results = idx.query(
                                vector=dummy_vector,
                                top_k=10,
                                namespace=ns_name,
                                include_metadata=True
                            )
                            
                            # Display results in a table
                            if results.matches:
                                # Extract metadata
                                data = []
                                for match in results.matches:
                                    if match.metadata:
                                        data.append(match.metadata)
                                
                                if data:
                                    df = pd.DataFrame(data)  # Fixed line
                                    st.dataframe(df)
                                else:
                                    st.write("No metadata available for these records")
                            else:
                                st.write("No records found")
                        except Exception as e:
                            st.error(f"Error retrieving data: {str(e)}")
                
                with col3:
                    # Button to delete namespace
                    if st.button(f"Delete", key=f"delete_{ns_name}"):
                        try:
                            # Confirm deletion
                            confirmation = st.checkbox(f"Confirm delete namespace '{ns_name}'", key=f"confirm_delete_{ns_name}")
                            if confirmation:
                                # Delete all vectors in the namespace
                                idx.delete(delete_all=True, namespace=ns_name)
                                st.success(f"Successfully deleted namespace '{ns_name}'")
                                
                                # Refresh the page to show updated stats
                                st.experimental_rerun()  # Use st.experimental_rerun() instead of rerun
                        except Exception as e:
                            st.error(f"Error deleting namespace {ns_name}: {str(e)}")
                
                # Add a divider between namespaces
                st.markdown("---")
        else:
            st.warning("No namespaces found - you may need to run data collection first.")
    except Exception as e:
        st.error(f"Error fetching index stats: {str(e)}")

# Tab 7: Diagnostic 
with tabs[6]:
    st.subheader("Diagnostic Information")
    
    # Add clear button
    if st.button("🔄 Clear All Data", key="clear_diagnostic"):
        with st.spinner("Clearing all data from Pinecone..."):
            try:
                idx.delete(delete_all=True, namespace="maps")
                idx.delete(delete_all=True, namespace="keywords")
                st.success("✅ All data cleared from Pinecone successfully!")
                st.cache_data.clear()
                st.experimental_rerun()
            except Exception as e:
                st.error(f"Error clearing data: {str(e)}")
    
    # Add refresh button
    if st.button("🔄 Refresh Diagnostic Data", key="refresh_diagnostic"):
        st.cache_data.clear()
        st.experimental_rerun()
    
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
                        vector=[0] * stats.get("dimension", 1536),  # Dummy vector for metadata-only query
                        top_k=5,
                        namespace=ns,
                        include_metadata=True
                    )
                    
                    if query_response.matches:
                        st.write(f"Found {len(query_response.matches)} records")
                        # Convert to DataFrame for better display
                        data = []
                        for match in query_response.matches:
                            if match.metadata:
                                data.append(match.metadata)
                        
                        if data:
                            df = pd.DataFrame(data)  # Fixed line
                            st.dataframe(df)
                        else:
                            st.write("No metadata available for these records")
                    else:
                        st.warning(f"No records found in namespace '{ns}'")
                except Exception as e:
                    st.error(f"Error querying namespace '{ns}': {str(e)}")
        else:
            st.warning("No namespaces found in the index. Data may not have been uploaded successfully.")
    except Exception as e:
        st.error(f"Error accessing Pinecone: {str(e)}")

    # New Apify diagnostic tools
    st.markdown("---")
    st.subheader("Apify API Diagnostic")

    if st.button("Test Apify Connection"):
        try:
            from src.config import secret
            
            # Get the API token
            apify_token = secret("APIFY_TOKEN")
            if not apify_token:
                st.error("❌ No Apify token found in secrets!")
            else:
                st.write(f"API Token (first/last 4 chars): {apify_token[:4]}...{apify_token[-4:]} (length: {len(apify_token)})")
                
                # Test endpoint that just returns user info
                url = "https://api.apify.com/v2/users/me"
                
                # Try both authentication methods
                st.write("Testing with query parameter authentication...")
                params = {"token": apify_token}
                resp1 = requests.get(url, params=params)
                st.write(f"Response status: {resp1.status_code}")
                
                st.write("Testing with Bearer token authentication...")
                headers = {"Authorization": f"Bearer {apify_token}"}
                resp2 = requests.get(url, headers=headers)
                st.write(f"Response status: {resp2.status_code}")
                
                # Use the successful response or show both
