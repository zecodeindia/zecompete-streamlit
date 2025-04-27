# streamlit_app_updates.py
"""
Updates for streamlit_app.py to integrate the OpenAI Assistant for keyword refinement.
These changes should be applied to the existing streamlit_app.py file.
"""

# ------  ADD THESE IMPORTS AT THE TOP OF THE FILE ------
# Add this in the import section
from src.openai_keyword_refiner import refine_keywords, batch_refine_keywords
from src.enhanced_keyword_pipeline import run_enhanced_keyword_pipeline, generate_enhanced_keywords_for_businesses

# ------ UPDATES TO THE KEYWORDS & SEARCH VOLUME TAB ------
"""
Replace the existing content in the "Keywords & Search Volume" tab (Tab 3) with this code:
"""

with tabs[2]:
    st.header("Keywords & Search Volume Analysis")

    # Clear previous keywords button
    if st.button("🔄 Clear Previous Keywords", key="clear_kw_tab"):
        with st.spinner("Clearing keyword data..."):
            safe_clear_namespace(idx, "keywords")
            st.success("✅ Cleared keyword data.")

    # Input city name for keyword context
    city = st.text_input("City for keywords", "Bengaluru")
    
    # Add a toggle for AI-powered keyword refinement
    use_ai_refinement = st.toggle("Use AI-powered keyword refinement", value=True)
    
    if use_ai_refinement:
        st.info("""
        🤖 **AI-powered keyword refinement** will:
        - Focus only on brand name + location pairs
        - Remove keywords with intents like "timings", "phone number", etc.
        - Ensure keywords are natural and realistic
        """)

    # Check business names button
    if st.button("🔎 Check Business Names"):
        with st.spinner("Retrieving business names from Pinecone..."):
            try:
                business_names = get_business_names_from_pinecone()
                if business_names:
                    st.success(f"✅ Found {len(business_names)} business names.")
                    st.write(business_names[:10])  # Show sample
                else:
                    st.warning("⚠️ No business names found in Pinecone (maps namespace may be empty).")
            except Exception as e:
                st.error(f"❌ Error fetching business names: {e}")

    # Generate keywords button
    if st.button("🚀 Generate Keywords & Get Search Volume"):
        with st.spinner(f"Running {'enhanced' if use_ai_refinement else 'standard'} keyword generation pipeline..."):
            try:
                if use_ai_refinement:
                    success = run_enhanced_keyword_pipeline(city)
                else:
                    success = run_keyword_pipeline(city)
                
                if success:
                    st.success("✅ Keyword pipeline completed!")
                else:
                    st.error("❌ Keyword pipeline failed")
            except Exception as e:
                st.error(f"❌ Error running keyword pipeline: {e}")
                import traceback
                st.code(traceback.format_exc())

# ------ ADD A NEW DIRECT KEYWORD REFINEMENT SECTION ------
"""
Add this new section to the "Keywords & Search Volume" tab after the current code:
"""

    # Add a new expander for direct keyword refinement
    with st.expander("Direct Keyword Refinement Tool"):
        st.subheader("Refine Existing Keywords")
        
        raw_keywords = st.text_area(
            "Enter raw keywords (one per line):",
            placeholder="ZARA Bengaluru timings\nH&M Indiranagar directions\nLevi's Majestic opening hours"
        )
        
        col1, col2 = st.columns(2)
        
        with col1:
            brand_input = st.text_input("Brand names (comma-separated):", "ZARA, H&M, Levi's")
        
        with col2:
            refine_city = st.text_input("City:", city)
        
        if st.button("✨ Refine Keywords"):
            if raw_keywords:
                keywords_list = [k.strip() for k in raw_keywords.split("\n") if k.strip()]
                brand_list = [b.strip() for b in brand_input.split(",") if b.strip()]
                
                with st.spinner("Refining keywords using AI..."):
                    try:
                        refined_keywords = refine_keywords(keywords_list, brand_list, refine_city)
                        
                        if refined_keywords:
                            st.success(f"✅ Refined {len(keywords_list)} keywords to {len(refined_keywords)} keywords")
                            
                            # Display before and after
                            col1, col2 = st.columns(2)
                            with col1:
                                st.write("📝 Original Keywords:")
                                for kw in keywords_list:
                                    st.write(f"- {kw}")
                            
                            with col2:
                                st.write("✨ Refined Keywords:")
                                for kw in refined_keywords:
                                    st.write(f"- {kw}")
                            
                            # Option to use these refined keywords
                            if st.button("Use These Refined Keywords"):
                                with st.spinner("Getting search volumes and uploading to Pinecone..."):
                                    df = get_search_volumes(refined_keywords)
                                    
                                    if not df.empty:
                                        # Add city to DataFrame
                                        df = df.assign(city=refine_city)
                                        
                                        # Upsert to Pinecone
                                        from src.embed_upsert import upsert_keywords
                                        upsert_keywords(df, refine_city)
                                        
                                        # Also save to CSV
                                        df.to_csv("keyword_volumes.csv", index=False)
                                        
                                        st.success("✅ Keywords processed and uploaded to Pinecone!")
                                    else:
                                        st.error("❌ Failed to get search volumes for refined keywords")
                        else:
                            st.warning("⚠️ Keyword refinement returned no results")
                    except Exception as e:
                        st.error(f"❌ Error during keyword refinement: {e}")
            else:
                st.warning("Please enter some keywords to refine")

# ------ DIAGNOSTICS TAB UPDATES ------
"""
Add this section to the "Diagnostic" tab to test the OpenAI Assistant connection:
"""

    # Add OpenAI Assistant test
    st.subheader("OpenAI Assistant Test")
    if st.button("Test OpenAI Assistant Connection"):
        try:
            from src.openai_keyword_refiner import create_assistant
            
            with st.spinner("Testing OpenAI Assistant creation..."):
                assistant_id = create_assistant()
                
                if assistant_id:
                    st.success(f"✅ Successfully created OpenAI Assistant (ID: {assistant_id[:10]}...)")
                else:
                    st.error("❌ Failed to create OpenAI Assistant")
        except Exception as e:
            st.error(f"❌ Error testing OpenAI Assistant: {e}")
            import traceback
            st.code(traceback.format_exc())
