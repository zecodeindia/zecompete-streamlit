"""
business_keywords_tab.py - Streamlit component for business names to keyword pipeline
"""
import streamlit as st
import pandas as pd
import plotly.express as px
import os
import datetime
from typing import List

# Import the pipeline functions
try:
    from enhanced_keyword_pipeline import (
        extract_business_names_from_pinecone,
        preprocess_business_names,
        get_search_volume_with_history,
        run_business_keyword_pipeline,
        combine_data_for_assistant
    )
    pipeline_imported = True
except ImportError:
    pipeline_imported = False

def render_business_keywords_tab():
    """Render the Business Keywords tab in the Streamlit app"""
    st.header("Business Names to Keywords Pipeline")
    
    if not pipeline_imported:
        st.error("⚠️ Enhanced keyword pipeline module not found. Please ensure it's properly installed.")
        return
    
    # Tabs for different functions
    tab1, tab2, tab3 = st.tabs(["Run Pipeline", "View Results", "Combined Assistant Data"])
    
    with tab1:
        st.subheader("Extract Business Names → Keywords → Search Volume")
        
        # Input city name for context
        city = st.text_input("City for keyword context", "Bengaluru", key="bk_city_input")
        
        # Button to extract business names
        if st.button("🔎 Extract Business Names from Pinecone", key="extract_biz_names"):
            with st.spinner("Extracting business names from Pinecone..."):
                try:
                    business_names = extract_business_names_from_pinecone()
                    if business_names:
                        st.success(f"✅ Found {len(business_names)} business names")
                        
                        # Display business names
                        with st.expander("View Business Names"):
                            for i, name in enumerate(business_names):
                                st.write(f"{i+1}. {name}")
                        
                        # Store names in session state for further processing
                        st.session_state.business_names = business_names
                    else:
                        st.warning("⚠️ No business names found in Pinecone maps namespace.")
                except Exception as e:
                    st.error(f"❌ Error extracting business names: {str(e)}")
        
        # Button to generate keywords from business names
        if st.button("🔄 Generate Keywords from Business Names", key="gen_kw_from_biz"):
            if "business_names" not in st.session_state or not st.session_state.business_names:
                st.warning("Please extract business names first.")
            else:
                with st.spinner("Generating keywords from business names..."):
                    try:
                        keywords = preprocess_business_names(st.session_state.business_names, city)
                        if keywords:
                            st.success(f"✅ Generated {len(keywords)} keywords from business names")
                            
                            # Display keywords
                            with st.expander("View Generated Keywords"):
                                for i, keyword in enumerate(keywords):
                                    st.write(f"{i+1}. {keyword}")
                            
                            # Store keywords in session state
                            st.session_state.keywords = keywords
                        else:
                            st.warning("⚠️ No keywords generated from business names.")
                    except Exception as e:
                        st.error(f"❌ Error generating keywords: {str(e)}")
        
        # Button to get search volume for keywords
        if st.button("📊 Get Search Volume Data", key="get_sv_data"):
            if "keywords" not in st.session_state or not st.session_state.keywords:
                st.warning("Please generate keywords first.")
            else:
                with st.spinner("Fetching search volume data with 12-month history..."):
                    try:
                        df = get_search_volume_with_history(st.session_state.keywords)
                        if not df.empty:
                            st.success(f"✅ Retrieved search volume data for {df['keyword'].nunique()} keywords")
                            
                            # Display sample data
                            with st.expander("View Sample Data"):
                                st.dataframe(df.head(10))
                            
                            # Store dataframe in session state
                            st.session_state.keyword_data = df
                            
                            # Option to save CSV
                            csv = df.to_csv(index=False).encode("utf-8")
                            st.download_button(
                                label="⬇️ Download Keyword Data as CSV",
                                data=csv,
                                file_name=f"keyword_volumes_{datetime.datetime.now().strftime('%Y%m%d')}.csv",
                                mime="text/csv",
                                key="bk_download_csv"
                            )
                        else:
                            st.warning("⚠️ No search volume data retrieved.")
                    except Exception as e:
                        st.error(f"❌ Error getting search volume data: {str(e)}")
        
        # Divider
        st.markdown("---")
        
        # Button to run the full pipeline
        st.subheader("Run Full Pipeline")
        if st.button("🚀 Run Complete Business → Keywords Pipeline", key="run_biz_kw_pipeline"):
            with st.spinner("Running full business to keywords pipeline..."):
                try:
                    success = run_business_keyword_pipeline(city)
                    if success:
                        st.success(f"""
                        ✅ Successfully completed the full pipeline:
                        1. Extracted business names from Pinecone maps namespace
                        2. Generated keywords from business names
                        3. Retrieved 12-month search volume history
                        4. Stored data in Pinecone keywords namespace
                        """)
                    else:
                        st.error("❌ Pipeline execution failed. Please check the logs for details.")
                except Exception as e:
                    st.error(f"❌ Error running pipeline: {str(e)}")
    
    with tab2:
        st.subheader("View Keyword Search Volume Results")
        
        # Load and display results
        results_df = None
        
        # Try to get data from session state first
        if "keyword_data" in st.session_state and not st.session_state.keyword_data.empty:
            results_df = st.session_state.keyword_data
        else:
            # Try to load from file
            try:
                if os.path.exists("keyword_volumes.csv"):
                    results_df = pd.read_csv("keyword_volumes.csv")
            except Exception as e:
                st.error(f"Error loading keyword data: {str(e)}")
        
        if results_df is not None and not results_df.empty:
            # Display summary statistics
            st.write(f"📊 Data for {results_df['keyword'].nunique()} keywords with {len(results_df)} monthly data points")
            
            # Create date field if needed
            if "date" not in results_df.columns:
                results_df["date"] = pd.to_datetime(results_df[["year", "month"]].assign(day=1))
            
            # Display top keywords by volume
            st.subheader("Top Keywords by Search Volume")
            top_keywords = results_df.groupby("keyword")["search_volume"].mean().sort_values(ascending=False)
            
            # Show bar chart of top 10 keywords
            fig = px.bar(
                top_keywords.reset_index().head(10),
                x="keyword",
                y="search_volume",
                title="Top 10 Keywords by Average Search Volume",
                labels={"search_volume": "Avg. Monthly Search Volume", "keyword": "Keyword"},
                color="search_volume",
                color_continuous_scale=px.colors.sequential.Viridis
            )
            st.plotly_chart(fig, use_container_width=True)
            
            # Keyword selector for trend visualization
            st.subheader("Keyword Trend Analysis")
            
            # Get unique keywords
            keywords = sorted(results_df["keyword"].unique().tolist())
            
            # Keyword selection
            selected_keywords = st.multiselect(
                "Select Keywords to Compare",
                options=keywords,
                default=keywords[:3] if len(keywords) >= 3 else keywords,
                key="bk_selected_kw"  # Add a unique key here to avoid duplication
            )
            
            if selected_keywords:
                # Filter data for selected keywords
                filtered_df = results_df[results_df["keyword"].isin(selected_keywords)]
                
                # Create line chart with Plotly
                fig = px.line(
                    filtered_df,
                    x="date",
                    y="search_volume",
                    color="keyword",
                    markers=True,
                    title="Keyword Search Volume Trends (Last 12 Months)",
                    labels={"date": "Month", "search_volume": "Search Volume", "keyword": "Keyword"}
                )
                
                # Customize the chart
                fig.update_layout(
                    xaxis_title="Month",
                    yaxis_title="Search Volume",
                    legend_title="Keywords",
                    hovermode="x unified"
                )
                
                # Display the chart
                st.plotly_chart(fig, use_container_width=True)
                
                # Display metrics for each keyword
                st.subheader("Keyword Metrics")
                
                # Create columns for metrics
                metrics_cols = st.columns(len(selected_keywords) if len(selected_keywords) <= 3 else 3)
                
                for i, keyword in enumerate(selected_keywords):
                    keyword_data = filtered_df[filtered_df["keyword"] == keyword]
                    if len(keyword_data) > 0:
                        avg_volume = keyword_data["search_volume"].mean()
                        competition = keyword_data["competition"].mean()
                        cpc = keyword_data["cpc"].mean()
                        
                        with metrics_cols[i % len(metrics_cols)]:
                            st.metric(
                                label=keyword,
                                value=f"{int(avg_volume)} searches"
                            )
                            st.caption(f"Competition: {competition:.2f}")
                            st.caption(f"CPC: ${cpc:.2f}")
                
                # Raw data viewer
                if st.checkbox("Show Raw Data", key="bk_show_raw"):
                    st.dataframe(
                        filtered_df[["keyword", "date", "search_volume", "competition", "cpc"]].sort_values(
                            ["keyword", "date"]
                        ),
                        use_container_width=True
                    )
            else:
                st.info("Please select at least one keyword to visualize trends.")
        else:
            st.info("No keyword data available. Please run the pipeline to generate data.")
    
    with tab3:
        st.subheader("Combined Data for OpenAI Assistant")
        
        # Input for natural language query
        query = st.text_input("Enter a question to re
