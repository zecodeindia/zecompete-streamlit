# This code snippet shows how to fix the tab integration
# Add this to your streamlit_app.py file

# First, ensure you have the correct imports at the top:
import os, itertools, pandas as pd, streamlit as st
import time, json, threading, requests, secrets
import matplotlib.pyplot as plt
import numpy as np
import datetime

# [Keep your existing imports and configurations]

# Set up the app
st.set_page_config(page_title="Competitor Mapper", layout="wide")
st.title("🗺️ Competitor Location & Demand Explorer")

# [Keep your existing initialization code]

# Create tabs with explicit labels to avoid confusion
tabs = st.tabs([
    "Run Analysis", 
    "Auto Integration", 
    "Keywords & Search Volume", 
    "Keyword Trends",  # Make sure this tab name is exactly as shown
    "Manual Upload", 
    "Ask Questions", 
    "Explore Data", 
    "Diagnostic"
])

# [Keep your tab implementations for tabs 0, 1, 2]

# Make sure tabs[3] is implemented correctly for Keyword Trends
with tabs[3]:  # This MUST be index 3 for "Keyword Trends"
    # Clear header for this section to avoid confusion
    st.subheader("Keyword Search Volume Trends")
    
    # Define the keyword trends tab content
    def show_keyword_trends():
        """Display keyword search volume trends"""
        # Check if trend data exists
        trend_file_path = "keyword_volumes.csv"
        if not os.path.exists(trend_file_path):
            st.info("No trend data available. Please run the keyword pipeline first to generate trend data.")
            if st.button("Generate Keywords with Trends"):
                try:
                    from src.keyword_pipeline import run_keyword_pipeline
                    city = st.session_state.get("last_city", "Bengaluru")
                    with st.spinner(f"Generating keywords with trend data for {city}..."):
                        success = run_keyword_pipeline(city)
                        if success:
                            st.success(f"✅ Generated keywords with trend data for {city}")
                            st.rerun()
                        else:
                            st.error("❌ Failed to generate keywords")
                except Exception as e:
                    st.error(f"Error generating keywords: {str(e)}")
            return
        
        # Load trend data
        try:
            df = pd.read_csv(trend_file_path)
            
            # Ensure proper data types
            df["year"] = pd.to_numeric(df["year"], errors="coerce").fillna(0).astype(int)
            df["month"] = pd.to_numeric(df["month"], errors="coerce").fillna(0).astype(int)
            df["search_volume"] = pd.to_numeric(df["search_volume"], errors="coerce").fillna(0).astype(int)
            
            # Create date field for better visualization
            df["date"] = pd.to_datetime(df[["year", "month"]].assign(day=1))
            
            # Get unique keywords
            keywords = sorted(df["keyword"].unique().tolist())
            
            # Keyword selection
            selected_keywords = st.multiselect(
                "Select Keywords to Compare",
                options=keywords,
                default=keywords[:3] if len(keywords) >= 3 else keywords
            )
            
            if not selected_keywords:
                st.warning("Please select at least one keyword to visualize trends.")
                return
            
            # Filter data for selected keywords
            filtered_df = df[df["keyword"].isin(selected_keywords)]
            
            # Create line chart with matplotlib
            fig, ax = plt.subplots(figsize=(10, 6))
            
            # Plot each keyword
            for i, keyword in enumerate(selected_keywords):
                keyword_data = filtered_df[filtered_df["keyword"] == keyword].sort_values("date")
                ax.plot(keyword_data["date"], keyword_data["search_volume"], marker='o', label=keyword)
            
            # Set chart properties
            ax.set_xlabel("Date")
            ax.set_ylabel("Search Volume")
            ax.set_title("Keyword Search Volume Trends")
            ax.legend()
            
            # Format x-axis dates
            fig.autofmt_xdate()
            
            # Display the chart
            st.pyplot(fig)
            
            # Display metrics for each keyword
            st.subheader("Trend Metrics")
            
            # Create columns for metrics
            cols = st.columns(min(len(selected_keywords), 3))
            
            for i, keyword in enumerate(selected_keywords):
                keyword_data = filtered_df[filtered_df["keyword"] == keyword].sort_values("date")
                
                if len(keyword_data) >= 2:
                    first_volume = keyword_data.iloc[0]["search_volume"]
                    last_volume = keyword_data.iloc[-1]["search_volume"]
                    max_volume = keyword_data["search_volume"].max()
                    min_volume = keyword_data["search_volume"].min()
                    avg_volume = keyword_data["search_volume"].mean()
                    
                    change = last_volume - first_volume
                    percent_change = (change / first_volume * 100) if first_volume > 0 else 0
                    
                    with cols[i % len(cols)]:
                        st.metric(
                            label=keyword,
                            value=f"{int(last_volume)} searches",
                            delta=f"{change:+d} ({percent_change:.1f}%)"
                        )
                        st.caption(f"Avg: {avg_volume:.1f} | Min: {min_volume} | Max: {max_volume}")
            
            # Display raw data option
            if st.checkbox("Show Raw Trend Data"):
                st.dataframe(
                    filtered_df[["keyword", "date", "search_volume", "competition", "cpc"]].sort_values(
                        ["keyword", "date"]
                    ),
                    use_container_width=True
                )
            
            # Download option
            csv = filtered_df.to_csv(index=False).encode("utf-8")
            st.download_button(
                label="⬇️ Download Trend Data as CSV",
                data=csv,
                file_name=f"keyword_trends_{datetime.datetime.now().strftime('%Y%m%d')}.csv",
                mime="text/csv"
            )
            
        except Exception as e:
            st.error(f"Error loading or processing trend data: {str(e)}")
            import traceback
            st.code(traceback.format_exc())
    
    # Call the function to show keyword trends
    show_keyword_trends()

# [Continue with your implementations for tabs 4, 5, 6, 7]
# Make sure you're using the correct indices for each tab

# NOTE: If the above doesn't work, try this approach:
# st.markdown(
#     """
#     <style>
#     .stTabs [data-baseweb="tab-list"] button [data-testid="stMarkdownContainer"] p {
#         font-weight: bold;
#     }
#     </style>
#     """,
#     unsafe_allow_html=True,
# )
