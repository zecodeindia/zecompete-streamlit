"""
streamlit_keyword_trends.py - Standalone module for keyword trend visualization

This module can be imported directly into your main streamlit_app.py
"""
import streamlit as st
import pandas as pd
import os
import datetime
import matplotlib.pyplot as plt
import numpy as np

def show_keyword_trends():
    """Display keyword search volume trends using matplotlib"""
    st.header("Keyword Search Volume Trends")
    
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
        
        # Ensure required columns exist
        required_columns = ["keyword", "year", "month", "search_volume"]
        missing_columns = [col for col in required_columns if col not in df.columns]
        if missing_columns:
            st.error(f"Missing required columns in CSV: {', '.join(missing_columns)}")
            return
        
        # Ensure proper data types
        df["year"] = pd.to_numeric(df["year"], errors="coerce").fillna(0).astype(int)
        df["month"] = pd.to_numeric(df["month"], errors="coerce").fillna(0).astype(int)
        df["search_volume"] = pd.to_numeric(df["search_volume"], errors="coerce").fillna(0).astype(int)
        
        # Create date field for better visualization
        df["date"] = pd.to_datetime(df[["year", "month"]].assign(day=1))
        
        # Get unique keywords
        keywords = sorted(df["keyword"].unique().tolist())
        st.write(f"Found {len(keywords)} keywords with trend data")
        
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
        
        # Create a DataFrame with a complete date range for each keyword
        min_date = filtered_df["date"].min()
        max_date = filtered_df["date"].max()
        
        # Create line chart with matplotlib
        fig, ax = plt.subplots(figsize=(10, 6))
        
        # Color palette
        colors = ['#1f77b4', '#ff7f0e', '#2ca02c', '#d62728', '#9467bd', '#8c564b', '#e377c2', '#7f7f7f', '#bcbd22', '#17becf']
        
        # Plot each keyword
        for i, keyword in enumerate(selected_keywords):
            keyword_data = filtered_df[filtered_df["keyword"] == keyword].sort_values("date")
            
            if len(keyword_data) > 0:
                color = colors[i % len(colors)]
                ax.plot(
                    keyword_data["date"], 
                    keyword_data["search_volume"], 
                    marker='o', 
                    linestyle='-', 
                    linewidth=2,
                    markersize=6,
                    label=keyword,
                    color=color
                )
        
        # Set chart properties
        ax.set_xlabel("Date", fontsize=12)
        ax.set_ylabel("Search Volume", fontsize=12)
        ax.set_title("Keyword Search Volume Trends", fontsize=14, fontweight='bold')
        ax.grid(True, linestyle='--', alpha=0.7)
        ax.legend(fontsize=10)
        
        # Format x-axis dates
        fig.autofmt_xdate()
        
        # Display the chart
        st.pyplot(fig)
        
        # Display metrics for each keyword
        st.subheader("Trend Metrics")
        
        # Create columns for metrics
        metrics_container = st.container()
        cols = metrics_container.columns(min(len(selected_keywords), 3))
        
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
        
        # Monthly average visualization
        st.subheader("Monthly Averages")
        
        # Prepare data for monthly averages
        monthly_data = filtered_df.groupby(["keyword", "month"])["search_volume"].mean().reset_index()
        
        # Map month numbers to names
        month_names = {
            1: "Jan", 2: "Feb", 3: "Mar", 4: "Apr", 5: "May", 6: "Jun",
            7: "Jul", 8: "Aug", 9: "Sep", 10: "Oct", 11: "Nov", 12: "Dec"
        }
        monthly_data["month_name"] = monthly_data["month"].map(month_names)
        
        # Create monthly averages chart
        fig2, ax2 = plt.subplots(figsize=(10, 6))
        
        # Set width of bars
        bar_width = 0.8 / len(selected_keywords)
        
        # Plot bars for each keyword
        for i, keyword in enumerate(selected_keywords):
            keyword_monthly = monthly_data[monthly_data["keyword"] == keyword]
            
            if not keyword_monthly.empty:
                # Sort by month
                keyword_monthly = keyword_monthly.sort_values("month")
                
                # Set positions of bars on x-axis
                positions = np.arange(len(month_names)) + i * bar_width
                
                # Create bars
                ax2.bar(
                    positions, 
                    keyword_monthly["search_volume"],
                    width=bar_width,
                    label=keyword,
                    color=colors[i % len(colors)]
                )
        
        # Set chart properties
        ax2.set_xlabel("Month", fontsize=12)
        ax2.set_ylabel("Average Search Volume", fontsize=12)
        ax2.set_title("Monthly Average Search Volume", fontsize=14, fontweight='bold')
        ax2.set_xticks(np.arange(len(month_names)) + (bar_width * len(selected_keywords) / 2) - (bar_width / 2))
        ax2.set_xticklabels([month_names[m] for m in range(1, 13)])
        ax2.legend(fontsize=10)
        ax2.grid(True, axis='y', linestyle='--', alpha=0.7)
        
        # Display the chart
        st.pyplot(fig2)
        
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

if __name__ == "__main__":
    show_keyword_trends()
