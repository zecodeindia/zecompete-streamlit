"""
streamlit_trend_visualization.py - Trend visualization for keyword search volumes
"""
import streamlit as st
import pandas as pd
import plotly.express as px
import os
import datetime

def load_keyword_data():
    """Load keyword data from CSV file"""
    try:
        # Try to load from the data directory first
        data_path = os.path.join("data", "keyword_volumes.csv")
        if os.path.exists(data_path):
            return pd.read_csv(data_path)
        
        # Try to load from the root directory
        if os.path.exists("keyword_volumes.csv"):
            return pd.read_csv("keyword_volumes.csv")
        
        # If no file exists, return empty DataFrame
        st.warning("No keyword data file found. Please run the keyword pipeline first.")
        return pd.DataFrame()
    except Exception as e:
        st.error(f"Error loading keyword data: {str(e)}")
        return pd.DataFrame()

def display_trend_visualization():
    """Display trend visualization for keyword search volumes"""
    st.title("Keyword Search Volume Trends")
    
    # Load keyword data
    df = load_keyword_data()
    
    if df.empty:
        st.info("No keyword data available. Please run the keyword pipeline to generate data.")
        return
    
    # Convert data types
    df["year"] = pd.to_numeric(df["year"], errors="coerce").fillna(0).astype(int)
    df["month"] = pd.to_numeric(df["month"], errors="coerce").fillna(0).astype(int)
    df["search_volume"] = pd.to_numeric(df["search_volume"], errors="coerce").fillna(0).astype(int)
    
    # Create date field for better visualization
    df["date"] = pd.to_datetime(df[["year", "month"]].assign(day=1))
    
    # Get unique keywords
    keywords = sorted(df["keyword"].unique().tolist())
    
    # Sidebar filters
    st.sidebar.header("Filters")
    
    # Keyword selection
    selected_keywords = st.sidebar.multiselect(
        "Select Keywords to Compare",
        options=keywords,
        default=keywords[:3] if len(keywords) >= 3 else keywords
    )
    
    # Date range selection
    min_date = df["date"].min()
    max_date = df["date"].max()
    
    date_range = st.sidebar.date_input(
        "Date Range",
        value=(min_date, max_date),
        min_value=min_date,
        max_value=max_date
    )
    
    if len(date_range) == 2:
        start_date, end_date = date_range
    else:
        start_date, end_date = min_date, max_date
    
    # Filter data based on selections
    filtered_df = df[
        df["keyword"].isin(selected_keywords) &
        (df["date"] >= pd.to_datetime(start_date)) &
        (df["date"] <= pd.to_datetime(end_date))
    ]
    
    if filtered_df.empty:
        st.warning("No data available for the selected filters.")
        return
    
    # Display trend chart
    st.subheader("Search Volume Trends Over Time")
    
    # Create line chart with Plotly
    fig = px.line(
        filtered_df,
        x="date",
        y="search_volume",
        color="keyword",
        markers=True,
        title="Keyword Search Volume Trends",
        labels={"date": "Date", "search_volume": "Monthly Search Volume", "keyword": "Keyword"}
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
    
    # Display trend metrics
    st.subheader("Trend Metrics")
    
    # Calculate metrics for each keyword
    metrics_cols = st.columns(len(selected_keywords) if selected_keywords else 1)
    
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
            
            with metrics_cols[i % len(metrics_cols)]:
                st.metric(
                    label=keyword,
                    value=f"{int(last_volume)} searches",
                    delta=f"{change:+d} ({percent_change:.1f}%)",
                    delta_color="normal" if change >= 0 else "inverse"
                )
                st.caption(f"Avg: {avg_volume:.1f} | Min: {min_volume} | Max: {max_volume}")
    
    # Display seasonality analysis
    st.subheader("Seasonality Analysis")
    
    # Aggregate data by month for all selected keywords
    month_map = {
        1: "Jan", 2: "Feb", 3: "Mar", 4: "Apr", 5: "May", 6: "Jun",
        7: "Jul", 8: "Aug", 9: "Sep", 10: "Oct", 11: "Nov", 12: "Dec"
    }
    
    monthly_avg = filtered_df.groupby(["keyword", "month"])["search_volume"].mean().reset_index()
    monthly_avg["month_name"] = monthly_avg["month"].map(month_map)
    
    # Create bar chart for monthly averages
    fig_monthly = px.bar(
        monthly_avg,
        x="month_name",
        y="search_volume",
        color="keyword",
        barmode="group",
        title="Average Monthly Search Volume (Seasonality)",
        labels={"month_name": "Month", "search_volume": "Average Search Volume", "keyword": "Keyword"}
    )
    
    # Sort months in correct order
    month_order = [month_map[i] for i in range(1, 13)]
    fig_monthly.update_xaxes(categoryorder="array", categoryarray=month_order)
    
    # Display the chart
    st.plotly_chart(fig_monthly, use_container_width=True)
    
    # Display raw data option
    if st.checkbox("Show Raw Data"):
        st.subheader("Raw Trend Data")
        st.dataframe(
            filtered_df[["keyword", "year", "month", "search_volume", "competition", "cpc"]].sort_values(
                ["keyword", "year", "month"]
            ),
            use_container_width=True
        )
    
    # Download options
    st.subheader("Download Data")
    
    csv = filtered_df.to_csv(index=False).encode("utf-8")
    st.download_button(
        label="Download Filtered Data as CSV",
        data=csv,
        file_name=f"keyword_trends_{datetime.datetime.now().strftime('%Y%m%d')}.csv",
        mime="text/csv"
    )

if __name__ == "__main__":
    display_trend_visualization()
