# Update streamlit_app.py
import os, itertools, pandas as pd, streamlit as st
from src.run_pipeline import run
from src.analytics    import insight_question
from pinecone import Pinecone  # Updated import

st.set_page_config(page_title="Competitor Mapper", layout="wide")
st.title("🗺️  Competitor Location & Demand Explorer")

brands = st.text_input("Brands (comma)", "Zudio, Max Fashion")
cities = st.text_input("Cities (comma)", "Bengaluru, Hyderabad")

if st.button("Run analysis"):
    for b, c in itertools.product(
            map(str.strip, brands.split(",")),
            map(str.strip, cities.split(","))):
        run(b, c)
    st.success("Data ready!")

# Updated Pinecone initialization
pc = Pinecone(api_key=st.secrets["PINECONE_API_KEY"])
idx = pc.Index("zecompete")  # or use host="https://zecompete-1df1x61.svc.aped-4627-b74a.pinecone.io"

tabs = st.tabs(["Ask", "Preview"])
with tabs[0]:
    q = st.text_area("Ask a question about the data")
    if st.button("Answer") and q:
        st.write(insight_question(q))
with tabs[1]:
    res = idx.describe_index_stats()
    st.json(res)