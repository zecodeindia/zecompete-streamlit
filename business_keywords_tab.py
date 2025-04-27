"""
business_keywords_tab.py – Streamlit component
Business-names → keyword pipeline UI + basic reporting
"""

from __future__ import annotations

import datetime as _dt
import os
from types import ModuleType
from typing import List, Optional, Tuple

import pandas as pd
import plotly.express as px
import streamlit as st

# ── resilient import of the keyword-pipeline helpers ───────────────────────────
try:  # new location (src/ sub-package)
    from src.enhanced_keyword_pipeline import (  # type: ignore
        combine_data_for_assistant,
        extract_business_names_from_pinecone,
        get_search_volume_with_history,
        preprocess_business_names,
        run_business_keyword_pipeline,
    )

except ModuleNotFoundError:
    # fall back to the old flat layout
    from enhanced_keyword_pipeline import (  # type: ignore
        combine_data_for_assistant,
        extract_business_names_from_pinecone,
        get_search_volume_with_history,
        preprocess_business_names,
        run_business_keyword_pipeline,
    )


def _nice_csv_download(df: pd.DataFrame, *, prefix: str) -> None:
    """Helper to present a download button for a DataFrame."""
    csv_bytes = df.to_csv(index=False).encode("utf-8")
    fname = f"{prefix}_{_dt.datetime.now():%Y%m%d_%H%M%S}.csv"
    st.download_button(
        "⬇️ Download CSV",
        data=csv_bytes,
        file_name=fname,
        mime="text/csv",
        key=f"dl_{prefix}",
    )


# ══════════════════════════════════════════════════════════════════════════════
#                              MAIN TAB RENDERER
# ══════════════════════════════════════════════════════════════════════════════
def render_business_keywords_tab() -> None:  # noqa: C901 (streamlit UI; okay)
    """Render the whole 'Business Keywords' tab group inside the caller tab."""
    st.header("🔑 Business Names → Keyword Pipeline")

    # bail out gracefully if helpers are missing
    if "extract_business_names_from_pinecone" not in globals():
        st.error(
            "Enhanced keyword-pipeline module could not be imported. "
            "Make sure it exists in either `src/` or the repo root."
        )
        return

    tab_run, tab_results, tab_assistant = st.tabs(
        ["Run pipeline", "View results", "Ask the assistant"]
    )

    # ───────────────────────── Tab 1 – run the pipeline ───────────────────────
    with tab_run:
        st.subheader("Step-by-step execution")

        city = st.text_input("City for keyword context", "Bengaluru")

        # 1) extract business names ------------------------------------------------
        if st.button("🔎 Extract business names (Pinecone)", key="btn_extract_biz"):
            with st.spinner("Querying Pinecone …"):
                try:
                    biz_names = extract_business_names_from_pinecone()
                    st.session_state["biz_names"] = biz_names
                except Exception as exc:
                    st.exception(exc)
                    biz_names = []

            if biz_names:
                st.success(f"Found **{len(biz_names)}** businesses.")
                with st.expander("Show names"):
                    st.write(biz_names)
            else:
                st.warning("No businesses found in the `maps` namespace.")

        # 2) generate keywords -----------------------------------------------------
        if st.button("🔄 Generate keywords", key="btn_gen_kw"):
            names: List[str] = st.session_state.get("biz_names", [])
            if not names:
                st.warning("Please run step 1 first.")
            else:
                with st.spinner("Generating keywords …"):
                    kw = preprocess_business_names(names, city)
                    st.session_state["keywords"] = kw
                st.success(f"Created **{len(kw)}** keywords.")
                st.write(kw)

        # 3) fetch search volume ---------------------------------------------------
        if st.button("📊 Fetch search-volume history", key="btn_fetch_sv"):
            kw: List[str] = st.session_state.get("keywords", [])
            if not kw:
                st.warning("Generate keywords first.")
            else:
                with st.spinner("Calling DataForSEO (or fallback)…"):
                    df = get_search_volume_with_history(kw)
                    st.session_state["keyword_df"] = df

                if df.empty:
                    st.error("No data returned.")
                else:
                    st.success("Volume data retrieved.")
                    st.dataframe(df.head())
                    _nice_csv_download(df, prefix="keyword_volumes")

        st.divider()
        # 4) one-click: run everything --------------------------------------------
        st.subheader("🚀 Run full pipeline")
        if st.button("Run **business → keywords** pipeline"):
            with st.spinner("Full pipeline running …"):
                ok = run_business_keyword_pipeline(city)
            st.success("Pipeline completed." if ok else "Pipeline failed.")

    # ───────────────────────── Tab 2 – view results ───────────────────────────
    with tab_results:
        st.subheader("Keyword search-volume insights")

        df: Optional[pd.DataFrame] = st.session_state.get("keyword_df")
        # try to load a CSV that the pipeline exported earlier
        if df is None:
            csv_path = os.path.join("data", "keyword_volumes.csv")
            if os.path.exists(csv_path):
                try:
                    df = pd.read_csv(csv_path)
                except Exception:
                    df = None

        if df is None or df.empty:
            st.info("Run the pipeline first to populate data.")
            st.stop()

        # ensure a datetime column
        if "date" not in df.columns:
            df["date"] = pd.to_datetime(
                df[["year", "month"]].assign(day=1), errors="coerce"
            )

        # top keywords by average volume
        top_kw = (
            df.groupby("keyword")["search_volume"]
            .mean()
            .sort_values(ascending=False)
            .head(15)
        )
        fig_bar = px.bar(
            top_kw.reset_index(),
            x="keyword",
            y="search_volume",
            title="Top keywords (avg. monthly volume)",
            labels={"search_volume": "Avg. monthly volume"},
        )
        st.plotly_chart(fig_bar, use_container_width=True)

        # trend comparison
        kw_choices = sorted(df["keyword"].unique())
        sel_kw = st.multiselect(
            "Compare keyword trends", options=kw_choices, default=kw_choices[:3]
        )
        if sel_kw:
            trend_df = df[df["keyword"].isin(sel_kw)]
            fig_line = px.line(
                trend_df,
                x="date",
                y="search_volume",
                color="keyword",
                markers=True,
                title="12-month trend",
                labels={"search_volume": "Monthly volume"},
            )
            st.plotly_chart(fig_line, use_container_width=True)

        # raw data toggle
        if st.checkbox("Show raw data"):
            st.dataframe(
                trend_df.sort_values(["keyword", "date"]),
                use_container_width=True,
                hide_index=True,
            )

    # ────────────────── Tab 3 – ask the assistant (LLM) ───────────────────────
    with tab_assistant:
        st.subheader("Natural-language Q&A")

        q = st.text_input(
            "Ask a question about the keyword data",
            placeholder="E.g. Which location shows the fastest growth?",
        )
        if st.button("🤖 Ask", key="btn_ask_assistant"):
            if not q:
                st.warning("Type a question first.")
            else:
                with st.spinner("Assistant thinking …"):
                    try:
                        answer = combine_data_for_assistant(q)
                        st.success("Answer ready:")
                        st.write(answer)
                    except Exception as exc:
                        st.error(f"Assistant error: {exc}")
