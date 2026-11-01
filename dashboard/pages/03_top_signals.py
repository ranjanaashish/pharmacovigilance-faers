"""Global Safety Signals Surveillance Page for Pharmacovigilance FAERS Dashboard.

Enables pharmacovigilance officers to prioritize and triage emerging safety signals
across the entire FAERS adverse event corpus based on PRR and fatality thresholds.
Institutional clinical styling with zero emojis.
"""

import sys
from pathlib import Path
import streamlit as st
import pandas as pd

dashboard_dir = Path(__file__).resolve().parent.parent
if str(dashboard_dir) not in sys.path:
    sys.path.append(str(dashboard_dir))

from data_loader import load_signals_data
from components.signal_table import render_signal_table
from components.charts import signal_bar
from components.theme import apply_healthcare_theme, render_top_nav, render_header


st.set_page_config(
    page_title="Signal Triage Registry | FAERS",
    layout="wide",
)

apply_healthcare_theme()
render_top_nav(active_module="Signal Triage Registry")

render_header(
    title="Global Disproportionality Signal Triage Registry",
    subtitle="National post-marketing surveillance registry identifying active compounds and MedDRA preferred terms exhibiting statistical reporting disproportionality.",
)

signals_df, _ = load_signals_data()

if signals_df is None or signals_df.empty:
    st.warning("No signal data loaded in memory. Verify data/processed/ pipeline artifacts.")
    st.stop()

# Triage Filter Controls
with st.expander("Surveillance Thresholds & Triage Parameters", expanded=True):
    f1, f2, f3 = st.columns(3)

    with f1:
        min_prr = st.slider(
            "Minimum PRR (Disproportionality):",
            min_value=2.0,
            max_value=20.0,
            value=2.0,
            step=0.5,
            help="Evans' threshold requires PRR >= 2.0 for confirmed disproportionality.",
        )

    with f2:
        min_cases = st.slider(
            "Minimum Case Count (a):",
            min_value=3,
            max_value=100,
            value=5,
            step=1,
            help="Minimum observed case reports for drug-reaction pair to exclude small-sample artifacts.",
        )

    with f3:
        min_chi2 = st.slider(
            "Minimum Chi-Square (χ²):",
            min_value=4.0,
            max_value=100.0,
            value=4.0,
            step=2.0,
            help="Pearson Chi-Square threshold with 1 df. Values >= 4.0 are statistically significant at p < 0.05.",
        )

# Apply Filter
filtered_df = signals_df.copy()
if "prr" in filtered_df.columns:
    filtered_df = filtered_df[filtered_df["prr"] >= min_prr]
if "case_count" in filtered_df.columns:
    filtered_df = filtered_df[filtered_df["case_count"] >= min_cases]
if "chi_square" in filtered_df.columns:
    filtered_df = filtered_df[filtered_df["chi_square"] >= min_chi2]

# Summary KPI Cards
c1, c2, c3, c4 = st.columns(4)
with c1:
    st.metric("Retained Safety Signals", f"{len(filtered_df):,}")
with c2:
    flagged_drugs = filtered_df["drug_name"].nunique() if "drug_name" in filtered_df.columns else 0
    st.metric("Flagged Active Substances", f"{flagged_drugs:,}")
with c3:
    max_prr = filtered_df["prr"].max() if not filtered_df.empty and "prr" in filtered_df.columns else 0.0
    st.metric("Peak Disproportionality", f"{max_prr:.1f}x")
with c4:
    high_fatal = (filtered_df["fatality_rate"] >= 0.15).sum() if not filtered_df.empty and "fatality_rate" in filtered_df.columns else 0
    st.metric("High Mortality Signals (>15%)", f"{high_fatal:,}")

st.markdown("<hr class='clinical-divider'>", unsafe_allow_html=True)

# Ranking Visualizations
col_rank1, col_rank2 = st.columns(2)

with col_rank1:
    if "drug_name" in filtered_df.columns and not filtered_df.empty:
        drug_counts = filtered_df["drug_name"].value_counts().reset_index()
        drug_counts.columns = ["drug_name", "signal_count"]
        st.plotly_chart(
            signal_bar(
                drug_counts,
                x_col="signal_count",
                y_col="drug_name",
                title="Top Substances by Validated Signal Count",
                orientation="h",
                limit=12,
            ),
            use_container_width=True,
        )

with col_rank2:
    if "reaction" in filtered_df.columns and "prr" in filtered_df.columns and not filtered_df.empty:
        rx_agg = filtered_df.groupby("reaction")["prr"].mean().reset_index()
        rx_agg.columns = ["reaction", "mean_prr"]
        st.plotly_chart(
            signal_bar(
                rx_agg,
                x_col="mean_prr",
                y_col="reaction",
                title="Top Adverse Reactions by Mean Disproportionality",
                orientation="h",
                limit=12,
            ),
            use_container_width=True,
        )

st.markdown("<hr class='clinical-divider'>", unsafe_allow_html=True)

# Tabular Registry
st.markdown("### National Safety Signal Priority Matrix")
render_signal_table(filtered_df, title="Global Priority Matrix")
