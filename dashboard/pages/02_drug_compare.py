"""Drug Head-to-Head Comparison Page for Pharmacovigilance FAERS Dashboard.

Compares safety profiles, shared adverse reaction disproportionalities,
and demographic distributions between two active pharmaceutical ingredients.
Institutional clinical styling with zero emojis.
"""

import sys
from pathlib import Path
import streamlit as st
import pandas as pd

dashboard_dir = Path(__file__).resolve().parent.parent
if str(dashboard_dir) not in sys.path:
    sys.path.append(str(dashboard_dir))

from data_loader import load_signals_data, load_reports_data, get_available_drugs
from components.signal_table import render_signal_table
from components.charts import age_histogram, trend_line
from components.theme import apply_healthcare_theme, render_top_nav, render_header


st.set_page_config(
    page_title="Comparative Safety Profiling | FAERS",
    layout="wide",
)

apply_healthcare_theme()
render_top_nav(active_module="Comparative Safety Profiling")

render_header(
    title="Head-to-Head Comparative Safety Profiling",
    subtitle="Evaluate comparative disproportionality metrics, shared reaction frequencies, and overlaid demographic exposure for two active pharmaceutical agents.",
)

signals_df, _ = load_signals_data()
reports_df, _ = load_reports_data()
available_drugs = get_available_drugs()

if len(available_drugs) < 2:
    st.warning("At least two distinct drugs are required in the dataset to perform comparative analysis.")
    st.stop()

default_idx_a = available_drugs.index("ASPIRIN") if "ASPIRIN" in available_drugs else 0
atorv_candidates = [i for i, d in enumerate(available_drugs) if "ATORVASTATIN" in d.upper()]
default_idx_b = atorv_candidates[0] if atorv_candidates else min(1, len(available_drugs) - 1)

# Selection Controls
col_select_a, col_select_b = st.columns(2)

with col_select_a:
    drug_a = st.selectbox(
        "Select Primary Compound (Index Drug A):",
        options=available_drugs,
        index=default_idx_a,
        key="compare_drug_a",
    )

with col_select_b:
    drug_b = st.selectbox(
        "Select Comparator Compound (Reference Drug B):",
        options=available_drugs,
        index=default_idx_b,
        key="compare_drug_b",
    )

if drug_a == drug_b:
    st.info("The same compound is selected for both cohorts. Select two distinct compounds for differential profiling.")

# Extract Subsets
signals_a = signals_df[signals_df["drug_name"].str.upper() == drug_a].copy() if signals_df is not None and "drug_name" in signals_df.columns else pd.DataFrame()
signals_b = signals_df[signals_df["drug_name"].str.upper() == drug_b].copy() if signals_df is not None and "drug_name" in signals_df.columns else pd.DataFrame()

reports_a = reports_df[reports_df["drug_name"].str.upper() == drug_a].copy() if reports_df is not None and "drug_name" in reports_df.columns else pd.DataFrame()
reports_b = reports_df[reports_df["drug_name"].str.upper() == drug_b].copy() if reports_df is not None and "drug_name" in reports_df.columns else pd.DataFrame()


def compute_metrics(sig_df: pd.DataFrame, rep_df: pd.DataFrame):
    total_rep = len(rep_df) if not rep_df.empty else (int(sig_df["case_count"].sum()) if not sig_df.empty and "case_count" in sig_df.columns else 0)
    sig_count = len(sig_df)
    
    fatal_count = 0
    if not rep_df.empty and "fatal" in rep_df.columns:
        fatal_count = int(rep_df["fatal"].sum())
    elif not sig_df.empty and "fatality_rate" in sig_df.columns and "case_count" in sig_df.columns:
        fatal_count = int((sig_df["fatality_rate"] * sig_df["case_count"]).sum())

    fatality_rate = (fatal_count / total_rep) if total_rep > 0 else 0.0
    
    avg_age = None
    if not rep_df.empty and "age" in rep_df.columns:
        valid_a = pd.to_numeric(rep_df["age"], errors="coerce").dropna()
        valid_a = valid_a[(valid_a >= 0) & (valid_a <= 110)]
        if not valid_a.empty:
            avg_age = valid_a.mean()

    return total_rep, sig_count, fatality_rate, avg_age


tot_a, sigs_a, fat_a, age_a = compute_metrics(signals_a, reports_a)
tot_b, sigs_b, fat_b, age_b = compute_metrics(signals_b, reports_b)

st.markdown("<hr class='clinical-divider'>", unsafe_allow_html=True)

# Comparative KPI Cards
col_card_a, col_card_b = st.columns(2)

with col_card_a:
    st.markdown(f"#### Cohort A: {drug_a}")
    ca1, ca2, ca3 = st.columns(3)
    with ca1:
        st.metric("Total Submissions", f"{tot_a:,}")
    with ca2:
        st.metric("Confirmed Signals", f"{sigs_a:,}")
    with ca3:
        st.metric("Mortality Rate", f"{fat_a * 100:.1f}%")

with col_card_b:
    st.markdown(f"#### Cohort B: {drug_b}")
    cb1, cb2, cb3 = st.columns(3)
    with cb1:
        st.metric("Total Submissions", f"{tot_b:,}")
    with cb2:
        st.metric("Confirmed Signals", f"{sigs_b:,}")
    with cb3:
        st.metric("Mortality Rate", f"{fat_b * 100:.1f}%")

st.markdown("<hr class='clinical-divider'>", unsafe_allow_html=True)

# Shared Adverse Events Differential Table
st.markdown("### Differential Risk on Shared Adverse Reactions")

if not signals_a.empty and not signals_b.empty and "reaction" in signals_a.columns and "reaction" in signals_b.columns:
    sig_a = signals_a.copy()
    sig_b = signals_b.copy()
    sig_a["_reaction_key"] = sig_a["reaction"].astype(str).str.strip().str.upper()
    sig_b["_reaction_key"] = sig_b["reaction"].astype(str).str.strip().str.upper()
    
    shared_keys = set(sig_a["_reaction_key"]).intersection(set(sig_b["_reaction_key"]))
    
    if shared_keys:
        st.caption(f"Identified {len(shared_keys)} adverse reaction terms reported in association with both compounds.")
        
        merged = pd.merge(
            sig_a[["_reaction_key", "reaction", "prr", "case_count", "fatality_rate"]].rename(
                columns={"prr": "prr_a", "case_count": "cases_a", "fatality_rate": "fat_a"}
            ),
            sig_b[["_reaction_key", "prr", "case_count", "fatality_rate"]].rename(
                columns={"prr": "prr_b", "case_count": "cases_b", "fatality_rate": "fat_b"}
            ),
            on="_reaction_key",
        )
        
        merged["differential_ratio"] = (merged["prr_a"] / merged["prr_b"]).round(2)
        
        def assign_higher_risk(row):
            if row["differential_ratio"] >= 1.5:
                return f"Higher on {drug_a} ({row['differential_ratio']:.1f}x)"
            elif row["differential_ratio"] <= 0.67:
                ratio_b = round(1.0 / row["differential_ratio"], 1) if row["differential_ratio"] > 0 else 0.0
                return f"Higher on {drug_b} ({ratio_b:.1f}x)"
            return "Comparable Disproportionality"

        merged["risk_attribution"] = merged.apply(assign_higher_risk, axis=1)

        display_comp = merged[[
            "reaction", "prr_a", "prr_b", "differential_ratio", "cases_a", "cases_b", "risk_attribution"
        ]].sort_values(by="differential_ratio", ascending=False)

        st.dataframe(
            display_comp,
            column_config={
                "reaction": st.column_config.TextColumn("Shared Adverse Reaction", width="large"),
                "prr_a": st.column_config.NumberColumn(f"PRR ({drug_a})", format="%.2fx"),
                "prr_b": st.column_config.NumberColumn(f"PRR ({drug_b})", format="%.2fx"),
                "differential_ratio": st.column_config.NumberColumn("Differential (A / B)", format="%.2fx"),
                "cases_a": st.column_config.NumberColumn(f"Reports ({drug_a})", format="%d"),
                "cases_b": st.column_config.NumberColumn(f"Reports ({drug_b})", format="%d"),
                "risk_attribution": st.column_config.TextColumn("Disproportionality Finding", width="medium"),
            },
            hide_index=True,
            use_container_width=True,
        )
    else:
        st.info("No shared adverse reactions with confirmed statistical signals were identified between these two compounds.")
else:
    st.info("Insufficient signal records for one or both selected compounds.")

st.markdown("<hr class='clinical-divider'>", unsafe_allow_html=True)

# Overlaid Demographic Charts
st.markdown("### Comparative Patient Demographic Exposure")
if not reports_a.empty or not reports_b.empty:
    comb_reports = pd.concat([reports_a, reports_b], ignore_index=True)
    c_chart1, c_chart2 = st.columns(2)
    with c_chart1:
        st.plotly_chart(
            age_histogram(comb_reports, title="Comparative Age Distribution Overlay"),
            use_container_width=True,
        )
    with c_chart2:
        if "quarter" in comb_reports.columns:
            st.plotly_chart(
                trend_line(comb_reports, title="Longitudinal Reporting Trends Comparison"),
                use_container_width=True,
            )
