"""Demographic Analysis Page for Pharmacovigilance FAERS Dashboard.

Analyzes reporting representation across age cohorts (Pediatric, Adult, Elderly)
and gender to detect potential real-world evidence surveillance blind spots.
Institutional clinical styling with zero emojis.
"""

import sys
from pathlib import Path
import streamlit as st
import pandas as pd

dashboard_dir = Path(__file__).resolve().parent.parent
if str(dashboard_dir) not in sys.path:
    sys.path.append(str(dashboard_dir))

from data_loader import load_reports_data, get_available_drugs
from components.charts import age_histogram, gender_pie, fatality_heatmap
from components.theme import apply_healthcare_theme, render_top_nav, render_header


st.set_page_config(
    page_title="Population Disparity Audit | FAERS",
    layout="wide",
)

apply_healthcare_theme()
render_top_nav(active_module="Population Disparity Audit")

render_header(
    title="Vulnerable Population Representation & Surveillance Disparity Audit",
    subtitle="Audit demographic reporting equity across pediatric, adult, and geriatric patient populations to detect post-marketing surveillance blind spots.",
)

reports_df, _ = load_reports_data()
available_drugs = get_available_drugs()

if reports_df is None or reports_df.empty:
    st.warning("No demographic records found in data/processed/. Verify ETL pipeline output.")
    st.stop()

# Cohort Selection
options = ["ALL MONITORED SUBSTANCES (AGGREGATE)"] + available_drugs
selected_option = st.selectbox(
    "Select Target Pharmaceutical Cohort:",
    options=options,
    index=0,
    help="Select a specific compound to audit demographic exposure or evaluate the entire registry corpus",
)

is_aggregate = selected_option == "ALL MONITORED SUBSTANCES (AGGREGATE)"
if is_aggregate:
    cohort_df = reports_df.copy()
    cohort_label = "All Monitored Substances"
else:
    cohort_df = reports_df[reports_df["drug_name"].str.upper() == selected_option].copy()
    cohort_label = selected_option

# Ensure Age Group Column
def assign_age_group(age):
    try:
        val = float(age)
        if pd.isna(val) or val < 0 or val > 120:
            return "Unknown"
        if val < 18:
            return "Pediatric (0-17)"
        elif val < 65:
            return "Adult (18-64)"
        else:
            return "Elderly (65+)"
    except Exception:
        return "Unknown"

if "age_group" not in cohort_df.columns and "age" in cohort_df.columns:
    cohort_df["age_group"] = cohort_df["age"].apply(assign_age_group)

# Age Cohort Breakdown
total_cohort_records = len(cohort_df)
group_counts = cohort_df["age_group"].value_counts() if "age_group" in cohort_df.columns else pd.Series()

ped_count = group_counts.get("Pediatric (0-17)", 0)
adult_count = group_counts.get("Adult (18-64)", 0)
eld_count = group_counts.get("Elderly (65+)", group_counts.get("Geriatric (65+)", 0))
unsp_count = group_counts.get("Unknown", group_counts.get("Unspecified Age", group_counts.get("Unknown / Unspecified", 0)))

ped_pct = (ped_count / total_cohort_records * 100) if total_cohort_records > 0 else 0
adult_pct = (adult_count / total_cohort_records * 100) if total_cohort_records > 0 else 0
eld_pct = (eld_count / total_cohort_records * 100) if total_cohort_records > 0 else 0
unsp_pct = (unsp_count / total_cohort_records * 100) if total_cohort_records > 0 else 0

# Metric Cards
k1, k2, k3, k4 = st.columns(4)
with k1:
    st.metric("Pediatric Cohort (0-17)", f"{ped_count:,}", f"{ped_pct:.1f}% Representation")
with k2:
    st.metric("Adult Cohort (18-64)", f"{adult_count:,}", f"{adult_pct:.1f}% Representation")
with k3:
    st.metric("Geriatric Cohort (65+)", f"{eld_count:,}", f"{eld_pct:.1f}% Representation")
with k4:
    st.metric("Unspecified Age Records", f"{unsp_count:,}", f"{unsp_pct:.1f}% Incomplete")

st.markdown("<hr class='clinical-divider'>", unsafe_allow_html=True)

# Automated Surveillance Disparity Alerts
disparities = []
if ped_pct < 5.0 and total_cohort_records > 100:
    disparities.append(f"Pediatric population exposure is under-represented ({ped_pct:.1f}% of cohort reports, threshold: 5.0%).")
if eld_pct < 5.0 and total_cohort_records > 100:
    disparities.append(f"Geriatric population exposure is under-represented ({eld_pct:.1f}% of cohort reports, threshold: 5.0%).")

if disparities:
    st.warning("Surveillance Disparity Notice: " + " ".join(disparities))
else:
    st.success("Surveillance Equity Status: Balanced demographic representation across evaluated patient age brackets.")

st.markdown("<hr class='clinical-divider'>", unsafe_allow_html=True)

# Visual Stratification
st.markdown(f"### Demographic Distribution: {cohort_label}")
col_vis1, col_vis2 = st.columns(2)

with col_vis1:
    st.plotly_chart(
        age_histogram(cohort_df, title=f"Age Distribution ({cohort_label})"),
        use_container_width=True,
    )

with col_vis2:
    st.plotly_chart(
        gender_pie(cohort_df, title=f"Patient Sex Breakdown ({cohort_label})"),
        use_container_width=True,
    )

st.markdown("<hr class='clinical-divider'>", unsafe_allow_html=True)

# Multi-compound Age Matrix Heatmap
st.markdown("### Cross-Substance Demographic Exposure Matrix")
st.caption("Distribution of spontaneous adverse event reports across age cohorts for the top 10 most reported compounds.")
st.plotly_chart(
    fatality_heatmap(reports_df, title="Top Substances by Age Cohort Stratification"),
    use_container_width=True,
)
