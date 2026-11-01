"""Compound Safety Surveillance Page for Pharmacovigilance FAERS Dashboard.

Provides single-substance deep dive analysis, calculating Evans' criteria disproportionality,
age/gender demographics, and longitudinal quarterly reporting trends.
Institutional clinical styling with complete absence of emojis.
"""

import sys
from pathlib import Path
import difflib
import streamlit as st
import pandas as pd

dashboard_dir = Path(__file__).resolve().parent.parent
if str(dashboard_dir) not in sys.path:
    sys.path.append(str(dashboard_dir))

from data_loader import load_signals_data, load_reports_data, get_available_drugs
from components.signal_table import render_signal_table
from components.charts import age_histogram, gender_pie, trend_line
from components.theme import apply_healthcare_theme, render_top_nav, render_header


st.set_page_config(
    page_title="Compound Surveillance | FAERS Intelligence",
    layout="wide",
)

apply_healthcare_theme()
render_top_nav(active_module="Compound Surveillance")

render_header(
    title="Compound Safety Surveillance & Disproportionality Characterization",
    subtitle="Query any monitored pharmaceutical substance to review statistical safety signals, mortality rates, and demographic exposure.",
)

signals_df, _ = load_signals_data()
reports_df, _ = load_reports_data()
available_drugs = get_available_drugs()

# Compound Selection Controls
col_search, col_select = st.columns([1, 1])

with col_search:
    user_query = st.text_input(
        "Search Active Substance by Name:",
        placeholder="e.g. ATORVASTATIN, ASPIRIN, VIOXX, WARFARIN...",
        help="Type any active drug substance name (case-insensitive)",
    ).strip().upper()

with col_select:
    default_idx = 0
    if user_query and user_query in available_drugs:
        default_idx = available_drugs.index(user_query)
    
    selected_from_list = st.selectbox(
        "Or Select from Registered Substance Directory:",
        options=available_drugs,
        index=default_idx if available_drugs else 0,
        help="Select directly from all registered pharmaceutical ingredients",
    )

active_drug = user_query if user_query else selected_from_list

if not active_drug:
    st.info("Select or enter a pharmaceutical compound name above to initialize surveillance profiling.")
    st.stop()

# Validate existence
drug_in_signals = (
    signals_df is not None
    and "drug_name" in signals_df.columns
    and not signals_df[signals_df["drug_name"].str.upper() == active_drug].empty
)
drug_in_reports = (
    reports_df is not None
    and "drug_name" in reports_df.columns
    and not reports_df[reports_df["drug_name"].str.upper() == active_drug].empty
)

if not drug_in_signals and not drug_in_reports:
    st.error(f"Compound '{active_drug}' is not recorded in the currently processed surveillance registry.")
    close_matches = difflib.get_close_matches(active_drug, available_drugs, n=4, cutoff=0.4)
    if close_matches:
        st.write("Potential nomenclature matches in registry:")
        for match in close_matches:
            st.markdown(f"- **{match}**")
    st.stop()

# Substance Subset Extraction
drug_signals = pd.DataFrame()
if signals_df is not None and "drug_name" in signals_df.columns:
    drug_signals = signals_df[signals_df["drug_name"].str.upper() == active_drug].copy()

drug_reports = pd.DataFrame()
if reports_df is not None and "drug_name" in reports_df.columns:
    drug_reports = reports_df[reports_df["drug_name"].str.upper() == active_drug].copy()

# Summary Metrics Row
col_kpi1, col_kpi2, col_kpi3, col_kpi4 = st.columns(4)

total_cases = len(drug_reports) if not drug_reports.empty else (
    int(drug_signals["case_count"].sum()) if not drug_signals.empty and "case_count" in drug_signals.columns else 0
)
with col_kpi1:
    st.metric(
        label="Total Submissions",
        value=f"{total_cases:,}",
        help="Total spontaneous reports identifying this compound as suspect or concomitant",
    )

sig_count = len(drug_signals)
with col_kpi2:
    st.metric(
        label="Validated Safety Signals",
        value=f"{sig_count:,}",
        delta="PRR >= 2.0" if sig_count > 0 else "Nominal",
        help="Total adverse reactions meeting statistical disproportionality thresholds",
    )

fatal_cases = 0
if not drug_reports.empty and "fatal" in drug_reports.columns:
    fatal_cases = int(drug_reports["fatal"].sum())
elif not drug_signals.empty and "fatality_rate" in drug_signals.columns and "case_count" in drug_signals.columns:
    fatal_cases = int((drug_signals["fatality_rate"] * drug_signals["case_count"]).sum())

fatality_pct = (fatal_cases / total_cases * 100) if total_cases > 0 else 0.0
with col_kpi3:
    st.metric(
        label="Fatal Outcome Reports",
        value=f"{fatal_cases:,}",
        delta=f"{fatality_pct:.1f}% Mortality",
        delta_color="inverse",
        help="Submissions where patient death was recorded as clinical outcome",
    )

avg_age = None
if not drug_reports.empty and "age" in drug_reports.columns:
    clean_ages = pd.to_numeric(drug_reports["age"], errors="coerce").dropna()
    clean_ages = clean_ages[(clean_ages >= 0) & (clean_ages <= 110)]
    if not clean_ages.empty:
        avg_age = clean_ages.mean()

with col_kpi4:
    st.metric(
        label="Mean Patient Age",
        value=f"{avg_age:.1f} yrs" if avg_age is not None else "Not Reported",
        help="Mean age of patients reporting adverse reactions to this compound",
    )

st.markdown("<hr class='clinical-divider'>", unsafe_allow_html=True)

# Main Tabbed Analysis
tab_signals, tab_demographics, tab_trends = st.tabs([
    "Evaluated Safety Signals", 
    "Patient Demographics & Cohort Profile", 
    "Longitudinal Surveillance Trends"
])

with tab_signals:
    st.markdown(f"#### Statistically Flagged Adverse Reactions: {active_drug}")
    if not drug_signals.empty:
        render_signal_table(drug_signals, title=f"{active_drug} Signals")
    else:
        st.info(f"No adverse events exceeded the statistical disproportionality threshold (PRR >= 2.0) for {active_drug}.")

with tab_demographics:
    st.markdown(f"#### Demographic Stratification: {active_drug}")
    if not drug_reports.empty:
        col_demo_left, col_demo_right = st.columns(2)
        with col_demo_left:
            st.plotly_chart(
                age_histogram(drug_reports, title=f"Age Distribution ({active_drug})"),
                use_container_width=True,
            )
        with col_demo_right:
            st.plotly_chart(
                gender_pie(drug_reports, title=f"Sex Breakdown ({active_drug})"),
                use_container_width=True,
            )
    else:
        st.info("Demographic records are not available for this compound in the current partition subset.")

with tab_trends:
    st.markdown(f"#### Chronological Reporting Timeline: {active_drug}")
    if not drug_reports.empty and "quarter" in drug_reports.columns:
        st.plotly_chart(
            trend_line(drug_reports, title=f"Quarterly Submission Volume ({active_drug})"),
            use_container_width=True,
        )
    else:
        st.info("Time-series quarter identifiers are not present in the current subset.")
