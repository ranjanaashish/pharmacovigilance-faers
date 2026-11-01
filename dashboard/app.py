"""Pharmacovigilance Big Data Analytics Dashboard.

Main executive landing page for the FAERS Adverse Drug Interaction Analyzer.
Architecture: Apache Spark Distributed Core + Apache Hive Lakehouse + MongoDB Document Store
Institutional UI design conforming to clinical health authority standards (zero emojis).
"""

import sys
from pathlib import Path

# Ensure dashboard directory and project root are on sys.path
_dashboard_dir = Path(__file__).resolve().parent
_project_root = _dashboard_dir.parent
if str(_dashboard_dir) not in sys.path:
    sys.path.insert(0, str(_dashboard_dir))
if str(_project_root) not in sys.path:
    sys.path.insert(0, str(_project_root))

import streamlit as st
import pandas as pd
from data_loader import load_signals_data, load_reports_data, get_available_drugs
from components.theme import apply_healthcare_theme, render_top_nav, render_header


st.set_page_config(
    page_title="Pharmacovigilance Intelligence Platform",
    layout="wide",
    initial_sidebar_state="expanded",
)


def render_sidebar(signals_df: pd.DataFrame, reports_df: pd.DataFrame, signals_src: str, reports_src: str):
    """Render clinical sidebar metadata, system status, and registry navigation."""
    with st.sidebar:
        st.markdown(
            """
            <div style="padding: 0.5rem 0 1rem 0;">
                <div style="font-size: 11px; font-weight: 700; letter-spacing: 0.1em; color: #38BDF8; text-transform: uppercase;">Surveillance System</div>
                <div style="font-size: 16px; font-weight: 700; color: #FFFFFF; letter-spacing: -0.01em;">FAERS Registry Analytics</div>
                <div style="font-size: 11px; color: #94A3B8; margin-top: 2px;">ED547 Big Data Core Engine</div>
            </div>
            """,
            unsafe_allow_html=True,
        )

        st.markdown("---")
        st.markdown("### SYSTEM TELEMETRY")

        total_reports = len(reports_df) if reports_df is not None else 0
        total_signals = len(signals_df) if signals_df is not None else 0
        unique_drugs = signals_df["drug_name"].nunique() if (signals_df is not None and "drug_name" in signals_df.columns) else 0

        st.metric(label="Ingested Patient Reports", value=f"{total_reports:,}")
        st.metric(label="Validated Signals (PRR >= 2)", value=f"{total_signals:,}")
        st.metric(label="Active Compounds Scored", value=f"{unique_drugs:,}")

        st.markdown("---")
        st.markdown("### DATA PIPELINE AUDIT")
        st.caption(f"Signal Store: {signals_src}")
        st.caption(f"Demographics Store: {reports_src}")

        st.markdown("---")
        st.markdown("### CLINICAL DIRECTORY")
        st.markdown(
            """
            - **01 Drug Search:** Substance-specific profile, PRR signals, demographics.
            - **02 Drug Compare:** Head-to-head comparative safety analysis.
            - **03 Top Signals:** Prioritized global disproportionality triage matrix.
            - **04 Demographics:** Vulnerable population exposure & under-reporting audit.
            """
        )

        st.markdown("---")
        st.caption("Post-Marketing Drug Safety Surveillance Engine | Distributed Systems Architecture")


def main():
    apply_healthcare_theme()

    signals_df, signals_src = load_signals_data()
    reports_df, reports_src = load_reports_data()

    render_sidebar(signals_df, reports_df, signals_src, reports_src)
    render_top_nav(active_module="Executive Overview")

    render_header(
        title="Post-Marketing Safety Surveillance & Disproportionality Intelligence",
        subtitle="Automated Early Detection of Adverse Drug Interactions via OpenFDA FAERS Distributed Big Data Pipeline",
    )

    if "Demo Mode" in signals_src:
        st.info(
            "Notice: Running in Standalone Demonstration Mode. A pre-compiled reference clinical cohort is loaded. "
            "To evaluate full batches, execute the PySpark batch pipeline to export parquet files."
        )

    # Top KPI Metrics Row
    col1, col2, col3, col4 = st.columns(4)

    total_reports_count = len(reports_df) if reports_df is not None else 0
    with col1:
        st.metric(
            label="Total Analyzed Submissions",
            value=f"{total_reports_count:,}",
            help="Total spontaneous adverse event submissions in the processed corpus",
        )

    critical_signals = 0
    if signals_df is not None and "prr" in signals_df.columns:
        critical_signals = int((signals_df["prr"] >= 10.0).sum())
    with col2:
        st.metric(
            label="Critical Signals (PRR >= 10.0)",
            value=f"{critical_signals:,}",
            delta="High Priority",
            delta_color="inverse",
            help="Signals exhibiting severe reporting disproportionality requiring immediate review",
        )

    high_fatality_signals = 0
    if signals_df is not None and "fatality_rate" in signals_df.columns:
        high_fatality_signals = int((signals_df["fatality_rate"] >= 0.10).sum())
    with col3:
        st.metric(
            label="Elevated Mortality (>10% Deaths)",
            value=f"{high_fatality_signals:,}",
            delta="Lethality Alert",
            delta_color="inverse",
            help="Adverse reactions with a case fatality rate exceeding 10 percent",
        )

    all_drugs = get_available_drugs()
    with col4:
        st.metric(
            label="Active Formulations",
            value=f"{len(all_drugs):,}",
            help="Unique active drug substances monitored in the pharmacovigilance registry",
        )

    st.markdown("<hr class='clinical-divider'>", unsafe_allow_html=True)

    # Methodological Section
    st.markdown("### Disproportionality Signal Methodology")
    col_theory, col_table = st.columns([3, 2])

    with col_theory:
        st.markdown(
            """
            In spontaneous reporting surveillance, **disproportionality analysis** quantifies whether a specific 
            adverse event is reported more frequently in association with a given drug relative to the background 
            rate across all other pharmaceutical compounds in the national database.

            **Proportional Reporting Ratio (PRR):**
            $$\\text{PRR} = \\frac{a / (a + b)}{c / (c + d)}$$

            **Evans' Standard Validation Criteria:**
            - **PRR >= 2.0**: Demonstrates at least two-fold increased relative reporting frequency.
            - **Observed Cases (a) >= 3**: Minimizes false-positive alerts resulting from low-count artifacts.
            - **Chi-Square (χ²) >= 4.0**: Statistically significant disproportionality ($p < 0.05$, $df=1$).
            """
        )

    with col_table:
        st.markdown("**2 × 2 Contingency Evaluation Matrix**")
        contingency_df = pd.DataFrame(
            [
                ["Target Substance", "a (Target Event Reports)", "b (Other Event Reports)", "a + b (Total Drug Submissions)"],
                ["Reference Database", "c (Target Event Reports)", "d (Other Event Reports)", "c + d (Background Submissions)"],
                ["Total Cohort", "a + c", "b + d", "N (Grand Total Records)"],
            ],
            columns=["Classification", "Index Adverse Reaction", "All Other Reactions", "Marginal Totals"],
        )
        st.dataframe(contingency_df, hide_index=True, use_container_width=True)

    st.markdown("<hr class='clinical-divider'>", unsafe_allow_html=True)

    # Big Data Architecture Flow
    st.markdown("### Big Data Pipeline Architecture")
    a1, a2, a3, a4 = st.columns(4)

    with a1:
        st.markdown(
            """
            <div class="metric-card-container">
                <div class="metric-card-label">Module IV · Ingestion</div>
                <div style="font-weight: 700; font-size: 14px; color: #0F172A; margin-bottom: 6px;">OpenFDA FAERS JSON</div>
                <div style="font-size: 12px; color: #64748B; line-height: 1.4;">
                    Automated ingestion of multi-gigabyte quarterly archives containing patient, drug, and reaction nested records.
                </div>
            </div>
            """,
            unsafe_allow_html=True,
        )

    with a2:
        st.markdown(
            """
            <div class="metric-card-container">
                <div class="metric-card-label">Module III · Distributed ETL</div>
                <div style="font-weight: 700; font-size: 14px; color: #0F172A; margin-bottom: 6px;">Apache Spark DAG</div>
                <div style="font-size: 12px; color: #64748B; line-height: 1.4;">
                    Distributed relational exploding, age/sex code harmonization, deduplication, and MapReduce 2x2 contingency matrix.
                </div>
            </div>
            """,
            unsafe_allow_html=True,
        )

    with a3:
        st.markdown(
            """
            <div class="metric-card-container">
                <div class="metric-card-label">Module II & V · Lakehouse</div>
                <div style="font-weight: 700; font-size: 14px; color: #0F172A; margin-bottom: 6px;">Hive & Parquet</div>
                <div style="font-size: 12px; color: #64748B; line-height: 1.4;">
                    Snappy-compressed columnar Parquet files coupled with Apache Hive analytical tables and MongoDB document lookups.
                </div>
            </div>
            """,
            unsafe_allow_html=True,
        )

    with a4:
        st.markdown(
            """
            <div class="metric-card-container">
                <div class="metric-card-label">Serving & Presentation</div>
                <div style="font-weight: 700; font-size: 14px; color: #0F172A; margin-bottom: 6px;">Clinical Intelligence UI</div>
                <div style="font-size: 12px; color: #64748B; line-height: 1.4;">
                    Responsive decision-support dashboard providing compound search, head-to-head profiling, and disparity analysis.
                </div>
            </div>
            """,
            unsafe_allow_html=True,
        )

    # Regulatory Disclaimer Footer
    st.markdown(
        """
        <div class="clinical-footer">
            <strong>Regulatory & Epidemiological Notice:</strong> Spontaneous adverse event reporting databases contain unverified 
            reports submitted by medical professionals, patients, and manufacturers. The presence of a safety signal or elevated PRR 
            establishes statistical disproportionality in reporting frequency and does not confirm biological causality. 
            Developed for academic demonstration and research in Big Data Analytics (ED547).
        </div>
        """,
        unsafe_allow_html=True,
    )


if __name__ == "__main__":
    main()
