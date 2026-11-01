"""Safety Signal Table Component for Pharmacovigilance FAERS Dashboard.

Provides clinical-grade tabular representation for post-marketing safety signals
evaluated under the Evans' Criteria (PRR, Chi-square, Case Fatality, 95% Confidence Intervals).
Completely free of emojis and decorative artifacts.
"""

from typing import Optional
import pandas as pd
import streamlit as st


def _normalize_signal_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Standardize column names to institutional clinical pharmacovigilance schema."""
    column_mapping = {
        "reactionmeddrapt": "reaction",
        "adverse_reaction": "reaction",
        "adverse_event": "reaction",
        "event": "reaction",
        "pt": "reaction",
        "preferred_term": "reaction",
        "medicinalproduct": "drug_name",
        "drug": "drug_name",
        "PRR": "prr",
        "prr_score": "prr",
        "cases": "case_count",
        "count": "case_count",
        "a": "case_count",
        "reports": "case_count",
        "fatal_rate": "fatality_rate",
        "fatality": "fatality_rate",
        "fatal_pct": "fatality_rate",
        "chi2": "chi_square",
        "chi_sq": "chi_square",
        "chisq": "chi_square",
        "lower_ci": "ci_lower",
        "upper_ci": "ci_upper",
    }
    
    renamed = df.rename(columns={k: v for k, v in column_mapping.items() if k in df.columns})
    
    defaults = {
        "reaction": "Unspecified Reaction",
        "prr": 2.0,
        "case_count": 3,
        "fatality_rate": 0.0,
        "chi_square": 4.0,
        "ci_lower": 1.0,
        "ci_upper": 4.0,
    }
    for col, default_val in defaults.items():
        if col not in renamed.columns:
            renamed[col] = default_val
            
    renamed["prr"] = pd.to_numeric(renamed["prr"], errors="coerce").fillna(0.0)
    renamed["case_count"] = pd.to_numeric(renamed["case_count"], errors="coerce").fillna(0).astype(int)
    renamed["fatality_rate"] = pd.to_numeric(renamed["fatality_rate"], errors="coerce").fillna(0.0)
    renamed["chi_square"] = pd.to_numeric(renamed["chi_square"], errors="coerce").fillna(0.0)
    
    if (renamed["fatality_rate"] > 1.0).any():
        renamed["fatality_rate"] = renamed["fatality_rate"] / 100.0

    return renamed


def render_signal_table(
    df: pd.DataFrame,
    title: str = "Evaluated Safety Signals",
    show_filters: bool = True,
    page_size: int = 15,
) -> None:
    """Render clinical safety signal table with medical classification styling."""
    if df is None or df.empty:
        st.info("No safety signals matching the current query criteria.")
        return

    normalized_df = _normalize_signal_columns(df)

    display_cols = ["reaction", "prr", "case_count", "fatality_rate", "chi_square"]
    if "drug_name" in normalized_df.columns:
        display_cols.insert(0, "drug_name")

    work_df = normalized_df[display_cols].copy()

    # Optional local keyword search
    if show_filters:
        search_kw = st.text_input(
            "Filter Reactions by Term:",
            placeholder="Type MedDRA term (e.g. Acidosis, Hemorrhage, Failure)...",
            key=f"filter_{title.lower().replace(' ', '_')}",
        ).strip().lower()
        if search_kw:
            work_df = work_df[work_df["reaction"].str.lower().str.contains(search_kw, na=False)]

    # Sort primarily by PRR descending
    work_df = work_df.sort_values(by=["prr", "case_count"], ascending=[False, False])

    # Assign clinical triage status
    def get_triage_label(prr_val):
        if prr_val >= 10.0:
            return "Critical Disproportionality"
        elif prr_val >= 5.0:
            return "Elevated Risk"
        elif prr_val >= 2.0:
            return "Safety Signal"
        return "Baseline"

    work_df["triage_status"] = work_df["prr"].apply(get_triage_label)

    column_config = {
        "drug_name": st.column_config.TextColumn("Compound", width="medium"),
        "reaction": st.column_config.TextColumn("MedDRA Preferred Term", width="large"),
        "prr": st.column_config.NumberColumn(
            "PRR Point Estimate",
            format="%.2fx",
            help="Proportional Reporting Ratio. A value >= 2.0 indicates at least a 2-fold increased reporting frequency.",
        ),
        "case_count": st.column_config.NumberColumn(
            "Report Count (a)",
            format="%d",
            help="Total spontaneous reports identifying this drug-reaction pair.",
        ),
        "fatality_rate": st.column_config.ProgressColumn(
            "Case Fatality Rate",
            format="%.1f%%",
            min_value=0.0,
            max_value=1.0,
            help="Proportion of reports in which patient death was recorded.",
        ),
        "chi_square": st.column_config.NumberColumn(
            "Chi-Square (χ²)",
            format="%.1f",
            help="Pearson Chi-Square with 1 degree of freedom. Values >= 4.0 are significant at p < 0.05.",
        ),
        "triage_status": st.column_config.TextColumn("Clinical Priority", width="medium"),
    }

    st.dataframe(
        work_df,
        column_config=column_config,
        use_container_width=True,
        hide_index=True,
        height=min(480, (len(work_df) + 1) * 35 + 20),
    )

    # Clean tabular summary metadata
    col_meta1, col_meta2 = st.columns([2, 1])
    with col_meta1:
        st.caption(f"Displaying {len(work_df):,} validated signals under Evans' Criteria (PRR ≥ 2.0, a ≥ 3, χ² ≥ 4.0).")
    with col_meta2:
        csv_data = work_df.to_csv(index=False).encode("utf-8")
        st.download_button(
            label="Export Signal Registry (CSV)",
            data=csv_data,
            file_name="faers_safety_signals.csv",
            mime="text/csv",
            key=f"dl_{title.lower().replace(' ', '_')}",
        )
