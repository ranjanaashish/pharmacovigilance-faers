"""Plotly Chart Visualizers for Pharmacovigilance FAERS Dashboard.

Provides clinical-grade Plotly visualizers adhering to healthcare design standards:
- Clear, publication-ready typography (Inter / sans-serif)
- Medical color taxonomy (Clinical Sapphire, Slate, Alert Crimson, Warning Amber)
- High-contrast responsive grids and zero emojis
"""

from typing import Optional, List, Union
import pandas as pd
import plotly.graph_objects as go
import plotly.express as px

# Healthcare Corporate Color Palette
CLINICAL_BLUE = "#0284C7"       # Primary medical blue
CLINICAL_DARK = "#0F172A"       # Deep slate navy
CLINICAL_TEAL = "#0D9488"       # Secondary clinical teal
ALERT_CRIMSON = "#DC2626"       # Critical risk / mortality
WARNING_AMBER = "#D97706"       # Moderate signal elevation
MUTED_SLATE = "#64748B"         # Secondary descriptive text
SURFACE_WHITE = "#FFFFFF"       # Card surface
GRID_SUBTLE = "#F1F5F9"         # Clean divider grid
BORDER_COLOR = "#E2E8F0"        # Card border

PALETTE_DRUGS = ["#0284C7", "#0D9488", "#D97706", "#DC2626", "#7C3AED", "#475569"]
GENDER_PALETTE = {
    "Female": "#0284C7",
    "Male": "#0F172A",
    "Unknown / Not Reported": "#94A3B8",
    "Other": "#CBD5E1",
}


def _apply_clinical_layout(fig: go.Figure, title: str, height: int = 340) -> go.Figure:
    """Standardize layout to institutional clinical specifications."""
    fig.update_layout(
        title=dict(
            text=f"<b>{title.upper()}</b>",
            font=dict(size=12, color=CLINICAL_DARK, family="Inter, sans-serif"),
            x=0.01,
            y=0.96,
        ),
        paper_bgcolor=SURFACE_WHITE,
        plot_bgcolor=SURFACE_WHITE,
        font=dict(family="Inter, -apple-system, sans-serif", size=11, color=CLINICAL_DARK),
        margin=dict(l=40, r=20, t=50, b=40),
        height=height,
        hoverlabel=dict(
            bgcolor=CLINICAL_DARK,
            font_size=12,
            font_family="Inter, sans-serif",
            font_color="#FFFFFF",
        ),
        legend=dict(
            orientation="h",
            yanchor="bottom",
            y=1.02,
            xanchor="right",
            x=1,
            font=dict(size=11, color=MUTED_SLATE),
        ),
    )
    fig.update_xaxes(
        showgrid=True,
        gridcolor=GRID_SUBTLE,
        zeroline=False,
        linecolor=BORDER_COLOR,
        tickfont=dict(size=10, color=MUTED_SLATE),
    )
    fig.update_yaxes(
        showgrid=True,
        gridcolor=GRID_SUBTLE,
        zeroline=False,
        linecolor=BORDER_COLOR,
        tickfont=dict(size=10, color=MUTED_SLATE),
    )
    return fig


def _empty_figure(message: str = "No clinical data available") -> go.Figure:
    """Return an empty figure placeholder."""
    fig = go.Figure()
    fig.add_annotation(
        text=message,
        xref="paper",
        yref="paper",
        x=0.5,
        y=0.5,
        showarrow=False,
        font=dict(size=12, color=MUTED_SLATE, family="Inter, sans-serif"),
    )
    fig.update_layout(
        paper_bgcolor=SURFACE_WHITE,
        plot_bgcolor=SURFACE_WHITE,
        xaxis=dict(visible=False),
        yaxis=dict(visible=False),
        height=320,
        margin=dict(l=20, r=20, t=20, b=20),
    )
    return fig


def age_histogram(
    df: pd.DataFrame,
    title: str = "Patient Age Distribution",
    age_col: Optional[str] = None,
    drug_col: Optional[str] = None,
) -> go.Figure:
    """Generate clinical age distribution histogram."""
    if df is None or df.empty:
        return _empty_figure("No age demographic records available")

    possible_age_cols = ["age", "patientonsetage", "patient_age", "onset_age"]
    target_age = age_col or next((c for c in possible_age_cols if c in df.columns), None)
    if not target_age:
        return _empty_figure("Age column not found in dataset")

    data = df.copy()
    data[target_age] = pd.to_numeric(data[target_age], errors="coerce")
    data = data.dropna(subset=[target_age])
    data = data[(data[target_age] >= 0) & (data[target_age] <= 110)]

    if data.empty:
        return _empty_figure("No valid age entries between 0 and 110")

    possible_drug_cols = ["drug_name", "medicinalproduct", "drug"]
    target_drug = drug_col or next((c for c in possible_drug_cols if c in df.columns), None)

    fig = go.Figure()

    if target_drug and data[target_drug].nunique() > 1:
        drugs = data[target_drug].unique()[:4]
        for i, drug in enumerate(drugs):
            subset = data[data[target_drug] == drug]
            fig.add_trace(
                go.Histogram(
                    x=subset[target_age],
                    name=str(drug).upper(),
                    nbinsx=24,
                    opacity=0.65,
                    marker=dict(color=PALETTE_DRUGS[i % len(PALETTE_DRUGS)]),
                    hovertemplate=f"<b>{drug}</b><br>Age: %{{x}}<br>Count: %{{y}}<extra></extra>",
                )
            )
        fig.update_layout(barmode="overlay")
    else:
        fig.add_trace(
            go.Histogram(
                x=data[target_age],
                nbinsx=28,
                marker=dict(
                    color=CLINICAL_BLUE,
                    line=dict(color="#FFFFFF", width=1),
                ),
                hovertemplate="Age Bracket: %{x}<br>Patient Reports: %{y:,}<extra></extra>",
            )
        )
        mean_age = data[target_age].mean()
        fig.add_vline(
            x=mean_age,
            line_dash="dash",
            line_color=ALERT_CRIMSON,
            line_width=1.5,
            annotation_text=f"Mean: {mean_age:.1f}y",
            annotation_position="top right",
            annotation_font=dict(size=10, color=ALERT_CRIMSON),
        )

    fig.update_xaxes(title_text="Age in Years")
    fig.update_yaxes(title_text="Case Count")
    return _apply_clinical_layout(fig, title)


def gender_pie(
    df: pd.DataFrame,
    title: str = "Patient Sex Distribution",
    gender_col: Optional[str] = None,
) -> go.Figure:
    """Generate clinical gender breakdown donut chart."""
    if df is None or df.empty:
        return _empty_figure("No gender records available")

    possible_gender_cols = ["gender", "patientsex", "sex"]
    target_col = gender_col or next((c for c in possible_gender_cols if c in df.columns), None)
    if not target_col:
        return _empty_figure("Gender column not found")

    counts = df[target_col].value_counts().reset_index()
    counts.columns = ["gender", "count"]

    color_map = [GENDER_PALETTE.get(str(g), "#94A3B8") for g in counts["gender"]]

    fig = go.Figure(
        data=[
            go.Pie(
                labels=counts["gender"],
                values=counts["count"],
                hole=0.6,
                marker=dict(colors=color_map, line=dict(color="#FFFFFF", width=2)),
                textinfo="label+percent",
                textfont=dict(size=11, family="Inter, sans-serif"),
                hovertemplate="<b>%{label}</b><br>Reports: %{value:,}<br>Proportion: %{percent}<extra></extra>",
            )
        ]
    )
    return _apply_clinical_layout(fig, title, height=320)


def trend_line(
    df: pd.DataFrame,
    title: str = "Longitudinal Reporting Volume",
    time_col: Optional[str] = None,
    drug_col: Optional[str] = None,
) -> go.Figure:
    """Generate chronological reporting trend line chart."""
    if df is None or df.empty:
        return _empty_figure("No longitudinal reporting records available")

    possible_time_cols = ["quarter", "receivedate", "report_date", "year_quarter"]
    target_time = time_col or next((c for c in possible_time_cols if c in df.columns), None)
    if not target_time:
        return _empty_figure("Time identifier not found")

    data = df.copy()
    data = data.dropna(subset=[target_time])
    data[target_time] = data[target_time].astype(str)

    possible_drug_cols = ["drug_name", "medicinalproduct"]
    target_drug = drug_col or next((c for c in possible_drug_cols if c in df.columns), None)

    fig = go.Figure()

    if target_drug and data[target_drug].nunique() > 1:
        drugs = data[target_drug].unique()[:4]
        for i, drug in enumerate(drugs):
            sub = data[data[target_drug] == drug].groupby(target_time).size().reset_index(name="count")
            sub = sub.sort_values(by=target_time)
            fig.add_trace(
                go.Scatter(
                    x=sub[target_time],
                    y=sub["count"],
                    mode="lines+markers",
                    name=str(drug).upper(),
                    line=dict(color=PALETTE_DRUGS[i % len(PALETTE_DRUGS)], width=2),
                    marker=dict(size=5),
                    hovertemplate=f"<b>{drug}</b><br>Period: %{{x}}<br>Volume: %{{y:,}}<extra></extra>",
                )
            )
    else:
        grouped = data.groupby(target_time).size().reset_index(name="count")
        grouped = grouped.sort_values(by=target_time)
        fig.add_trace(
            go.Scatter(
                x=grouped[target_time],
                y=grouped["count"],
                mode="lines+markers",
                name="Total Reports",
                line=dict(color=CLINICAL_BLUE, width=2.5),
                marker=dict(size=6, color=CLINICAL_BLUE),
                fill="tozeroy",
                fillcolor="rgba(2, 132, 199, 0.08)",
                hovertemplate="Period: %{x}<br>Submissions: %{y:,}<extra></extra>",
            )
        )

    fig.update_xaxes(title_text="Surveillance Reporting Period")
    fig.update_yaxes(title_text="Case Reports")
    return _apply_clinical_layout(fig, title)


def signal_bar(
    df: pd.DataFrame,
    x_col: str,
    y_col: str,
    title: str = "Disproportionality Ranking",
    orientation: str = "h",
    color_col: Optional[str] = None,
    limit: int = 15,
) -> go.Figure:
    """Generate clean clinical horizontal ranking bar chart."""
    if df is None or df.empty or x_col not in df.columns or y_col not in df.columns:
        return _empty_figure("No data for ranking matrix")

    data = df.copy()
    if orientation == "h":
        data = data.sort_values(by=x_col, ascending=True).tail(limit)
    else:
        data = data.sort_values(by=y_col, ascending=False).head(limit)

    fig = go.Figure(
        data=[
            go.Bar(
                x=data[x_col],
                y=data[y_col],
                orientation=orientation,
                marker=dict(
                    color=CLINICAL_BLUE,
                    line=dict(color="#FFFFFF", width=1),
                ),
                hovertemplate="<b>%{y}</b><br>Score: %{x:.2f}<extra></extra>",
            )
        ]
    )

    if orientation == "h":
        fig.update_xaxes(title_text="Statistical Metric Value")
        fig.update_yaxes(title_text="")
    else:
        fig.update_xaxes(title_text="")
        fig.update_yaxes(title_text="Statistical Metric Value")

    return _apply_clinical_layout(fig, title, height=400)


def fatality_heatmap(
    df: pd.DataFrame,
    title: str = "Exposure Across Patient Age Brackets",
    drug_col: str = "drug_name",
    age_col: str = "age_group",
) -> go.Figure:
    """Generate annotated demographic cross-tabulation matrix."""
    if df is None or df.empty or drug_col not in df.columns or age_col not in df.columns:
        return _empty_figure("Insufficient data for demographic cross-matrix")

    top_drugs = df[drug_col].value_counts().head(10).index
    filtered = df[df[drug_col].isin(top_drugs)]

    ct = pd.crosstab(filtered[drug_col], filtered[age_col])
    if ct.empty:
        return _empty_figure("Matrix computation yielded no values")

    fig = px.imshow(
        ct,
        labels=dict(x="Age Cohort", y="Active Substance", color="Reports"),
        color_continuous_scale=[[0, "#F8FAFC"], [0.2, "#BAE6FD"], [0.6, "#0284C7"], [1, "#0F172A"]],
        text_auto=True,
        aspect="auto",
    )
    fig.update_traces(textfont=dict(size=10, family="Inter, sans-serif"))
    return _apply_clinical_layout(fig, title, height=420)
