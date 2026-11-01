"""Institutional Healthcare Dashboard Design System & Theme Engine.

Implements clinical-grade UI styling inspired by modern healthcare platforms (Figma/Cerner/Epic):
- Deep clinical slate/navy headers (#0F172A, #1E293B)
- Clean clinical card layouts with subtle borders (#E2E8F0) and elevation
- High-contrast clinical typography (Inter, -apple-system, sans-serif)
- Medical risk indicator badges (Critical, Alert, Nominal)
- Total removal of emojis and non-professional decorative artifacts
"""

import streamlit as st


def apply_healthcare_theme():
    """Inject institutional healthcare stylesheet into the Streamlit DOM."""
    st.markdown(
        """
        <style>
        @import url('https://fonts.googleapis.com/css2?family=Inter:wght@300;400;500;600;700&display=swap');

        /* Global typography & background */
        html, body, [class*="css"] {
            font-family: 'Inter', -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, sans-serif;
            color: #0F172A;
            background-color: #F8FAFC;
        }

        /* App container max width & padding */
        .block-container {
            padding-top: 1.75rem;
            padding-bottom: 3rem;
            padding-left: 2rem;
            padding-right: 2rem;
            max-width: 1400px;
        }

        /* Top Institutional Banner */
        .clinical-top-bar {
            display: flex;
            align-items: center;
            justify-content: space-between;
            padding: 0.75rem 1.25rem;
            background: #FFFFFF;
            border: 1px solid #E2E8F0;
            border-radius: 8px;
            margin-bottom: 1.5rem;
            box-shadow: 0 1px 2px rgba(15, 23, 42, 0.04);
        }
        .clinical-top-bar-left {
            display: flex;
            align-items: center;
            gap: 1rem;
        }
        .clinical-logo-badge {
            background: #0284C7;
            color: #FFFFFF;
            font-size: 11px;
            font-weight: 700;
            letter-spacing: 0.08em;
            text-transform: uppercase;
            padding: 4px 8px;
            border-radius: 4px;
        }
        .clinical-portal-title {
            font-size: 14px;
            font-weight: 600;
            color: #1E293B;
            letter-spacing: -0.01em;
        }
        .clinical-system-status {
            display: flex;
            align-items: center;
            gap: 0.5rem;
            font-size: 12px;
            color: #059669;
            font-weight: 600;
        }
        .status-dot-active {
            width: 8px;
            height: 8px;
            background-color: #10B981;
            border-radius: 50%;
            display: inline-block;
        }

        /* Header typography */
        .clinical-header {
            margin-bottom: 1.25rem;
        }
        .clinical-header h1 {
            font-size: 24px;
            font-weight: 700;
            color: #0F172A;
            letter-spacing: -0.02em;
            margin: 0;
            padding: 0;
        }
        .clinical-header p {
            font-size: 13px;
            color: #64748B;
            margin-top: 4px;
            margin-bottom: 0;
        }

        /* Clinical KPI Cards */
        .metric-card-container {
            background: #FFFFFF;
            border: 1px solid #E2E8F0;
            border-radius: 8px;
            padding: 1.15rem 1.25rem;
            box-shadow: 0 1px 3px rgba(15, 23, 42, 0.04);
            margin-bottom: 1rem;
            height: 100%;
        }
        .metric-card-label {
            font-size: 11px;
            font-weight: 600;
            text-transform: uppercase;
            letter-spacing: 0.05em;
            color: #64748B;
            margin-bottom: 0.35rem;
        }
        .metric-card-value {
            font-size: 26px;
            font-weight: 700;
            color: #0F172A;
            line-height: 1.2;
            letter-spacing: -0.02em;
        }
        .metric-card-subtext {
            font-size: 11px;
            color: #64748B;
            margin-top: 0.4rem;
            display: flex;
            align-items: center;
            gap: 0.35rem;
        }
        .badge-positive {
            color: #059669;
            background: #ECFDF5;
            padding: 2px 6px;
            border-radius: 4px;
            font-weight: 600;
        }
        .badge-critical {
            color: #DC2626;
            background: #FEF2F2;
            padding: 2px 6px;
            border-radius: 4px;
            font-weight: 600;
        }
        .badge-warning {
            color: #D97706;
            background: #FFFBEB;
            padding: 2px 6px;
            border-radius: 4px;
            font-weight: 600;
        }
        .badge-neutral {
            color: #0284C7;
            background: #F0F9FF;
            padding: 2px 6px;
            border-radius: 4px;
            font-weight: 600;
        }

        /* Section divider */
        .clinical-divider {
            height: 1px;
            background: #E2E8F0;
            margin: 1.5rem 0;
            border: none;
        }

        /* Streamlit metric element overrides */
        [data-testid="stMetric"] {
            background-color: #FFFFFF;
            border: 1px solid #E2E8F0;
            border-radius: 8px;
            padding: 1rem 1.25rem;
            box-shadow: 0 1px 3px rgba(15, 23, 42, 0.04);
        }
        [data-testid="stMetricLabel"] {
            font-size: 11px !important;
            font-weight: 600 !important;
            text-transform: uppercase !important;
            letter-spacing: 0.05em !important;
            color: #64748B !important;
        }
        [data-testid="stMetricValue"] {
            font-size: 26px !important;
            font-weight: 700 !important;
            color: #0F172A !important;
            letter-spacing: -0.02em !important;
        }

        /* Sidebar styling */
        [data-testid="stSidebar"] {
            background-color: #0F172A;
            border-right: 1px solid #1E293B;
        }
        [data-testid="stSidebar"] [class*="css"] {
            color: #F1F5F9;
        }
        [data-testid="stSidebar"] .stMarkdown p, 
        [data-testid="stSidebar"] .stMarkdown span {
            color: #94A3B8;
            font-size: 12px;
        }
        [data-testid="stSidebar"] h1, 
        [data-testid="stSidebar"] h2, 
        [data-testid="stSidebar"] h3 {
            color: #F8FAFC !important;
            font-size: 13px !important;
            font-weight: 700 !important;
            text-transform: uppercase !important;
            letter-spacing: 0.06em !important;
        }
        [data-testid="stSidebar"] [data-testid="stMetric"] {
            background-color: #1E293B !important;
            border: 1px solid #334155 !important;
        }
        [data-testid="stSidebar"] [data-testid="stMetricLabel"] {
            color: #94A3B8 !important;
        }
        [data-testid="stSidebar"] [data-testid="stMetricValue"] {
            color: #F8FAFC !important;
        }

        /* Expander & Tabs */
        .streamlit-expanderHeader {
            background-color: #FFFFFF !important;
            border: 1px solid #E2E8F0 !important;
            border-radius: 6px !important;
            font-weight: 600 !important;
            font-size: 13px !important;
            color: #1E293B !important;
        }
        .stTabs [data-baseweb="tab-list"] {
            gap: 1.5rem;
            border-bottom: 1px solid #E2E8F0;
        }
        .stTabs [data-baseweb="tab"] {
            font-size: 13px;
            font-weight: 600;
            color: #64748B;
            padding: 0.5rem 0.25rem;
        }
        .stTabs [aria-selected="true"] {
            color: #0284C7 !important;
            border-bottom: 2px solid #0284C7 !important;
        }

        /* Buttons */
        .stButton>button {
            border-radius: 6px;
            font-size: 13px;
            font-weight: 600;
            padding: 0.5rem 1rem;
            border: 1px solid #CBD5E1;
            background: #FFFFFF;
            color: #0F172A;
            transition: all 0.15s ease-in-out;
        }
        .stButton>button:hover {
            border-color: #0284C7;
            color: #0284C7;
            background: #F0F9FF;
        }

        /* Regulatory footnote */
        .clinical-footer {
            margin-top: 3rem;
            padding-top: 1rem;
            border-top: 1px solid #E2E8F0;
            font-size: 11px;
            color: #94A3B8;
            line-height: 1.5;
        }
        </style>
        """,
        unsafe_allow_html=True,
    )


def render_top_nav(active_module: str = "Surveillance System"):
    """Render institutional clinical top navigation banner."""
    st.markdown(
        f"""
        <div class="clinical-top-bar">
            <div class="clinical-top-bar-left">
                <span class="clinical-logo-badge">FDA FAERS</span>
                <span class="clinical-portal-title">Post-Marketing Pharmacovigilance & Disproportionality Intelligence</span>
                <span style="color: #CBD5E1;">|</span>
                <span style="font-size: 12px; color: #64748B; font-weight: 500;">Module: {active_module}</span>
            </div>
            <div class="clinical-system-status">
                <span class="status-dot-active"></span>
                <span>REGISTRY STREAM ONLINE</span>
            </div>
        </div>
        """,
        unsafe_allow_html=True,
    )


def render_header(title: str, subtitle: str):
    """Render clean clinical section header."""
    st.markdown(
        f"""
        <div class="clinical-header">
            <h1>{title}</h1>
            <p>{subtitle}</p>
        </div>
        """,
        unsafe_allow_html=True,
    )
