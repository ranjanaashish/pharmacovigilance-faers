"""Pharmacovigilance Dashboard Components Package.

This package provides reusable UI components and chart visualizers
for the FAERS Big Data Analytics Streamlit application.
"""

from .signal_table import render_signal_table
from .charts import (
    age_histogram,
    gender_pie,
    trend_line,
    signal_bar,
    fatality_heatmap,
)

__all__ = [
    "render_signal_table",
    "age_histogram",
    "gender_pie",
    "trend_line",
    "signal_bar",
    "fatality_heatmap",
]
