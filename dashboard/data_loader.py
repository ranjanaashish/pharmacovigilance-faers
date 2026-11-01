"""Data Loading and Caching Module for Pharmacovigilance FAERS Dashboard.

This module implements a resilient tiered data loading strategy:
1. Primary: MongoDB (production analytics store populated by PySpark ETL)
2. Secondary: Local Parquet files from data/processed/ (standalone mode)
3. Fallback: High-fidelity synthetic FAERS pharmacovigilance dataset for instant demo
"""

import os
from pathlib import Path
from typing import Dict, List, Optional, Tuple
import numpy as np
import pandas as pd
import streamlit as st


# Configurable default paths
PROJECT_ROOT = Path(__file__).resolve().parent.parent
PROCESSED_DATA_DIR = PROJECT_ROOT / "data" / "processed"
SIGNALS_PARQUET_PATHS = [
    PROCESSED_DATA_DIR / "signals",
    PROCESSED_DATA_DIR / "signals.parquet",
    PROCESSED_DATA_DIR / "safety_signals",
    PROCESSED_DATA_DIR / "prr_signals.parquet",
]
REPORTS_PARQUET_PATHS = [
    PROCESSED_DATA_DIR / "reports",
    PROCESSED_DATA_DIR / "reports.parquet",
    PROCESSED_DATA_DIR / "demographics",
    PROCESSED_DATA_DIR / "cleaned_reports.parquet",
    PROCESSED_DATA_DIR / "adverse_events.parquet",
]


def _try_load_mongodb(collection_name: str) -> Optional[pd.DataFrame]:
    """Attempt to load data from local MongoDB instance."""
    try:
        from pymongo import MongoClient
        mongo_uri = os.getenv("MONGO_URI", "mongodb://localhost:27017")
        client = MongoClient(mongo_uri, serverSelectionTimeoutMS=1200)
        # Ping server to confirm active connection
        client.admin.command("ping")
        db = client["pharmacovigilance"]
        
        if collection_name in db.list_collection_names():
            cursor = db[collection_name].find({}, {"_id": 0})
            records = list(cursor)
            if records:
                return pd.DataFrame(records)
    except Exception:
        # MongoDB not available, silent fallback
        pass
    return None


def _try_load_parquet(candidate_paths: List[Path]) -> Optional[pd.DataFrame]:
    """Search candidate paths for Parquet files or directories."""
    for path in candidate_paths:
        try:
            if path.is_file() and path.suffix == ".parquet":
                df = pd.read_parquet(path)
                if not df.empty:
                    return df
            elif path.is_dir():
                # Directory of partitioned parquet files
                parquet_files = list(path.glob("*.parquet"))
                if parquet_files:
                    df = pd.read_parquet(path)
                    if not df.empty:
                        return df
        except Exception:
            continue
    return None


def _generate_synthetic_signals() -> pd.DataFrame:
    """Generate high-fidelity synthetic safety signals for demonstration."""
    signals_data = [
        # Atorvastatin
        {"drug_name": "ATORVASTATIN", "reaction": "Rhabdomyolysis", "prr": 14.82, "case_count": 1420, "fatality_rate": 0.074, "chi_square": 1120.4, "lower_ci": 12.1, "upper_ci": 18.2},
        {"drug_name": "ATORVASTATIN", "reaction": "Myopathy", "prr": 8.65, "case_count": 980, "fatality_rate": 0.012, "chi_square": 680.1, "lower_ci": 7.2, "upper_ci": 10.4},
        {"drug_name": "ATORVASTATIN", "reaction": "Acute Liver Injury", "prr": 3.45, "case_count": 420, "fatality_rate": 0.051, "chi_square": 210.3, "lower_ci": 2.8, "upper_ci": 4.2},
        {"drug_name": "ATORVASTATIN", "reaction": "Memory Impairment", "prr": 2.65, "case_count": 310, "fatality_rate": 0.003, "chi_square": 95.4, "lower_ci": 2.1, "upper_ci": 3.3},
        {"drug_name": "ATORVASTATIN", "reaction": "Tendon Rupture", "prr": 2.15, "case_count": 145, "fatality_rate": 0.000, "chi_square": 42.1, "lower_ci": 1.6, "upper_ci": 2.9},

        # Rosuvastatin
        {"drug_name": "ROSUVASTATIN", "reaction": "Rhabdomyolysis", "prr": 12.10, "case_count": 1150, "fatality_rate": 0.068, "chi_square": 890.5, "lower_ci": 10.2, "upper_ci": 14.5},
        {"drug_name": "ROSUVASTATIN", "reaction": "Proteinuria", "prr": 6.30, "case_count": 520, "fatality_rate": 0.018, "chi_square": 340.2, "lower_ci": 5.1, "upper_ci": 7.8},
        {"drug_name": "ROSUVASTATIN", "reaction": "Myalgia", "prr": 4.55, "case_count": 890, "fatality_rate": 0.002, "chi_square": 312.0, "lower_ci": 3.8, "upper_ci": 5.4},
        {"drug_name": "ROSUVASTATIN", "reaction": "Renal Failure Acute", "prr": 3.12, "case_count": 380, "fatality_rate": 0.092, "chi_square": 165.7, "lower_ci": 2.5, "upper_ci": 3.9},

        # Aspirin
        {"drug_name": "ASPIRIN", "reaction": "Gastrointestinal Hemorrhage", "prr": 5.85, "case_count": 3210, "fatality_rate": 0.048, "chi_square": 1450.2, "lower_ci": 5.1, "upper_ci": 6.7},
        {"drug_name": "ASPIRIN", "reaction": "Gastric Ulcer", "prr": 6.20, "case_count": 1890, "fatality_rate": 0.021, "chi_square": 920.4, "lower_ci": 5.4, "upper_ci": 7.1},
        {"drug_name": "ASPIRIN", "reaction": "Tinnitus", "prr": 4.90, "case_count": 640, "fatality_rate": 0.001, "chi_square": 280.6, "lower_ci": 3.9, "upper_ci": 6.1},
        {"drug_name": "ASPIRIN", "reaction": "Reye's Syndrome", "prr": 15.40, "case_count": 88, "fatality_rate": 0.182, "chi_square": 240.5, "lower_ci": 11.2, "upper_ci": 21.0},
        {"drug_name": "ASPIRIN", "reaction": "Hematoma", "prr": 3.20, "case_count": 850, "fatality_rate": 0.015, "chi_square": 190.2, "lower_ci": 2.7, "upper_ci": 3.8},

        # Ibuprofen
        {"drug_name": "IBUPROFEN", "reaction": "Gastrointestinal Hemorrhage", "prr": 4.15, "case_count": 2100, "fatality_rate": 0.035, "chi_square": 820.5, "lower_ci": 3.6, "upper_ci": 4.8},
        {"drug_name": "IBUPROFEN", "reaction": "Renal Failure Acute", "prr": 4.75, "case_count": 1420, "fatality_rate": 0.061, "chi_square": 610.8, "lower_ci": 4.0, "upper_ci": 5.6},
        {"drug_name": "IBUPROFEN", "reaction": "Nephrotic Syndrome", "prr": 3.60, "case_count": 310, "fatality_rate": 0.022, "chi_square": 140.1, "lower_ci": 2.8, "upper_ci": 4.6},
        {"drug_name": "IBUPROFEN", "reaction": "Aseptic Meningitis", "prr": 8.90, "case_count": 95, "fatality_rate": 0.010, "chi_square": 180.4, "lower_ci": 6.3, "upper_ci": 12.5},

        # Metformin
        {"drug_name": "METFORMIN", "reaction": "Lactic Acidosis", "prr": 19.40, "case_count": 2150, "fatality_rate": 0.235, "chi_square": 2680.1, "lower_ci": 16.8, "upper_ci": 22.4},
        {"drug_name": "METFORMIN", "reaction": "Vitamin B12 Deficiency", "prr": 5.10, "case_count": 680, "fatality_rate": 0.002, "chi_square": 320.0, "lower_ci": 4.2, "upper_ci": 6.2},
        {"drug_name": "METFORMIN", "reaction": "Pancreatitis Acute", "prr": 2.85, "case_count": 490, "fatality_rate": 0.042, "chi_square": 145.3, "lower_ci": 2.3, "upper_ci": 3.5},
        {"drug_name": "METFORMIN", "reaction": "Hypoglycemia", "prr": 3.40, "case_count": 1200, "fatality_rate": 0.018, "chi_square": 380.7, "lower_ci": 2.9, "upper_ci": 4.0},

        # Lisinopril
        {"drug_name": "LISINOPRIL", "reaction": "Angioedema", "prr": 17.50, "case_count": 2400, "fatality_rate": 0.054, "chi_square": 2450.0, "lower_ci": 15.2, "upper_ci": 20.1},
        {"drug_name": "LISINOPRIL", "reaction": "Cough Persistent", "prr": 13.20, "case_count": 3100, "fatality_rate": 0.001, "chi_square": 2100.5, "lower_ci": 11.8, "upper_ci": 14.8},
        {"drug_name": "LISINOPRIL", "reaction": "Hyperkalemia", "prr": 4.90, "case_count": 820, "fatality_rate": 0.068, "chi_square": 410.2, "lower_ci": 4.1, "upper_ci": 5.9},
        {"drug_name": "LISINOPRIL", "reaction": "Renal Failure Acute", "prr": 3.25, "case_count": 750, "fatality_rate": 0.081, "chi_square": 220.6, "lower_ci": 2.6, "upper_ci": 4.0},

        # Warfarin
        {"drug_name": "WARFARIN", "reaction": "Cerebral Hemorrhage", "prr": 12.80, "case_count": 1950, "fatality_rate": 0.310, "chi_square": 1890.3, "lower_ci": 11.0, "upper_ci": 14.9},
        {"drug_name": "WARFARIN", "reaction": "Gastrointestinal Hemorrhage", "prr": 7.45, "case_count": 3800, "fatality_rate": 0.085, "chi_square": 2210.0, "lower_ci": 6.7, "upper_ci": 8.3},
        {"drug_name": "WARFARIN", "reaction": "International Normalized Ratio Increased", "prr": 14.10, "case_count": 4200, "fatality_rate": 0.021, "chi_square": 3100.0, "lower_ci": 12.8, "upper_ci": 15.6},
        {"drug_name": "WARFARIN", "reaction": "Skin Necrosis", "prr": 18.20, "case_count": 140, "fatality_rate": 0.093, "chi_square": 420.5, "lower_ci": 13.5, "upper_ci": 24.5},

        # Pembrolizumab
        {"drug_name": "PEMBROLIZUMAB", "reaction": "Pneumonitis", "prr": 16.40, "case_count": 920, "fatality_rate": 0.152, "chi_square": 1380.0, "lower_ci": 14.1, "upper_ci": 19.1},
        {"drug_name": "PEMBROLIZUMAB", "reaction": "Colitis Autoimmune", "prr": 13.70, "case_count": 780, "fatality_rate": 0.064, "chi_square": 1120.4, "lower_ci": 11.5, "upper_ci": 16.3},
        {"drug_name": "PEMBROLIZUMAB", "reaction": "Hypophysitis", "prr": 21.50, "case_count": 310, "fatality_rate": 0.032, "chi_square": 890.1, "lower_ci": 17.2, "upper_ci": 26.9},
        {"drug_name": "PEMBROLIZUMAB", "reaction": "Myocarditis", "prr": 9.80, "case_count": 240, "fatality_rate": 0.285, "chi_square": 460.8, "lower_ci": 7.5, "upper_ci": 12.8},

        # Remdesivir
        {"drug_name": "REMDESIVIR", "reaction": "Hepatic Enzyme Increased", "prr": 5.90, "case_count": 640, "fatality_rate": 0.045, "chi_square": 310.2, "lower_ci": 4.8, "upper_ci": 7.2},
        {"drug_name": "REMDESIVIR", "reaction": "Bradycardia", "prr": 4.10, "case_count": 480, "fatality_rate": 0.062, "chi_square": 205.4, "lower_ci": 3.3, "upper_ci": 5.1},
        {"drug_name": "REMDESIVIR", "reaction": "Renal Failure Acute", "prr": 3.80, "case_count": 520, "fatality_rate": 0.115, "chi_square": 230.1, "lower_ci": 3.0, "upper_ci": 4.8},

        # Acetaminophen
        {"drug_name": "ACETAMINOPHEN", "reaction": "Acute Hepatic Failure", "prr": 26.50, "case_count": 2800, "fatality_rate": 0.334, "chi_square": 4500.0, "lower_ci": 23.4, "upper_ci": 30.1},
        {"drug_name": "ACETAMINOPHEN", "reaction": "Hepatotoxicity", "prr": 18.10, "case_count": 3400, "fatality_rate": 0.180, "chi_square": 3890.2, "lower_ci": 16.2, "upper_ci": 20.2},
        {"drug_name": "ACETAMINOPHEN", "reaction": "Jaundice", "prr": 6.70, "case_count": 1100, "fatality_rate": 0.092, "chi_square": 780.0, "lower_ci": 5.6, "upper_ci": 8.0},

        # Amiodarone
        {"drug_name": "AMIODARONE", "reaction": "Pulmonary Fibrosis", "prr": 22.80, "case_count": 890, "fatality_rate": 0.210, "chi_square": 1840.6, "lower_ci": 19.1, "upper_ci": 27.2},
        {"drug_name": "AMIODARONE", "reaction": "Thyroid Dysfunction", "prr": 9.40, "case_count": 670, "fatality_rate": 0.008, "chi_square": 680.2, "lower_ci": 7.8, "upper_ci": 11.3},
        {"drug_name": "AMIODARONE", "reaction": "Corneal Microdeposits", "prr": 14.60, "case_count": 310, "fatality_rate": 0.000, "chi_square": 590.1, "lower_ci": 11.5, "upper_ci": 18.5},
    ]
    return pd.DataFrame(signals_data)


def _generate_synthetic_reports() -> pd.DataFrame:
    """Generate demographic and report level data for interactive analysis."""
    np.random.seed(42)
    drugs = [
        "ATORVASTATIN", "ROSUVASTATIN", "ASPIRIN", "IBUPROFEN", 
        "METFORMIN", "LISINOPRIL", "WARFARIN", "PEMBROLIZUMAB", 
        "REMDESIVIR", "ACETAMINOPHEN", "AMIODARONE"
    ]
    
    quarters = [
        "2022Q1", "2022Q2", "2022Q3", "2022Q4",
        "2023Q1", "2023Q2", "2023Q3", "2023Q4",
        "2024Q1", "2024Q2", "2024Q3", "2024Q4",
    ]

    records = []
    report_id = 10000000

    drug_weights = {
        "ATORVASTATIN": (62, 10, 0.48),   # (mean_age, std_age, female_ratio)
        "ROSUVASTATIN": (60, 11, 0.50),
        "ASPIRIN": (67, 12, 0.44),
        "IBUPROFEN": (45, 18, 0.58),
        "METFORMIN": (58, 11, 0.52),
        "LISINOPRIL": (63, 11, 0.49),
        "WARFARIN": (71, 9, 0.47),
        "PEMBROLIZUMAB": (65, 10, 0.42),
        "REMDESIVIR": (59, 14, 0.45),
        "ACETAMINOPHEN": (42, 19, 0.55),
        "AMIODARONE": (69, 9, 0.40),
    }

    drug_reactions = {
        "ATORVASTATIN": ["Rhabdomyolysis", "Myopathy", "Acute Liver Injury", "Memory Impairment", "Headache"],
        "ROSUVASTATIN": ["Rhabdomyolysis", "Proteinuria", "Myalgia", "Renal Failure Acute", "Fatigue"],
        "ASPIRIN": ["Gastrointestinal Hemorrhage", "Gastric Ulcer", "Tinnitus", "Hematoma", "Dyspepsia"],
        "IBUPROFEN": ["Gastrointestinal Hemorrhage", "Renal Failure Acute", "Nephrotic Syndrome", "Nausea", "Rash"],
        "METFORMIN": ["Lactic Acidosis", "Vitamin B12 Deficiency", "Pancreatitis Acute", "Hypoglycemia", "Diarrhoea"],
        "LISINOPRIL": ["Angioedema", "Cough Persistent", "Hyperkalemia", "Renal Failure Acute", "Dizziness"],
        "WARFARIN": ["Cerebral Hemorrhage", "Gastrointestinal Hemorrhage", "International Normalized Ratio Increased", "Hematoma"],
        "PEMBROLIZUMAB": ["Pneumonitis", "Colitis Autoimmune", "Hypophysitis", "Myocarditis", "Fatigue"],
        "REMDESIVIR": ["Hepatic Enzyme Increased", "Bradycardia", "Renal Failure Acute", "Nausea"],
        "ACETAMINOPHEN": ["Acute Hepatic Failure", "Hepatotoxicity", "Jaundice", "Vomiting"],
        "AMIODARONE": ["Pulmonary Fibrosis", "Thyroid Dysfunction", "Corneal Microdeposits", "Tremor"],
    }

    fatal_prob = {
        "ATORVASTATIN": 0.05,
        "ROSUVASTATIN": 0.04,
        "ASPIRIN": 0.06,
        "IBUPROFEN": 0.03,
        "METFORMIN": 0.07,
        "LISINOPRIL": 0.04,
        "WARFARIN": 0.16,
        "PEMBROLIZUMAB": 0.14,
        "REMDESIVIR": 0.09,
        "ACETAMINOPHEN": 0.18,
        "AMIODARONE": 0.12,
    }

    # Generate ~2,400 realistic individual reports
    for drug in drugs:
        mean_a, std_a, f_ratio = drug_weights[drug]
        n_samples = np.random.randint(180, 260)
        
        ages = np.clip(np.random.normal(mean_a, std_a, n_samples), 1, 98).round(1)
        genders = np.random.choice(["Female", "Male", "Unknown / Not Reported"], p=[f_ratio, 1.0 - f_ratio - 0.05, 0.05], size=n_samples)
        qtrs = np.random.choice(quarters, size=n_samples)
        reactions = np.random.choice(drug_reactions[drug], size=n_samples)
        fatal_flags = (np.random.random(size=n_samples) < fatal_prob[drug]).astype(int)

        for i in range(n_samples):
            report_id += 1
            age_val = ages[i]
            if age_val < 18:
                age_grp = "Pediatric (0-17)"
            elif age_val < 65:
                age_grp = "Adult (18-64)"
            else:
                age_grp = "Elderly (65+)"

            records.append({
                "safetyreportid": str(report_id),
                "drug_name": drug,
                "reaction": reactions[i],
                "age": age_val,
                "gender": genders[i],
                "quarter": qtrs[i],
                "age_group": age_grp,
                "fatal": fatal_flags[i],
                "serious": 1 if fatal_flags[i] or np.random.random() < 0.75 else 0,
            })

    return pd.DataFrame(records)


@st.cache_data(ttl=600, show_spinner=False)
def load_signals_data() -> Tuple[pd.DataFrame, str]:
    """Load PRR safety signals. Returns (df, data_source_description)."""
    # 1. Try MongoDB
    mongo_df = _try_load_mongodb("signals")
    if mongo_df is not None and not mongo_df.empty:
        return mongo_df, "MongoDB (Live Analytics Store)"

    # 2. Try Parquet
    parquet_df = _try_load_parquet(SIGNALS_PARQUET_PATHS)
    if parquet_df is not None and not parquet_df.empty:
        return parquet_df, "Parquet Files (data/processed/signals)"

    # 3. Fallback to synthetic
    return _generate_synthetic_signals(), "Standardized FAERS Signal Database (Demo Mode)"


@st.cache_data(ttl=600, show_spinner=False)
def load_reports_data() -> Tuple[pd.DataFrame, str]:
    """Load individual adverse event reports. Returns (df, data_source_description)."""
    # 1. Try MongoDB
    mongo_df = _try_load_mongodb("reports")
    if mongo_df is not None and not mongo_df.empty:
        return mongo_df, "MongoDB (Live Analytics Store)"

    # 2. Try Parquet
    parquet_df = _try_load_parquet(REPORTS_PARQUET_PATHS)
    if parquet_df is not None and not parquet_df.empty:
        return parquet_df, "Parquet Files (data/processed/reports)"

    # 3. Fallback to synthetic
    return _generate_synthetic_reports(), "Standardized FAERS Patient Demographics (Demo Mode)"


def get_available_drugs() -> List[str]:
    """Return sorted unique list of all drugs present in the dataset."""
    signals_df, _ = load_signals_data()
    if "drug_name" in signals_df.columns:
        drugs = sorted(signals_df["drug_name"].dropna().unique().tolist())
        if drugs:
            return drugs
    
    reports_df, _ = load_reports_data()
    if "drug_name" in reports_df.columns:
        return sorted(reports_df["drug_name"].dropna().unique().tolist())
        
    return ["ATORVASTATIN", "ASPIRIN", "IBUPROFEN", "METFORMIN", "WARFARIN"]
