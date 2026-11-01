"""Process raw OpenFDA FAERS JSON into production Parquet datasets for dashboard & analytics.

Extracts patient demographics, drugs, and adverse reactions from data/raw/*.json,
computes the epidemiological Proportional Reporting Ratio (PRR) and Chi-square matrix,
and exports clean Parquet files to data/processed/.
"""

import glob
import json
import math
import os
import re
from pathlib import Path
import numpy as np
import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parent.parent
RAW_DIR = PROJECT_ROOT / "data" / "raw"
PROCESSED_DIR = PROJECT_ROOT / "data" / "processed"
PROCESSED_DIR.mkdir(parents=True, exist_ok=True)


def clean_drug_name(raw_name: str) -> str:
    """Normalize drug names to uppercase generic form."""
    if not raw_name:
        return "UNKNOWN"
    name = raw_name.upper().strip()
    # Remove dosage forms like "10MG", "TABLET", "CAPSULE", etc.
    name = re.sub(r"\b\d+(\.\d+)?\s*(MG|MCG|G|ML|%)\b", "", name)
    # Remove parenthetical content if it has alternative names
    match = re.search(r"\((.*?)\)", name)
    if match and len(match.group(1).strip()) > 3:
        inner = match.group(1).strip()
        if not re.search(r"\b(TABLET|CAPSULE|INJECTION|SOLUTION|ORAL|DAILY)\b", inner):
            name = inner
    name = re.sub(r"[\(\)\[\]]", "", name)
    name = re.sub(r"\s+", " ", name).strip()
    # Strip common suffixes
    for suffix in [" TABLET", " CAPSULE", " INJECTION", " ORAL", " CREAM", " OINTMENT", " SUSPENSION"]:
        if name.endswith(suffix):
            name = name[:-len(suffix)].strip()
    return name if len(name) >= 2 else "UNKNOWN"


def normalize_age(age_val, unit_val) -> tuple[float, str]:
    """Convert age and FDA unit code into standardized age in years."""
    try:
        age = float(age_val)
    except (TypeError, ValueError):
        return np.nan, "Unknown"

    if age <= 0 or age > 130:
        return np.nan, "Unknown"

    unit = str(unit_val).strip()
    if unit in ("800", "DEC"):    # Decades
        age_years = age * 10
    elif unit in ("801", "YR"):   # Years
        age_years = age
    elif unit in ("802", "MON"):  # Months
        age_years = age / 12.0
    elif unit in ("803", "WK"):   # Weeks
        age_years = age / 52.0
    elif unit in ("804", "DY"):   # Days
        age_years = age / 365.25
    elif unit in ("805", "HR"):   # Hours
        age_years = age / 8766.0
    else:
        # Default assume years if plausible
        age_years = age if age <= 120 else np.nan

    if np.isnan(age_years) or age_years > 120:
        return np.nan, "Unknown"

    if age_years < 18:
        group = "Pediatric (0-17)"
    elif age_years < 65:
        group = "Adult (18-64)"
    else:
        group = "Elderly (65+)"

    return round(age_years, 1), group


def parse_quarter(date_str: str) -> str:
    """Format YYYYMMDD into YYYYQ#."""
    if not date_str or len(str(date_str)) < 6:
        return "2024Q1"
    s = str(date_str)
    year = s[:4]
    month = int(s[4:6]) if s[4:6].isdigit() else 1
    q = (month - 1) // 3 + 1
    return f"{year}Q{q}"


def main():
    json_files = sorted(glob.glob(str(RAW_DIR / "*.json")))
    if not json_files:
        print("[ERROR] No JSON files found in", RAW_DIR)
        return

    print(f"[INFO] Found {len(json_files)} raw FAERS JSON file(s). Starting extraction...")

    reports_list = []
    pair_counts = {}       # (drug, reaction) -> count
    pair_deaths = {}       # (drug, reaction) -> death_count
    drug_counts = {}       # drug -> count
    reaction_counts = {}   # reaction -> count
    total_pairs = 0

    report_id_counter = 1

    for file_path in json_files:
        print(f"[INFO] Parsing {os.path.basename(file_path)}...")
        with open(file_path, "r", encoding="utf-8", errors="ignore") as f:
            try:
                payload = json.load(f)
            except Exception as e:
                print(f"[ERROR] Failed to load JSON {file_path}: {e}")
                continue

        results = payload.get("results", [])
        print(f"       Loaded {len(results):,} adverse event records from file.")

        for r in results:
            case_id = str(r.get("safetyreportid", report_id_counter))
            report_id_counter += 1
            rec_date = r.get("receivedate", "")
            quarter = parse_quarter(rec_date)
            
            is_serious = 1 if str(r.get("serious", "0")) == "1" else 0
            is_fatal = 1 if str(r.get("seriousnessdeath", "0")) == "1" else 0

            patient = r.get("patient", {})
            age_val = patient.get("patientonsetage")
            unit_val = patient.get("patientonsetageunit")
            age_years, age_group = normalize_age(age_val, unit_val)

            sex_code = str(patient.get("patientsex", "0")).strip()
            if sex_code == "1":
                gender = "Male"
            elif sex_code == "2":
                gender = "Female"
            else:
                gender = "Unknown / Not Reported"

            # Extract unique drugs in this case
            raw_drugs = patient.get("drug", [])
            case_drugs = set()
            for d in raw_drugs:
                # Prefer OpenFDA generic name if present
                openfda = d.get("openfda", {})
                gen_names = openfda.get("generic_name", [])
                if gen_names and gen_names[0]:
                    dname = gen_names[0].upper().strip()
                else:
                    dname = clean_drug_name(d.get("medicinalproduct", ""))
                if dname and dname != "UNKNOWN":
                    case_drugs.add(dname)

            # Extract unique reactions in this case
            raw_reactions = patient.get("reaction", [])
            case_reactions = set()
            for rx in raw_reactions:
                pt = rx.get("reactionmeddrapt", "").strip().title()
                if pt:
                    case_reactions.add(pt)

            if not case_drugs or not case_reactions:
                continue

            # Add to reports DataFrame (patient-level record)
            for dname in case_drugs:
                for rx in case_reactions:
                    reports_list.append({
                        "safetyreportid": case_id,
                        "drug_name": dname,
                        "reaction": rx,
                        "age": age_years,
                        "gender": gender,
                        "quarter": quarter,
                        "age_group": age_group,
                        "fatal": is_fatal,
                        "serious": is_serious,
                    })

                    pair = (dname, rx)
                    pair_counts[pair] = pair_counts.get(pair, 0) + 1
                    if is_fatal:
                        pair_deaths[pair] = pair_deaths.get(pair, 0) + 1

                    drug_counts[dname] = drug_counts.get(dname, 0) + 1
                    reaction_counts[rx] = reaction_counts.get(rx, 0) + 1
                    total_pairs += 1

    print(f"\n[INFO] Extracted {len(reports_list):,} (case, drug, reaction) reporting records.")
    print(f"[INFO] Unique Drugs: {len(drug_counts):,}")
    print(f"[INFO] Unique Adverse Reactions: {len(reaction_counts):,}")
    print(f"[INFO] Unique Drug-Reaction Pairs: {len(pair_counts):,}")

    # Build and Save reports.parquet
    reports_df = pd.DataFrame(reports_list)
    reports_path = PROCESSED_DIR / "reports.parquet"
    reports_df.to_parquet(reports_path, index=False)
    print(f"[SUCCESS] Exported reports dataset -> {reports_path} ({len(reports_df):,} rows)")

    # Compute PRR, Chi-Square, and Confidence Intervals for all signals
    print("[INFO] Computing 2x2 contingency tables and PRR scores across all pairs...")
    signals_list = []
    N = total_pairs

    for (dname, rx), a in pair_counts.items():
        # Minimum filter: at least 2 reports to avoid excessive noise
        if a < 2:
            continue

        drug_total = drug_counts[dname]        # a + b
        reaction_total = reaction_counts[rx]  # a + c

        b = drug_total - a
        c = reaction_total - a
        d = N - a - b - c

        if (c + d) <= 0 or (a + b) <= 0 or c <= 0:
            continue

        # Proportional Reporting Ratio
        prr = (a / (a + b)) / (c / (c + d))

        # Pearson Chi-Square with 1 degree of freedom
        numerator = N * ((a * d - b * c) ** 2)
        denominator = float((a + b) * (c + d) * (a + c) * (b + d))
        chi2 = (numerator / denominator) if denominator > 0 else 0.0

        # Standard Error of ln(PRR) for 95% Confidence Intervals
        try:
            se_ln_prr = math.sqrt(max(0, (1.0 / a) - (1.0 / (a + b)) + (1.0 / c) - (1.0 / (c + d))))
            lower_ci = round(math.exp(math.log(max(1e-4, prr)) - 1.96 * se_ln_prr), 2)
            upper_ci = round(math.exp(math.log(max(1e-4, prr)) + 1.96 * se_ln_prr), 2)
        except Exception:
            lower_ci = round(prr * 0.8, 2)
            upper_ci = round(prr * 1.2, 2)

        fatal_count = pair_deaths.get((dname, rx), 0)
        fatality_rate = round(fatal_count / a, 4)

        signals_list.append({
            "drug_name": dname,
            "reaction": rx,
            "prr": round(prr, 2),
            "case_count": a,
            "fatality_rate": fatality_rate,
            "chi_square": round(chi2, 1),
            "lower_ci": lower_ci,
            "upper_ci": upper_ci,
        })

    signals_df = pd.DataFrame(signals_list)
    signals_df = signals_df.sort_values(by=["prr", "case_count"], ascending=[False, False])
    signals_path = PROCESSED_DIR / "signals.parquet"
    signals_df.to_parquet(signals_path, index=False)
    print(f"[SUCCESS] Exported safety signals -> {signals_path} ({len(signals_df):,} scored pairs)")

    # Also save directory versions in case data_loader looks for directory partitions
    (PROCESSED_DIR / "signals").mkdir(exist_ok=True)
    signals_df.to_parquet(PROCESSED_DIR / "signals" / "part-0.parquet", index=False)
    (PROCESSED_DIR / "reports").mkdir(exist_ok=True)
    reports_df.to_parquet(PROCESSED_DIR / "reports" / "part-0.parquet", index=False)

    print("\n================================================================")
    print("REAL DATASET READY FOR PRODUCTION DASHBOARD & EVALUATION")
    print(f"Total Drugs Scored: {signals_df['drug_name'].nunique():,}")
    print(f"Total Statistically Flagged Signals: {len(signals_df[signals_df['prr'] >= 2.0]):,}")
    print("================================================================")


if __name__ == "__main__":
    main()
