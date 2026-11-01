"""ETL Step 04: FAERS Data Quality Validation and Profiling.

Performs comprehensive data quality validation and statistical profiling
on the flattened adverse event dataset in data/processed/03_flattened/:
- Total record count and case counts.
- Column null rate analysis.
- Unique cardinality metrics (unique drugs, reactions, cases).
- Top 10 most frequently reported drugs and adverse reactions.
- Patient sex distribution and age bracket histogram.
- Severity and clinical outcome rates (serious, death, hospitalization).
- Writes a formatted text report to data/processed/validation_report.txt.
- Returns a structured dictionary of quality metrics.
"""

import argparse
from datetime import datetime
import sys
from pathlib import Path
from typing import Any, Dict, List, Optional

# Setup project root for config imports
PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from config.spark_config import get_spark_session

DEFAULT_INPUT_DIR = PROJECT_ROOT / "data" / "processed" / "03_flattened"
DEFAULT_REPORT_PATH = PROJECT_ROOT / "data" / "processed" / "validation_report.txt"


def run_validate(
    input_dir: Optional[str] = None,
    report_path: Optional[str] = None,
    spark: Optional[SparkSession] = None,
) -> Dict[str, Any]:
    """Execute quality checks, generate profile metrics, and save text report.

    Parameters
    ----------
    input_dir : str, optional
        Filesystem path to flattened Parquet files. Defaults to data/processed/03_flattened.
    report_path : str, optional
        Target text file path for report. Defaults to data/processed/validation_report.txt.
    spark : SparkSession, optional
        Active PySpark session. If None, initialized via config.spark_config.

    Returns
    -------
    dict
        Dictionary containing all computed data quality metrics.
    """
    in_path = Path(input_dir) if input_dir else DEFAULT_INPUT_DIR
    out_report = Path(report_path) if report_path else DEFAULT_REPORT_PATH

    print("=" * 70)
    print("ETL Step 04: FAERS Data Quality Validation & Profiling")
    print(f"Input Directory : {in_path.resolve()}")
    print(f"Report Target   : {out_report.resolve()}")
    print("=" * 70)

    if not in_path.exists():
        raise FileNotFoundError(
            f"Input directory does not exist: {in_path.resolve()}. "
            "Please run etl/03_flatten.py before executing validation step."
        )

    active_spark = spark or get_spark_session(app_name="FAERS-ETL-04-Validate")

    print(f"[INFO] Reading flattened Parquet records from {in_path}...")
    df: DataFrame = active_spark.read.parquet(str(in_path))

    # 1. Total records count
    total_records = df.count()
    if total_records == 0:
        print("[WARNING] Dataset is empty (0 records). Skipping detailed metrics.")
        return {"total_records": 0}

    print(f"[INFO] Total Flattened Records: {total_records:,}")

    # 2. Unique counts
    print("[INFO] Computing unique cardinalities...")
    unique_counts = df.select(
        F.countDistinct("case_id").alias("unique_cases"),
        F.countDistinct("drug_name").alias("unique_drugs"),
        F.countDistinct("reaction").alias("unique_reactions"),
    ).first()

    unique_cases = unique_counts["unique_cases"] if unique_counts else 0
    unique_drugs = unique_counts["unique_drugs"] if unique_counts else 0
    unique_reactions = unique_counts["unique_reactions"] if unique_counts else 0

    print(f"    Unique Cases     : {unique_cases:,}")
    print(f"    Unique Drugs     : {unique_drugs:,}")
    print(f"    Unique Reactions : {unique_reactions:,}")

    # 3. Column null rates
    print("[INFO] Calculating column null rates...")
    null_exprs = [
        F.sum(F.when(F.col(c).isNull(), 1).otherwise(0)).alias(c)
        for c in df.columns
    ]
    null_row = df.select(null_exprs).first()

    null_rates: Dict[str, float] = {}
    null_counts_dict: Dict[str, int] = {}
    for col_name in df.columns:
        cnt = null_row[col_name] if null_row else 0
        null_counts_dict[col_name] = cnt
        null_rates[col_name] = round((cnt / total_records) * 100.0, 2)

    # 4. Top 10 most common drugs
    print("[INFO] Aggregating top 10 most reported drugs...")
    top_drugs_df = (
        df.groupBy("drug_name")
        .count()
        .orderBy(F.col("count").desc())
        .limit(10)
    )
    top_drugs_rows = top_drugs_df.collect()
    top_drugs = [
        {
            "rank": idx,
            "drug_name": row["drug_name"],
            "count": row["count"],
            "percentage": round((row["count"] / total_records) * 100.0, 2),
        }
        for idx, row in enumerate(top_drugs_rows, start=1)
    ]

    # 5. Top 10 most common reactions
    print("[INFO] Aggregating top 10 most reported adverse reactions...")
    top_rxns_df = (
        df.groupBy("reaction")
        .count()
        .orderBy(F.col("count").desc())
        .limit(10)
    )
    top_rxns_rows = top_rxns_df.collect()
    top_reactions = [
        {
            "rank": idx,
            "reaction": row["reaction"],
            "count": row["count"],
            "percentage": round((row["count"] / total_records) * 100.0, 2),
        }
        for idx, row in enumerate(top_rxns_rows, start=1)
    ]

    # 6. Patient sex distribution
    print("[INFO] Computing patient sex distribution...")
    sex_df = df.groupBy("patient_sex").count().orderBy(F.col("count").desc())
    sex_rows = sex_df.collect()
    sex_distribution: Dict[str, Dict[str, Any]] = {}
    for row in sex_rows:
        cnt = row["count"]
        sex_distribution[row["patient_sex"]] = {
            "count": cnt,
            "percentage": round((cnt / total_records) * 100.0, 2),
        }

    # 7. Patient age histogram (bins: 0-18, 18-40, 40-65, 65+, Unknown)
    print("[INFO] Computing patient age distribution bins...")
    age_binned_df = df.withColumn(
        "age_bin",
        F.when(F.col("patient_age").isNull(), "Unknown / Missing")
        .when(F.col("patient_age") < 18.0, "0-18 (Pediatric)")
        .when((F.col("patient_age") >= 18.0) & (F.col("patient_age") < 40.0), "18-40 (Young Adult)")
        .when((F.col("patient_age") >= 40.0) & (F.col("patient_age") < 65.0), "40-65 (Middle Aged)")
        .otherwise("65+ (Elderly)"),
    )
    age_counts = age_binned_df.groupBy("age_bin").count().collect()
    age_counts_dict = {r["age_bin"]: r["count"] for r in age_counts}

    ordered_age_bins = [
        "0-18 (Pediatric)",
        "18-40 (Young Adult)",
        "40-65 (Middle Aged)",
        "65+ (Elderly)",
        "Unknown / Missing",
    ]
    age_histogram: Dict[str, Dict[str, Any]] = {}
    for b in ordered_age_bins:
        cnt = age_counts_dict.get(b, 0)
        age_histogram[b] = {
            "count": cnt,
            "percentage": round((cnt / total_records) * 100.0, 2),
        }

    # 8. Clinical outcomes rates
    outcome_counts = df.select(
        F.sum("is_serious").alias("serious_cnt"),
        F.sum("outcome_death").alias("death_cnt"),
        F.sum("outcome_hospitalization").alias("hosp_cnt"),
    ).first()

    serious_cnt = int(outcome_counts["serious_cnt"] or 0)
    death_cnt = int(outcome_counts["death_cnt"] or 0)
    hosp_cnt = int(outcome_counts["hosp_cnt"] or 0)

    outcomes = {
        "serious_cases": {"count": serious_cnt, "percentage": round(serious_cnt / total_records * 100.0, 2)},
        "death_cases": {"count": death_cnt, "percentage": round(death_cnt / total_records * 100.0, 2)},
        "hospitalization_cases": {"count": hosp_cnt, "percentage": round(hosp_cnt / total_records * 100.0, 2)},
    }

    # Assemble metrics dictionary
    quality_metrics: Dict[str, Any] = {
        "timestamp": datetime.utcnow().isoformat(),
        "total_records": total_records,
        "unique_cases": unique_cases,
        "unique_drugs": unique_drugs,
        "unique_reactions": unique_reactions,
        "null_rates_pct": null_rates,
        "patient_sex_distribution": sex_distribution,
        "patient_age_histogram": age_histogram,
        "top_10_drugs": top_drugs,
        "top_10_reactions": top_reactions,
        "clinical_outcomes": outcomes,
    }

    # Print summary to console
    print("\n" + "=" * 70)
    print("DATA QUALITY & PROFILE SUMMARY")
    print("=" * 70)
    print(f"Total Rows: {total_records:,} | Unique Cases: {unique_cases:,} | Unique Drugs: {unique_drugs:,} | Unique Reactions: {unique_reactions:,}\n")

    print("Column Null Rates:")
    for col_name, rate in null_rates.items():
        bar = "#" * int(rate / 5)
        print(f"  - {col_name:26}: {rate:5.2f}% null [{bar:<20}]")

    print("\nTop 10 Most Common Drugs:")
    for d in top_drugs:
        print(f"  {d['rank']:2d}. {d['drug_name']:<30} : {d['count']:>8,} records ({d['percentage']:5.2f}%)")

    print("\nTop 10 Most Common Adverse Reactions:")
    for r in top_reactions:
        print(f"  {r['rank']:2d}. {r['reaction']:<30} : {r['count']:>8,} records ({r['percentage']:5.2f}%)")

    print("\nPatient Sex Distribution:")
    for sex, data in sex_distribution.items():
        print(f"  - {sex:<10} : {data['count']:>8,} ({data['percentage']:5.2f}%)")

    print("\nPatient Age Histogram:")
    for bin_name, data in age_histogram.items():
        bar = "=" * int(data["percentage"] / 2)
        print(f"  - {bin_name:<20} : {data['count']:>8,} ({data['percentage']:5.2f}%) [{bar}]")

    print("\nClinical Severity Indicators:")
    print(f"  - Serious Reports       : {outcomes['serious_cases']['count']:,} ({outcomes['serious_cases']['percentage']}%)")
    print(f"  - Hospitalization Cases : {outcomes['hospitalization_cases']['count']:,} ({outcomes['hospitalization_cases']['percentage']}%)")
    print(f"  - Fatal Outcomes (Death): {outcomes['death_cases']['count']:,} ({outcomes['death_cases']['percentage']}%)")
    print("=" * 70)

    # 9. Generate and write validation report text file
    out_report.parent.mkdir(parents=True, exist_ok=True)
    report_lines: List[str] = [
        "================================================================================",
        "PHARMACOVIGILANCE FAERS PIPELINE: DATA QUALITY VALIDATION REPORT",
        "================================================================================",
        f"Generated At (UTC): {datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S UTC')}",
        f"Input Parquet Path: {in_path.resolve()}",
        f"Pipeline Target   : Big Data Analytics ED547 Pharmacovigilance Signals",
        "--------------------------------------------------------------------------------",
        "",
        "1. DATASET DIMENSIONS & CARDINALITY",
        "--------------------------------------------------------------------------------",
        f"Total Flattened Records (drug-event pairs) : {total_records:,}",
        f"Unique Safety Report Cases (safetyreportid): {unique_cases:,}",
        f"Unique Drug Terms (Standardized generic)   : {unique_drugs:,}",
        f"Unique Reaction Terms (MedDRA PT)          : {unique_reactions:,}",
        "",
        "2. COLUMN COMPLETENESS & NULL RATES",
        "--------------------------------------------------------------------------------",
        f"{'Column Name':<28} {'Null Count':<12} {'Null Rate (%)':<15} {'Status'}",
        "-" * 70,
    ]

    for col_name in df.columns:
        rate = null_rates[col_name]
        cnt = null_counts_dict[col_name]
        status = "CRITICAL" if col_name in ("case_id", "drug_name", "reaction") and rate > 0 else (
            "WARN" if rate > 50.0 else "PASS"
        )
        report_lines.append(f"{col_name:<28} {cnt:<12,} {rate:<15.2f} {status}")

    report_lines.extend([
        "",
        "3. PATIENT DEMOGRAPHIC PROFILES",
        "--------------------------------------------------------------------------------",
        "Sex Distribution:",
    ])
    for sex, data in sex_distribution.items():
        report_lines.append(f"  - {sex:<15}: {data['count']:>10,} records ({data['percentage']:5.2f}%)")

    report_lines.append("\nAge Bracket Distribution:")
    for bin_name, data in age_histogram.items():
        report_lines.append(f"  - {bin_name:<22}: {data['count']:>10,} records ({data['percentage']:5.2f}%)")

    report_lines.extend([
        "",
        "4. TOP 10 REPORTED DRUGS",
        "--------------------------------------------------------------------------------",
        f"{'Rank':<6} {'Drug Name':<34} {'Record Count':<15} {'Percentage'}",
        "-" * 70,
    ])
    for d in top_drugs:
        report_lines.append(f"{d['rank']:<6} {d['drug_name']:<34} {d['count']:<15,} {d['percentage']:5.2f}%")

    report_lines.extend([
        "",
        "5. TOP 10 REPORTED ADVERSE REACTIONS (MedDRA Preferred Terms)",
        "--------------------------------------------------------------------------------",
        f"{'Rank':<6} {'MedDRA Reaction Term':<34} {'Record Count':<15} {'Percentage'}",
        "-" * 70,
    ])
    for r in top_reactions:
        report_lines.append(f"{r['rank']:<6} {r['reaction']:<34} {r['count']:<15,} {r['percentage']:5.2f}%")

    report_lines.extend([
        "",
        "6. CLINICAL SEVERITY & OUTCOMES SUMMARY",
        "--------------------------------------------------------------------------------",
        f"Serious Cases         : {outcomes['serious_cases']['count']:,} ({outcomes['serious_cases']['percentage']}%)",
        f"Hospitalization Cases : {outcomes['hospitalization_cases']['count']:,} ({outcomes['hospitalization_cases']['percentage']}%)",
        f"Fatal Outcomes (Death): {outcomes['death_cases']['count']:,} ({outcomes['death_cases']['percentage']}%)",
        "",
        "================================================================================",
        "END OF VALIDATION REPORT",
        "================================================================================",
    ])

    report_content = "\n".join(report_lines)
    with open(out_report, "w", encoding="utf-8") as f:
        f.write(report_content)

    print(f"\n[SUCCESS] Validation report successfully written to: {out_report.resolve()}")

    return quality_metrics


def main() -> None:
    """CLI entrypoint for ETL Step 04."""
    parser = argparse.ArgumentParser(
        description="ETL Step 04: Profile data quality and generate validation report for flattened FAERS data.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument(
        "--input-dir",
        type=str,
        default=str(DEFAULT_INPUT_DIR),
        help="Path to folder containing flattened Parquet files",
    )
    parser.add_argument(
        "--report-path",
        type=str,
        default=str(DEFAULT_REPORT_PATH),
        help="Target text file path for quality report",
    )

    args = parser.parse_args()

    try:
        run_validate(input_dir=args.input_dir, report_path=args.report_path)
    except Exception as err:
        print(f"[FATAL] ETL Step 04 failed: {err}")
        sys.exit(1)


if __name__ == "__main__":
    main()
