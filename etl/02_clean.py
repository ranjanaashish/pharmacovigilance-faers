"""ETL Step 02: FAERS Data Cleaning and Standardization.

Reads ingested Parquet data from data/processed/01_ingested/, performs:
1. Deduplication by case_id (safetyreportid), retaining latest receivedate report.
2. Demographics mapping:
   - patient_sex: 1 -> Male, 2 -> Female, others -> Unknown.
   - patient_age: Normalized to years using FDA unit codes (800=Decade, 801=Year, 802=Month, 803=Week, 804=Day, 805=Hour).
   - patient_weight: Validated numeric weights, outliers mapped to NULL.
3. Removal of uninformative reports where both drug and reaction arrays are empty/null.
4. Output written to data/processed/02_cleaned/ in Parquet format.
"""

import argparse
import sys
from pathlib import Path
from typing import Optional

# Setup project root for config imports
PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from config.spark_config import get_spark_session

DEFAULT_INPUT_DIR = PROJECT_ROOT / "data" / "processed" / "01_ingested"
DEFAULT_OUTPUT_DIR = PROJECT_ROOT / "data" / "processed" / "02_cleaned"


def run_clean(
    input_dir: Optional[str] = None,
    output_dir: Optional[str] = None,
    spark: Optional[SparkSession] = None,
) -> DataFrame:
    """Execute cleaning, deduplication, and demographic standardization on ingested FAERS data.

    Parameters
    ----------
    input_dir : str, optional
        Filesystem path to ingested Parquet folder. Defaults to data/processed/01_ingested.
    output_dir : str, optional
        Filesystem path where cleaned Parquet dataset is saved. Defaults to data/processed/02_cleaned.
    spark : SparkSession, optional
        Active PySpark session. If None, initialized via config.spark_config.

    Returns
    -------
    DataFrame
        PySpark DataFrame of cleaned and deduplicated reports.
    """
    in_path = Path(input_dir) if input_dir else DEFAULT_INPUT_DIR
    out_path = Path(output_dir) if output_dir else DEFAULT_OUTPUT_DIR

    print("=" * 70)
    print("ETL Step 02: FAERS Data Cleaning & Deduplication")
    print(f"Input Directory  : {in_path.resolve()}")
    print(f"Output Directory : {out_path.resolve()}")
    print("=" * 70)

    if not in_path.exists():
        raise FileNotFoundError(
            f"Input directory does not exist: {in_path.resolve()}. "
            "Please run etl/01_ingest.py before executing clean step."
        )

    active_spark = spark or get_spark_session(app_name="FAERS-ETL-02-Clean")

    print(f"[INFO] Reading ingested Parquet data from {in_path}...")
    df = active_spark.read.parquet(str(in_path))
    raw_count = df.count()
    print(f"[INFO] Total records read from ingest: {raw_count:,}")

    # 1. Deduplicate by safetyreportid (case_id), keeping latest receivedate
    # Case updates in FAERS retain safetyreportid with higher version / newer receivedate.
    print("[INFO] Deduplicating cases by safetyreportid (keeping latest receivedate)...")
    window_dedup = Window.partitionBy("safetyreportid").orderBy(
        F.col("receivedate").desc_nulls_last()
    )
    deduped_df = (
        df.withColumn("_row_num", F.row_number().over(window_dedup))
        .filter(F.col("_row_num") == 1)
        .drop("_row_num")
    )

    # 2. Map patient sex codes
    # OpenFDA FAERS patientsex: 1 = Male, 2 = Female, 0 / null / other = Unknown
    sex_col = F.col("patient.patientsex").cast("string")
    mapped_sex = (
        F.when(sex_col == "1", "Male")
        .when(sex_col == "2", "Female")
        .otherwise("Unknown")
    )

    # 3. Normalize age to years
    # FDA Age Unit Codes:
    # 800 = Decade (value * 10.0)
    # 801 = Year   (value * 1.0)
    # 802 = Month  (value / 12.0)
    # 803 = Week   (value / 52.0)
    # 804 = Day    (value / 365.25)
    # 805 = Hour   (value / 8766.0)
    raw_age = F.col("patient.patientonsetage").cast("double")
    raw_unit = F.col("patient.patientonsetageunit").cast("string")

    normalized_age = (
        F.when(raw_age.isNull() | (raw_age < 0.0), F.lit(None).cast("double"))
        .when(raw_unit == "800", raw_age * 10.0)
        .when(raw_unit == "801", raw_age * 1.0)
        .when(raw_unit == "802", raw_age / 12.0)
        .when(raw_unit == "803", raw_age / 52.0)
        .when(raw_unit == "804", raw_age / 365.25)
        .when(raw_unit == "805", raw_age / 8766.0)
        .when(raw_unit.isNull() & (raw_age >= 0.0) & (raw_age <= 120.0), raw_age)
        .otherwise(F.lit(None).cast("double"))
    )
    # Filter biologically implausible human ages (> 125 years)
    normalized_age = F.when(
        (normalized_age >= 0.0) & (normalized_age <= 125.0),
        F.round(normalized_age, 1),
    ).otherwise(F.lit(None).cast("double"))

    # 4. Handle missing/invalid numeric weight (kg)
    # Typical adult weights are 2 - 500 kg; outliers mapped to NULL
    raw_weight = F.col("patient.patientweight").cast("double")
    normalized_weight = F.when(
        (raw_weight > 0.0) & (raw_weight <= 600.0),
        F.round(raw_weight, 1),
    ).otherwise(F.lit(None).cast("double"))

    # 5. Filter out records where both drug and reaction arrays are null or empty
    print("[INFO] Filtering out uninformative records where both drug and reaction are missing...")
    drug_empty = F.col("patient.drug").isNull() | (F.size("patient.drug") <= 0)
    reaction_empty = F.col("patient.reaction").isNull() | (F.size("patient.reaction") <= 0)
    both_empty = drug_empty & reaction_empty

    # Assemble cleaned dataset
    cleaned_df = (
        deduped_df
        .filter(~both_empty)
        .withColumn("case_id", F.col("safetyreportid"))
        .withColumn("receive_date", F.col("receivedate"))
        .withColumn("patient_sex", mapped_sex)
        .withColumn("patient_age", normalized_age)
        .withColumn("patient_weight", normalized_weight)
        .select(
            "case_id",
            "receive_date",
            "patient_age",
            "patient_sex",
            "patient_weight",
            "serious",
            "seriousnessdeath",
            "seriousnesshospitalization",
            "patient",
        )
    )

    # Write cleaned records to Parquet
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_uri = str(out_path)
    print(f"[INFO] Writing cleaned records to {out_uri}...")
    cleaned_df.write.mode("overwrite").parquet(out_uri)

    # Read back to evaluate metrics and verify
    persisted_df = active_spark.read.parquet(out_uri)
    clean_count = persisted_df.count()

    print(f"[SUCCESS] Cleaning complete.")
    print(f"    Raw input records : {raw_count:,}")
    print(f"    Clean unique cases: {clean_count:,} (Removed {raw_count - clean_count:,} duplicates/empty)")
    print("\nCleaned DataFrame Schema:")
    persisted_df.printSchema()

    print("\nSample Cleaned Records:")
    persisted_df.select(
        "case_id", "receive_date", "patient_age", "patient_sex", "patient_weight", "serious"
    ).show(5, truncate=False)

    return persisted_df


def main() -> None:
    """CLI entrypoint for ETL Step 02."""
    parser = argparse.ArgumentParser(
        description="ETL Step 02: Clean, deduplicate, and standardize FAERS records.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument(
        "--input-dir",
        type=str,
        default=str(DEFAULT_INPUT_DIR),
        help="Path to folder containing ingested Parquet files",
    )
    parser.add_argument(
        "--output-dir",
        type=str,
        default=str(DEFAULT_OUTPUT_DIR),
        help="Target folder for cleaned Parquet files",
    )

    args = parser.parse_args()

    try:
        run_clean(input_dir=args.input_dir, output_dir=args.output_dir)
    except Exception as err:
        print(f"[FATAL] ETL Step 02 failed: {err}")
        sys.exit(1)


if __name__ == "__main__":
    main()
