"""ETL Step 01: Raw FAERS JSON Ingestion.

Reads raw OpenFDA FAERS JSON files from data/raw/ using PySpark,
flattens the top-level 'results' array into individual adverse event records,
projects key metadata and patient fields, and writes optimized Parquet
files to data/processed/01_ingested/.
"""

import argparse
import sys
from pathlib import Path
from typing import Optional

# Add project root to sys.path to enable config imports
PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from config.spark_config import get_spark_session

DEFAULT_INPUT_DIR = PROJECT_ROOT / "data" / "raw"
DEFAULT_OUTPUT_DIR = PROJECT_ROOT / "data" / "processed" / "01_ingested"


def run_ingest(
    input_dir: Optional[str] = None,
    output_dir: Optional[str] = None,
    spark: Optional[SparkSession] = None,
) -> DataFrame:
    """Read raw FAERS JSON files, extract event records, and save to Parquet.

    Parameters
    ----------
    input_dir : str, optional
        Filesystem path to folder containing raw JSON files. Defaults to data/raw.
    output_dir : str, optional
        Filesystem path where ingested Parquet data is saved. Defaults to data/processed/01_ingested.
    spark : SparkSession, optional
        Active PySpark session. If None, creates a session using config.spark_config.

    Returns
    -------
    DataFrame
        PySpark DataFrame containing the ingested event records.

    Raises
    ------
    FileNotFoundError
        If input directory does not exist or contains no JSON files.
    """
    raw_path = Path(input_dir) if input_dir else DEFAULT_INPUT_DIR
    target_path = Path(output_dir) if output_dir else DEFAULT_OUTPUT_DIR

    print("=" * 70)
    print("ETL Step 01: Raw FAERS JSON Ingestion")
    print(f"Input Directory  : {raw_path.resolve()}")
    print(f"Output Directory : {target_path.resolve()}")
    print("=" * 70)

    # Validate input directory and check for JSON files
    if not raw_path.exists():
        raise FileNotFoundError(f"Input directory does not exist: {raw_path.resolve()}")

    json_files = list(raw_path.glob("*.json"))
    if not json_files:
        raise FileNotFoundError(
            f"No .json files found in {raw_path.resolve()}. "
            "Please run data/download_faers.py first to populate raw data."
        )

    print(f"[INFO] Found {len(json_files)} raw JSON file(s) to ingest.")

    # Initialize SparkSession
    active_spark = spark or get_spark_session(app_name="FAERS-ETL-01-Ingest")

    # Read raw JSON files using multiline JSON parser
    # OpenFDA FAERS files contain a single root object per file: { meta: {...}, results: [...] }
    json_glob_path = str(raw_path / "*.json")
    print(f"[INFO] Reading JSON files from: {json_glob_path}")

    raw_df = (
        active_spark.read
        .option("multiline", "true")
        .option("mode", "PERMISSIVE")
        .json(json_glob_path)
    )

    # Extract individual event records:
    # If the root object contains a 'results' array (standard OpenFDA format), explode it.
    # Otherwise, assume data is already newline-delimited individual records.
    if "results" in raw_df.columns:
        print("[INFO] Detected top-level 'results' array. Exploding into event records...")
        events_df = raw_df.select(F.explode("results").alias("record")).select("record.*")
    else:
        print("[INFO] Processing records directly from root schema...")
        events_df = raw_df

    # Defensively verify presence of required fields
    key_fields = [
        "safetyreportid",
        "receivedate",
        "serious",
        "seriousnessdeath",
        "seriousnesshospitalization",
    ]
    for field in key_fields:
        if field not in events_df.columns:
            print(f"[WARNING] Field '{field}' missing from raw schema. Defaulting to NULL.")
            events_df = events_df.withColumn(field, F.lit(None).cast("string"))

    if "patient" not in events_df.columns:
        print("[WARNING] Field 'patient' missing from raw schema. Defaulting to NULL struct.")
        events_df = events_df.withColumn("patient", F.lit(None))

    # Project the key required fields
    ingested_df = events_df.select(
        F.col("safetyreportid").cast("string").alias("safetyreportid"),
        F.col("receivedate").cast("string").alias("receivedate"),
        F.col("serious").cast("string").alias("serious"),
        F.col("seriousnessdeath").cast("string").alias("seriousnessdeath"),
        F.col("seriousnesshospitalization").cast("string").alias("seriousnesshospitalization"),
        F.col("patient"),
    ).filter(F.col("safetyreportid").isNotNull())

    # Write to Parquet format (partition coalescing for manageable file sizes)
    target_path.parent.mkdir(parents=True, exist_ok=True)
    out_uri = str(target_path)

    print(f"[INFO] Writing ingested records to Parquet at {out_uri}...")
    ingested_df.write.mode("overwrite").parquet(out_uri)

    # Read back saved dataset to report exact count and confirm write success
    persisted_df = active_spark.read.parquet(out_uri)
    total_count = persisted_df.count()

    print(f"[SUCCESS] Ingestion complete. Total records stored: {total_count:,}")
    print("\nIngested DataFrame Schema:")
    persisted_df.printSchema()

    print("\nSample Ingested Records:")
    persisted_df.show(5, truncate=True)

    return persisted_df


def main() -> None:
    """CLI entrypoint for ETL Step 01."""
    parser = argparse.ArgumentParser(
        description="ETL Step 01: Ingest raw OpenFDA FAERS JSON files into Parquet format.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument(
        "--input-dir",
        type=str,
        default=str(DEFAULT_INPUT_DIR),
        help="Path to folder containing raw FAERS JSON files",
    )
    parser.add_argument(
        "--output-dir",
        type=str,
        default=str(DEFAULT_OUTPUT_DIR),
        help="Target folder for ingested Parquet files",
    )

    args = parser.parse_args()

    try:
        run_ingest(input_dir=args.input_dir, output_dir=args.output_dir)
    except Exception as err:
        print(f"[FATAL] ETL Step 01 failed: {err}")
        sys.exit(1)


if __name__ == "__main__":
    main()
