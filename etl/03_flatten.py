"""ETL Step 03: Explode and Flatten Nested FAERS Arrays.

Reads cleaned Parquet records from data/processed/02_cleaned/, explodes
the nested patient.drug and patient.reaction arrays into a flat (case, drug, reaction)
representation required for pharmacovigilance signal detection (PRR / Chi-square).

Transformations:
- Explodes patient.drug into individual drug records.
- Explodes patient.reaction into individual adverse reaction records.
- Extracts medicinalproduct, generic_name, drugindication, drugcharacterization, reactionmeddrapt.
- Normalizes drug_name: prioritizes FDA generic_name; falls back to uppercase trimmed medicinalproduct.
- Maps drugcharacterization codes: 1 -> Primary Suspect, 2 -> Concomitant, 3 -> Interacting.
- Extracts and casts binary severity/outcome indicators (is_serious, outcome_death, outcome_hospitalization).
- Writes final flat schema to data/processed/03_flattened/.
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
from config.spark_config import get_spark_session

DEFAULT_INPUT_DIR = PROJECT_ROOT / "data" / "processed" / "02_cleaned"
DEFAULT_OUTPUT_DIR = PROJECT_ROOT / "data" / "processed" / "03_flattened"


def run_flatten(
    input_dir: Optional[str] = None,
    output_dir: Optional[str] = None,
    spark: Optional[SparkSession] = None,
) -> DataFrame:
    """Explode nested drug and reaction arrays into flattened drug-event records.

    Parameters
    ----------
    input_dir : str, optional
        Filesystem path to cleaned Parquet data. Defaults to data/processed/02_cleaned.
    output_dir : str, optional
        Filesystem path where flattened Parquet dataset is saved. Defaults to data/processed/03_flattened.
    spark : SparkSession, optional
        Active PySpark session. If None, initialized via config.spark_config.

    Returns
    -------
    DataFrame
        PySpark DataFrame adhering to the target flat schema.
    """
    in_path = Path(input_dir) if input_dir else DEFAULT_INPUT_DIR
    out_path = Path(output_dir) if output_dir else DEFAULT_OUTPUT_DIR

    print("=" * 70)
    print("ETL Step 03: Flatten Nested FAERS Drug and Reaction Arrays")
    print(f"Input Directory  : {in_path.resolve()}")
    print(f"Output Directory : {out_path.resolve()}")
    print("=" * 70)

    if not in_path.exists():
        raise FileNotFoundError(
            f"Input directory does not exist: {in_path.resolve()}. "
            "Please run etl/02_clean.py before executing flatten step."
        )

    active_spark = spark or get_spark_session(app_name="FAERS-ETL-03-Flatten")

    print(f"[INFO] Reading cleaned Parquet records from {in_path}...")
    df = active_spark.read.parquet(str(in_path))
    input_count = df.count()
    print(f"[INFO] Read {input_count:,} cleaned case reports.")

    # Filter reports to those with at least one drug and at least one reaction
    valid_reports = df.filter(
        F.col("patient.drug").isNotNull()
        & (F.size(F.col("patient.drug")) > 0)
        & F.col("patient.reaction").isNotNull()
        & (F.size(F.col("patient.reaction")) > 0)
    )

    # 1. Explode patient.drug array -> 1 row per (case, drug)
    print("[INFO] Exploding patient.drug array...")
    drug_exploded = valid_reports.select(
        "case_id",
        "receive_date",
        "patient_age",
        "patient_sex",
        "patient_weight",
        "serious",
        "seriousnessdeath",
        "seriousnesshospitalization",
        "patient.reaction",
        F.explode("patient.drug").alias("drug_elem"),
    )

    # 2. Explode patient.reaction array -> 1 row per (case, drug, reaction)
    print("[INFO] Exploding patient.reaction array...")
    both_exploded = drug_exploded.select(
        "case_id",
        "receive_date",
        "patient_age",
        "patient_sex",
        "patient_weight",
        "serious",
        "seriousnessdeath",
        "seriousnesshospitalization",
        "drug_elem",
        F.explode("reaction").alias("reaction_elem"),
    )

    # Inspect nested drug schema dynamically to handle optional OpenFDA subfields
    drug_subfields = [f.name for f in both_exploded.schema["drug_elem"].dataType.fields]
    reaction_subfields = [f.name for f in both_exploded.schema["reaction_elem"].dataType.fields]

    # Check for openfda.generic_name
    has_generic_name = False
    if "openfda" in drug_subfields:
        openfda_type = [f.dataType for f in both_exploded.schema["drug_elem"].dataType.fields if f.name == "openfda"][0]
        if hasattr(openfda_type, "fields"):
            openfda_field_names = [f.name for f in openfda_type.fields]
            has_generic_name = "generic_name" in openfda_field_names

    if has_generic_name:
        raw_generic = F.col("drug_elem.openfda.generic_name").getItem(0)
    else:
        raw_generic = F.lit(None).cast("string")

    # Extract medicinalproduct
    if "medicinalproduct" in drug_subfields:
        raw_medicinal = F.col("drug_elem.medicinalproduct")
    else:
        raw_medicinal = F.lit(None).cast("string")

    # Extract drugindication
    if "drugindication" in drug_subfields:
        raw_indication = F.col("drug_elem.drugindication")
    else:
        raw_indication = F.lit(None).cast("string")

    # Extract reactionmeddrapt
    if "reactionmeddrapt" in reaction_subfields:
        raw_reaction = F.col("reaction_elem.reactionmeddrapt")
    else:
        raw_reaction = F.lit(None).cast("string")

    # Extract drugcharacterization
    if "drugcharacterization" in drug_subfields:
        raw_char = F.col("drug_elem.drugcharacterization").cast("string")
    else:
        raw_char = F.lit(None).cast("string")

    # Drug name normalization:
    # Use generic_name if available and valid; otherwise fallback to uppercase medicinalproduct
    clean_generic = F.trim(F.upper(raw_generic))
    clean_medicinal = F.trim(F.upper(raw_medicinal))

    normalized_drug = (
        F.when(clean_generic.isNotNull() & (clean_generic != "") & (clean_generic != "NULL"), clean_generic)
        .otherwise(clean_medicinal)
    )

    # Map drugcharacterization codes:
    # 1 -> Primary Suspect, 2 -> Concomitant, 3 -> Interacting
    drug_role = (
        F.when(raw_char == "1", "Primary Suspect")
        .when(raw_char == "2", "Concomitant")
        .when(raw_char == "3", "Interacting")
        .otherwise("Other")
    )

    # Clean reaction term (MedDRA Preferred Term uppercase)
    clean_reaction = F.trim(F.upper(raw_reaction))

    # Clean indication term
    clean_indication = F.trim(F.upper(raw_indication))

    # Binary outcome flags
    is_serious = F.when(F.col("serious") == "1", 1).otherwise(0)
    outcome_death = F.when(F.col("seriousnessdeath") == "1", 1).otherwise(0)
    outcome_hospitalization = F.when(F.col("seriousnesshospitalization") == "1", 1).otherwise(0)

    # Assemble final flattened DataFrame
    print("[INFO] Projecting and validating final target schema...")
    flattened_df = (
        both_exploded
        .withColumn("drug_name", normalized_drug)
        .withColumn("drug_role", drug_role)
        .withColumn("drug_indication", clean_indication)
        .withColumn("reaction", clean_reaction)
        .withColumn("is_serious", is_serious)
        .withColumn("outcome_death", outcome_death)
        .withColumn("outcome_hospitalization", outcome_hospitalization)
        .select(
            "case_id",
            "receive_date",
            "patient_age",
            "patient_sex",
            "patient_weight",
            "drug_name",
            "drug_role",
            "drug_indication",
            "reaction",
            "is_serious",
            "outcome_death",
            "outcome_hospitalization",
        )
        # Drop records missing critical drug or reaction values
        .filter(
            F.col("drug_name").isNotNull()
            & (F.col("drug_name") != "")
            & (F.col("drug_name") != "NULL")
            & F.col("reaction").isNotNull()
            & (F.col("reaction") != "")
            & (F.col("reaction") != "NULL")
        )
        # Deduplicate repeated (case, drug, reaction) combinations within same case
        .dropDuplicates(["case_id", "drug_name", "reaction"])
    )

    # Write flattened dataset to Parquet
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_uri = str(out_path)
    print(f"[INFO] Writing flattened records to Parquet at {out_uri}...")
    flattened_df.write.mode("overwrite").parquet(out_uri)

    # Verification and summary
    persisted_df = active_spark.read.parquet(out_uri)
    total_flat_records = persisted_df.count()

    print(f"[SUCCESS] Flattening complete.")
    print(f"    Clean case reports   : {input_count:,}")
    print(f"    Flattened pairs      : {total_flat_records:,}")
    print("\nFlattened Schema:")
    persisted_df.printSchema()

    print("\nSample Flattened Records:")
    persisted_df.select(
        "case_id", "drug_name", "drug_role", "reaction", "patient_sex", "patient_age", "outcome_death"
    ).show(10, truncate=False)

    return persisted_df


def main() -> None:
    """CLI entrypoint for ETL Step 03."""
    parser = argparse.ArgumentParser(
        description="ETL Step 03: Explode and flatten FAERS nested drug and reaction arrays.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument(
        "--input-dir",
        type=str,
        default=str(DEFAULT_INPUT_DIR),
        help="Path to folder containing cleaned Parquet files",
    )
    parser.add_argument(
        "--output-dir",
        type=str,
        default=str(DEFAULT_OUTPUT_DIR),
        help="Target folder for flattened Parquet files",
    )

    args = parser.parse_args()

    try:
        run_flatten(input_dir=args.input_dir, output_dir=args.output_dir)
    except Exception as err:
        print(f"[FATAL] ETL Step 03 failed: {err}")
        sys.exit(1)


if __name__ == "__main__":
    main()
