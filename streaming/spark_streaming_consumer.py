"""PySpark Structured Streaming Consumer for FAERS Adverse Event Stream.

Consumes real-time adverse event JSON messages from Apache Kafka, deserializes
the nested schema, applies the standard FAERS ETL cleaning, normalization,
and flattening pipeline, and sinks the processed events into Parquet storage
with fault-tolerant checkpointing.

Processing includes:
- Schema enforcement for OpenFDA nested report structures
- Sex code mapping: 1 -> Male, 2 -> Female, 0 / Other -> Unknown
- Age normalization across temporal units (decade, year, month, week, day, hour)
- Drug name normalization prioritizing generic name with brand fallback
- Drug characterization role mapping (Primary Suspect, Secondary Suspect, etc.)
- MedDRA Preferred Term (PT) reaction extraction and sanitization
- Fatality and serious outcome flag derivation
- Append mode Parquet sink with configurable 30-second micro-batch trigger
"""

import argparse
import logging
import os
import sys
from pathlib import Path
from typing import Optional

# Ensure project root is available on sys.path
PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from pyspark.sql import DataFrame, SparkSession
import pyspark.sql.functions as F
from pyspark.sql.types import (
    ArrayType,
    DoubleType,
    StringType,
    StructField,
    StructType,
)

# Configuration helpers
try:
    from config.spark_config import get_spark_session
except ImportError:
    def get_spark_session(app_name: str = "FAERS-Streaming-Consumer") -> SparkSession:
        return (
            SparkSession.builder.appName(app_name)
            .master("local[*]")
            .config("spark.sql.shuffle.partitions", "8")
            .getOrCreate()
        )

try:
    from config.kafka_config import KAFKA_BROKER
except ImportError:
    KAFKA_BROKER = os.getenv("KAFKA_BROKER", "localhost:9092")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("faers.streaming.consumer")


def get_faers_json_schema() -> StructType:
    """Define the structured PySpark schema matching OpenFDA FAERS JSON reports.

    Returns:
        StructType representation of raw FAERS adverse event reports.
    """
    drug_schema = StructType([
        StructField("medicinalproduct", StringType(), True),
        StructField("drugcharacterization", StringType(), True),
        StructField("drugdosagetext", StringType(), True),
        StructField("drugindication", StringType(), True),
        StructField(
            "openfda",
            StructType([
                StructField("generic_name", ArrayType(StringType()), True),
                StructField("brand_name", ArrayType(StringType()), True),
                StructField("substance_name", ArrayType(StringType()), True),
                StructField("manufacturer_name", ArrayType(StringType()), True),
            ]),
            True,
        ),
    ])

    reaction_schema = StructType([
        StructField("reactionmeddrapt", StringType(), True),
        StructField("reactionoutcome", StringType(), True),
    ])

    patient_schema = StructType([
        StructField("patientonsetage", StringType(), True),
        StructField("patientonsetageunit", StringType(), True),
        StructField("patientsex", StringType(), True),
        StructField("patientweight", StringType(), True),
        StructField("drug", ArrayType(drug_schema), True),
        StructField("reaction", ArrayType(reaction_schema), True),
    ])

    report_schema = StructType([
        StructField("safetyreportid", StringType(), True),
        StructField("safetyreportversion", StringType(), True),
        StructField("receivedate", StringType(), True),
        StructField("occurcountry", StringType(), True),
        StructField("serious", StringType(), True),
        StructField("seriousnessdeath", StringType(), True),
        StructField("seriousnesshospitalization", StringType(), True),
        StructField("seriousnesslifethreatening", StringType(), True),
        StructField("seriousnessdisabling", StringType(), True),
        StructField("patient", patient_schema, True),
    ])

    return report_schema


def normalize_sex_udf_column() -> F.Column:
    """SQL expression mapping raw FAERS sex codes to standard descriptors.

    Codes:
        '1' -> 'Male'
        '2' -> 'Female'
        '0' / other / null -> 'Unknown'
    """
    return (
        F.when(F.col("patient_sex_raw") == "1", F.lit("Male"))
        .when(F.col("patient_sex_raw") == "2", F.lit("Female"))
        .otherwise(F.lit("Unknown"))
    )


def normalize_age_udf_column() -> F.Column:
    """SQL expression converting multi-unit age values into standardized decimal years.

    Unit codes (ICH E2B standard):
        '800' or DECADE: age * 10.0
        '801' or YEAR: age * 1.0
        '802' or MONTH: age / 12.0
        '803' or WEEK: age / 52.0
        '804' or DAY: age / 365.25
        '805' or HOUR: age / 8760.0
    """
    raw_age = F.col("patient_age_raw").cast(DoubleType())
    unit = F.upper(F.trim(F.col("patient_age_unit_raw")))

    age_in_years = (
        F.when(unit.isin("800", "DEC", "DECADE", "DECADES"), raw_age * 10.0)
        .when(unit.isin("801", "YR", "YEAR", "YEARS", "YRS"), raw_age * 1.0)
        .when(unit.isin("802", "MON", "MONTH", "MONTHS", "MTH"), raw_age / 12.0)
        .when(unit.isin("803", "WK", "WEEK", "WEEKS"), raw_age / 52.0)
        .when(unit.isin("804", "DY", "DAY", "DAYS"), raw_age / 365.25)
        .when(unit.isin("805", "HR", "HOUR", "HOURS"), raw_age / 8760.0)
        .when(raw_age.isNotNull() & (raw_age >= 0) & (raw_age <= 125), raw_age)  # Default assume years if unit missing
        .otherwise(F.lit(None).cast(DoubleType()))
    )

    # Sanity filter: ages beyond 0-125 years are considered biologically implausible/erroneous
    return F.when((age_in_years >= 0.0) & (age_in_years <= 125.0), F.round(age_in_years, 2)).otherwise(
        F.lit(None).cast(DoubleType())
    )


def normalize_drug_role_column() -> F.Column:
    """SQL expression classifying drug role into ICH standard suspect categories.

    Codes:
        '1' -> 'Primary Suspect'
        '2' -> 'Secondary Suspect'
        '3' -> 'Concomitant'
        '4' -> 'Interacting'
    """
    role = F.upper(F.trim(F.col("raw_drug_role")))
    return (
        F.when(role.isin("1", "PS", "PRIMARY SUSPECT"), F.lit("Primary Suspect"))
        .when(role.isin("2", "SS", "SECONDARY SUSPECT"), F.lit("Secondary Suspect"))
        .when(role.isin("3", "C", "CONCOMITANT"), F.lit("Concomitant"))
        .when(role.isin("4", "I", "INTERACTING"), F.lit("Interacting"))
        .otherwise(F.lit("Concomitant"))
    )


def transform_streaming_events(stream_df: DataFrame) -> DataFrame:
    """Transform raw Kafka message payload into cleaned, flattened FAERS events.

    Args:
        stream_df: Streaming DataFrame read from Kafka (containing 'value' bytes).

    Returns:
        Flattened and normalized DataFrame ready for analytical persistence.
    """
    schema = get_faers_json_schema()

    # 1. Parse JSON payload
    parsed_df = (
        stream_df.selectExpr("CAST(value AS STRING) AS json_str")
        .withColumn("data", F.from_json(F.col("json_str"), schema))
        .select("data.*")
        .filter(F.col("safetyreportid").isNotNull())
    )

    # 2. Extract report and patient header attributes
    header_df = parsed_df.select(
        F.col("safetyreportid").alias("report_id"),
        F.col("safetyreportversion").alias("report_version"),
        F.col("receivedate").alias("received_date"),
        F.coalesce(F.col("occurcountry"), F.lit("UNKNOWN")).alias("country"),
        (F.col("serious") == "1").alias("is_serious"),
        (F.col("seriousnessdeath") == "1").alias("is_fatal"),
        (F.col("seriousnesshospitalization") == "1").alias("is_hospitalization"),
        (F.col("seriousnesslifethreatening") == "1").alias("is_life_threatening"),
        F.col("patient.patientsex").alias("patient_sex_raw"),
        F.col("patient.patientonsetage").alias("patient_age_raw"),
        F.col("patient.patientonsetageunit").alias("patient_age_unit_raw"),
        F.col("patient.drug").alias("drugs_array"),
        F.col("patient.reaction").alias("reactions_array"),
    )

    # 3. Apply demographic normalization
    demographics_df = (
        header_df.withColumn("sex", normalize_sex_udf_column())
        .withColumn("age_years", normalize_age_udf_column())
        .drop("patient_sex_raw", "patient_age_raw", "patient_age_unit_raw")
    )

    # 4. Explode nested drugs array
    exploded_drugs_df = (
        demographics_df.filter(
            F.col("drugs_array").isNotNull() & (F.size(F.col("drugs_array")) > 0)
        )
        .withColumn("drug_struct", F.explode("drugs_array"))
        .withColumn(
            "generic_name_cand",
            F.element_at(F.col("drug_struct.openfda.generic_name"), 1),
        )
        .withColumn(
            "medicinal_name_cand",
            F.col("drug_struct.medicinalproduct"),
        )
        # Drug normalization: generic preferred, uppercase, trimmed
        .withColumn(
            "drug_name",
            F.upper(F.trim(F.coalesce(F.col("generic_name_cand"), F.col("medicinal_name_cand")))),
        )
        .withColumn("raw_drug_role", F.col("drug_struct.drugcharacterization"))
        .withColumn("drug_role", normalize_drug_role_column())
        .withColumn("indication", F.upper(F.trim(F.col("drug_struct.drugindication"))))
        .filter(F.col("drug_name").isNotNull() & (F.length(F.col("drug_name")) > 0))
        .drop("drugs_array", "drug_struct", "generic_name_cand", "medicinal_name_cand", "raw_drug_role")
    )

    # 5. Explode nested reactions array to create 1-to-1 (drug, reaction) pairs
    flattened_df = (
        exploded_drugs_df.filter(
            F.col("reactions_array").isNotNull() & (F.size(F.col("reactions_array")) > 0)
        )
        .withColumn("reaction_struct", F.explode("reactions_array"))
        .withColumn(
            "reaction",
            F.upper(F.trim(F.col("reaction_struct.reactionmeddrapt"))),
        )
        .withColumn("reaction_outcome", F.col("reaction_struct.reactionoutcome"))
        .filter(F.col("reaction").isNotNull() & (F.length(F.col("reaction")) > 0))
        .drop("reactions_array", "reaction_struct")
    )

    # 6. Final select aligning with ETL flattened schema
    final_stream = flattened_df.select(
        "report_id",
        "report_version",
        "received_date",
        "country",
        "sex",
        "age_years",
        "drug_name",
        "drug_role",
        "indication",
        "reaction",
        "reaction_outcome",
        "is_serious",
        "is_fatal",
        "is_hospitalization",
        "is_life_threatening",
    )

    return final_stream


def run_streaming_consumer(
    bootstrap_servers: str,
    topic: str = "faers-raw-events",
    output_path: str = "data/streaming_output/events_parquet",
    checkpoint_dir: str = "data/streaming_output/checkpoint",
    trigger_interval: str = "30 seconds",
    console_mode: bool = False,
    spark: Optional[SparkSession] = None,
) -> None:
    """Start the PySpark Structured Streaming consumer query.

    Args:
        bootstrap_servers: Kafka broker addresses.
        topic: Source Kafka topic.
        output_path: Destination folder for output Parquet files.
        checkpoint_dir: Directory for stream checkpointing and offset commits.
        trigger_interval: Processing time interval trigger (default '30 seconds').
        console_mode: If True, writes formatted stream output to standard output instead of Parquet.
        spark: Existing SparkSession, or None to initialize a new session.
    """
    spark_session = spark or get_spark_session(app_name="FAERS-Structured-Streaming-Consumer")
    spark_session.sparkContext.setLogLevel("WARN")

    logger.info(f"Connecting Structured Streaming to Kafka topic '{topic}' at '{bootstrap_servers}'...")

    # Read from Kafka
    raw_kafka_df = (
        spark_session.readStream.format("kafka")
        .option("kafka.bootstrap.servers", bootstrap_servers)
        .option("subscribe", topic)
        .option("startingOffsets", "earliest")
        .option("failOnDataLoss", "false")
        .load()
    )

    # Apply data transformations
    processed_df = transform_streaming_events(raw_kafka_df)

    # Configure destination sink
    if console_mode:
        logger.info("Starting streaming sink to CONSOLE...")
        query = (
            processed_df.writeStream.outputMode("append")
            .format("console")
            .option("truncate", "false")
            .trigger(processingTime=trigger_interval)
            .start()
        )
    else:
        logger.info(f"Starting streaming sink to PARQUET at '{output_path}' with checkpoint at '{checkpoint_dir}'...")
        Path(output_path).mkdir(parents=True, exist_ok=True)
        Path(checkpoint_dir).mkdir(parents=True, exist_ok=True)

        query = (
            processed_df.writeStream.outputMode("append")
            .format("parquet")
            .option("path", output_path)
            .option("checkpointLocation", checkpoint_dir)
            .trigger(processingTime=trigger_interval)
            .start()
        )

    logger.info(f"Streaming query started with ID: {query.id}. Trigger interval: {trigger_interval}")
    print("\n" + "=" * 65)
    print("       FAERS STRUCTURED STREAMING CONSUMER ACTIVE       ")
    print("=" * 65)
    print(f" Source Topic       : {topic}")
    print(f" Micro-batch Trigger: {trigger_interval}")
    print(f" Destination Sink   : {'Console' if console_mode else output_path}")
    print(f" Checkpoint Location: {checkpoint_dir}")
    print(" Press Ctrl+C in terminal to stop streaming consumption.")
    print("=" * 65 + "\n")

    try:
        query.awaitTermination()
    except KeyboardInterrupt:
        logger.info("Stopping streaming query gracefully upon user request...")
        query.stop()
        logger.info("Streaming query stopped successfully.")


def main() -> None:
    """CLI entrypoint for Spark Structured Streaming Consumer."""
    parser = argparse.ArgumentParser(
        description="PySpark Structured Streaming consumer for FAERS Kafka events.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument(
        "--bootstrap-servers",
        type=str,
        default=KAFKA_BROKER,
        help="Kafka bootstrap server host:port",
    )
    parser.add_argument(
        "--topic",
        type=str,
        default="faers-raw-events",
        help="Kafka topic to consume from",
    )
    parser.add_argument(
        "--output-path",
        type=str,
        default=str(PROJECT_ROOT / "data" / "streaming_output" / "events_parquet"),
        help="Destination directory for processed Parquet events",
    )
    parser.add_argument(
        "--checkpoint-dir",
        type=str,
        default=str(PROJECT_ROOT / "data" / "streaming_output" / "checkpoint"),
        help="Directory for stream checkpointing metadata",
    )
    parser.add_argument(
        "--trigger-interval",
        type=str,
        default="30 seconds",
        help="Micro-batch trigger duration (e.g., '10 seconds', '30 seconds', '1 minute')",
    )
    parser.add_argument(
        "--console-output",
        action="store_true",
        help="Direct micro-batch stream output to console instead of Parquet",
    )

    args = parser.parse_args()

    run_streaming_consumer(
        bootstrap_servers=args.bootstrap_servers,
        topic=args.topic,
        output_path=args.output_path,
        checkpoint_dir=args.checkpoint_dir,
        trigger_interval=args.trigger_interval,
        console_mode=args.console_output,
    )


if __name__ == "__main__":
    main()
