"""
Demographic Analysis Module for FAERS Adverse Event Surveillance.

Analyzes patient demographics per drug across all adverse event reports:
- Report volume
- Age summary statistics (mean, median approx, min, max)
- Sex distribution (Male, Female, Unknown)
- Age group stratification (Pediatric: 0-18, Adult: 18-65, Elderly: 65+)
"""

import sys
import logging
from typing import Optional, Union
from pyspark.sql import DataFrame, SparkSession
import pyspark.sql.functions as F
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    LongType,
    DoubleType,
)

logger = logging.getLogger("pharma_analytics.demographics")
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")


def get_spark_session(app_name: str = "FAERS-Demographic-Analysis") -> SparkSession:
    """Creates or retrieves an active SparkSession."""
    return (
        SparkSession.builder.appName(app_name)
        .config("spark.sql.shuffle.partitions", "200")
        .config("spark.sql.adaptive.enabled", "true")
        .enableHiveSupport()
        .getOrCreate()
    )


def analyze_demographics(
    spark: SparkSession,
    events_path: Union[str, DataFrame],
    output_path: Optional[str] = None,
) -> DataFrame:
    """
    Computes patient demographic profiles grouped by drug name.

    Args:
        spark: Active SparkSession.
        events_path: Path to parquet dataset or existing PySpark DataFrame.
        output_path: Optional destination path to write output parquet files.

    Returns:
        DataFrame containing demographic metrics per drug.
    """
    logger.info("Starting demographic analysis...")

    # 1. Load events
    if isinstance(events_path, str):
        logger.info(f"Loading events dataset from: {events_path}")
        df = spark.read.parquet(events_path)
    else:
        df = events_path

    cols = df.columns

    # Identify relevant columns
    drug_col = "drug_name" if "drug_name" in cols else "medicinalproduct"
    age_col = None
    for cand in ["patient_age", "age", "patientage"]:
        if cand in cols:
            age_col = cand
            break

    sex_col = None
    for cand in ["patient_sex", "sex", "patientsex", "gender"]:
        if cand in cols:
            sex_col = cand
            break

    report_col = None
    for cand in ["safetyreportid", "report_id", "id", "caseid"]:
        if cand in cols:
            report_col = cand
            break

    role_col = "drug_role" if "drug_role" in cols else ("role_cod" if "role_cod" in cols else None)

    # 2. Filter valid drugs and Primary Suspect role if present
    base_df = df.filter(F.col(drug_col).isNotNull())
    if role_col:
        base_df = base_df.filter(
            (F.col(role_col) == "Primary Suspect")
            | (F.upper(F.trim(F.col(role_col))) == "PRIMARY SUSPECT")
            | (F.upper(F.trim(F.col(role_col))) == "PS")
        )

    base_df = base_df.withColumn("drug_name", F.upper(F.trim(F.col(drug_col))))

    # 3. Clean and standardize Age (FAERS ages can have unvalidated numbers, filter 0-125)
    if age_col:
        clean_age = (
            F.when(
                (F.col(age_col).cast("double") >= 0.0) & (F.col(age_col).cast("double") <= 125.0),
                F.col(age_col).cast("double"),
            )
            .otherwise(F.lit(None))
            .alias("clean_age")
        )
    else:
        clean_age = F.lit(None).cast("double").alias("clean_age")

    # 4. Standardize Sex (FAERS standard: 1=Male, 2=Female, 0/UNK=Unknown)
    if sex_col:
        is_male_cond = (
            F.upper(F.col(sex_col)).isin("M", "MALE")
            | (F.col(sex_col).isin(1, "1"))
        )
        is_female_cond = (
            F.upper(F.col(sex_col)).isin("F", "FEMALE")
            | (F.col(sex_col).isin(2, "2"))
        )
    else:
        is_male_cond = F.lit(False)
        is_female_cond = F.lit(False)

    prep_df = base_df.select(
        *( [report_col] if report_col else [] ),
        "drug_name",
        clean_age,
        F.when(is_male_cond, F.lit(1)).otherwise(F.lit(0)).alias("is_male"),
        F.when(is_female_cond, F.lit(1)).otherwise(F.lit(0)).alias("is_female"),
        F.when(~(is_male_cond | is_female_cond), F.lit(1)).otherwise(F.lit(0)).alias("is_unknown_sex"),
    )

    # De-duplicate to patient/report level per drug if report_col is available
    if report_col:
        patient_level_df = prep_df.dropDuplicates([report_col, "drug_name"])
    else:
        patient_level_df = prep_df

    # 5. Define age group flags
    patient_level_df = patient_level_df.withColumn(
        "is_pediatric",
        F.when((F.col("clean_age") >= 0.0) & (F.col("clean_age") < 18.0), F.lit(1)).otherwise(F.lit(0)),
    ).withColumn(
        "is_adult",
        F.when((F.col("clean_age") >= 18.0) & (F.col("clean_age") < 65.0), F.lit(1)).otherwise(F.lit(0)),
    ).withColumn(
        "is_elderly",
        F.when(F.col("clean_age") >= 65.0, F.lit(1)).otherwise(F.lit(0)),
    )

    # 6. Aggregate per drug
    total_reps = F.count(F.lit(1)).cast("double")

    demo_summary = (
        patient_level_df.groupBy("drug_name")
        .agg(
            F.count(F.lit(1)).alias("total_reports"),
            F.round(F.avg("clean_age"), 2).alias("avg_age"),
            F.round(F.percentile_approx("clean_age", 0.5), 1).alias("median_age"),
            F.round(F.min("clean_age"), 1).alias("min_age"),
            F.round(F.max("clean_age"), 1).alias("max_age"),
            # Sex breakdown percentages
            F.round((F.sum("is_male") / total_reps) * 100.0, 2).alias("pct_male"),
            F.round((F.sum("is_female") / total_reps) * 100.0, 2).alias("pct_female"),
            F.round((F.sum("is_unknown_sex") / total_reps) * 100.0, 2).alias("pct_unknown"),
            # Age group distribution counts
            F.sum("is_pediatric").alias("pediatric_count"),
            F.sum("is_adult").alias("adult_count"),
            F.sum("is_elderly").alias("elderly_count"),
            # Age group distribution percentages
            F.round((F.sum("is_pediatric") / total_reps) * 100.0, 2).alias("pct_pediatric"),
            F.round((F.sum("is_adult") / total_reps) * 100.0, 2).alias("pct_adult"),
            F.round((F.sum("is_elderly") / total_reps) * 100.0, 2).alias("pct_elderly"),
        )
        .orderBy(F.col("total_reports").desc())
    )

    logger.info("Demographic aggregation complete.")

    if output_path:
        logger.info(f"Writing demographic metrics to parquet at: {output_path}")
        demo_summary.write.mode("overwrite").parquet(output_path)
        logger.info("Demographic analysis saved successfully.")

    return demo_summary


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python demographic_analysis.py <events_parquet_path> [output_parquet_path]")
        sys.exit(1)

    in_path = sys.argv[1]
    out_path = sys.argv[2] if len(sys.argv) > 2 else None

    spark_sess = get_spark_session()
    try:
        analyze_demographics(spark_sess, in_path, out_path)
    finally:
        spark_sess.stop()
