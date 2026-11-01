"""
PRR (Proportional Reporting Ratio) Calculator for FAERS Safety Signal Detection.

This module implements the Evans et al. (2001) Disproportionality Analysis algorithm
using PySpark DataFrames API for high-throughput pharmacovigilance adverse event surveillance.

Formulas:
    Contingency 2x2 Table:
                 Reaction Y    Other Reactions    Total
    Drug X           a                b            a + b (drug_total)
    Other Drugs      c                d            c + d
    Total          a + c            b + d            N

    PRR = (a / (a + b)) / (c / (c + d))
    chi_square = N * (a*d - b*c)^2 / ((a+b)*(c+d)*(a+c)*(b+d))

Signal Threshold:
    PRR >= 2.0 AND a >= 3 AND chi_square >= 4.0
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
    BooleanType,
)

logger = logging.getLogger("pharma_analytics.prr")
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")


def get_spark_session(app_name: str = "FAERS-PRR-Calculator") -> SparkSession:
    """Creates or retrieves an active SparkSession configured for Hive and memory efficiency."""
    return (
        SparkSession.builder.appName(app_name)
        .config("spark.sql.shuffle.partitions", "200")
        .config("spark.sql.adaptive.enabled", "true")
        .enableHiveSupport()
        .getOrCreate()
    )


def compute_prr(
    spark: SparkSession,
    events_path: Union[str, DataFrame],
    output_path: Optional[str] = None,
) -> DataFrame:
    """
    Computes Proportional Reporting Ratio (PRR) and Chi-square statistics for all
    (drug_name, reaction) pairs in the provided FAERS events dataset.

    Args:
        spark: Active SparkSession.
        events_path: Path to parquet events dataset, or an existing PySpark DataFrame.
        output_path: Optional destination path to write output parquet files.

    Returns:
        DataFrame containing PRR contingency metrics, test statistics, and signal flags.
    """
    logger.info("Starting PRR computation...")

    # 1. Load events data
    if isinstance(events_path, str):
        logger.info(f"Reading events from parquet: {events_path}")
        df = spark.read.parquet(events_path)
    else:
        df = events_path

    # Identify primary column names flexibly
    cols = df.columns
    drug_col = "drug_name" if "drug_name" in cols else "medicinalproduct"
    reaction_col = "reaction" if "reaction" in cols else "reactionmeddrapt"
    role_col = "drug_role" if "drug_role" in cols else "role_cod"

    # Identify report ID column (if available for report-level distinct counting)
    report_col = None
    for cand in ["safetyreportid", "report_id", "id", "caseid"]:
        if cand in cols:
            report_col = cand
            break

    # Identify fatality column
    fatal_col = None
    for cand in ["is_fatal", "seriousnessdeath", "seriousness_death", "fatal", "death", "patient_death"]:
        if cand in cols:
            fatal_col = cand
            break

    logger.info(
        f"Columns detected -> Drug: '{drug_col}', Reaction: '{reaction_col}', "
        f"Role: '{role_col}', ReportID: '{report_col}', Fatal: '{fatal_col}'"
    )

    # 2. Filter to Primary Suspect drugs only
    # FAERS designates Primary Suspect as 'Primary Suspect' or code 'PS'
    filtered_df = (
        df.filter(
            (F.col(role_col) == "Primary Suspect")
            | (F.upper(F.trim(F.col(role_col))) == "PRIMARY SUSPECT")
            | (F.upper(F.trim(F.col(role_col))) == "PS")
        )
        .filter(F.col(drug_col).isNotNull() & F.col(reaction_col).isNotNull())
        .withColumn("drug_name", F.upper(F.trim(F.col(drug_col))))
        .withColumn("reaction", F.upper(F.trim(F.col(reaction_col))))
    )

    # Add binary fatal flag
    if fatal_col:
        filtered_df = filtered_df.withColumn(
            "_fatal_flag",
            F.when(
                F.col(fatal_col).isin(1, "1", True, "Y", "yes", "YES"),
                F.lit(1),
            ).otherwise(F.lit(0)),
        )
    else:
        filtered_df = filtered_df.withColumn("_fatal_flag", F.lit(0))

    # 3. Compute Total Report Count N
    if report_col:
        N = filtered_df.select(report_col).distinct().count()
    else:
        # Fallback to row count if no unique report ID is provided
        report_col = "_auto_report_id"
        filtered_df = filtered_df.withColumn(report_col, F.monotonically_increasing_id())
        N = filtered_df.count()

    logger.info(f"Total distinct Primary Suspect reports (N): {N}")

    if N == 0:
        logger.warning("No Primary Suspect events found in dataset. Returning empty DataFrame.")
        empty_schema = StructType([
            StructField("drug_name", StringType(), False),
            StructField("reaction", StringType(), False),
            StructField("a", LongType(), False),
            StructField("b", LongType(), False),
            StructField("c", LongType(), False),
            StructField("d", LongType(), False),
            StructField("drug_total", LongType(), False),
            StructField("reaction_total", LongType(), False),
            StructField("total_reports", LongType(), False),
            StructField("prr", DoubleType(), True),
            StructField("chi_square", DoubleType(), True),
            StructField("is_signal", BooleanType(), False),
            StructField("death_count", LongType(), False),
            StructField("fatality_rate", DoubleType(), True),
        ])
        return spark.createDataFrame([], empty_schema)

    # 4. Compute drug_total (a + b) per drug
    drug_totals = (
        filtered_df.select(report_col, "drug_name")
        .distinct()
        .groupBy("drug_name")
        .agg(F.count(report_col).alias("drug_total"))
    )

    # 5. Compute reaction_total (a + c) per reaction
    reaction_totals = (
        filtered_df.select(report_col, "reaction")
        .distinct()
        .groupBy("reaction")
        .agg(F.count(report_col).alias("reaction_total"))
    )

    # 6. Compute co-occurrence count 'a' and fatal reports per (drug, reaction) pair
    pair_counts = (
        filtered_df.groupBy("drug_name", "reaction")
        .agg(
            F.countDistinct(report_col).alias("a"),
            F.countDistinct(
                F.when(F.col("_fatal_flag") == 1, F.col(report_col))
            ).alias("death_count"),
        )
    )

    # 7. Join and construct 2x2 contingency table elements
    contingency_df = (
        pair_counts.join(drug_totals, on="drug_name", how="inner")
        .join(reaction_totals, on="reaction", how="inner")
        .withColumn("b", F.col("drug_total") - F.col("a"))
        .withColumn("c", F.col("reaction_total") - F.col("a"))
        .withColumn(
            "d",
            F.lit(N) - F.col("drug_total") - F.col("reaction_total") + F.col("a"),
        )
        .withColumn("total_reports", F.lit(N))
    )

    # 8. Handle division by zero
    # Prevent division by zero when:
    # - (c + d) == 0 (no other drugs in the study)
    # - c == 0 (denominator of PRR: c / (c+d) = 0)
    # - (a + b) == 0
    # - (a + c) == 0
    # - (b + d) == 0
    valid_df = contingency_df.filter(
        (F.col("c") > 0)
        & ((F.col("c") + F.col("d")) > 0)
        & ((F.col("a") + F.col("b")) > 0)
        & ((F.col("a") + F.col("c")) > 0)
        & ((F.col("b") + F.col("d")) > 0)
    )

    # 9. Compute PRR and Chi-square
    # PRR = (a / (a + b)) / (c / (c + d)) = (a * (c + d)) / (c * (a + b))
    a_col = F.col("a").cast("double")
    b_col = F.col("b").cast("double")
    c_col = F.col("c").cast("double")
    d_col = F.col("d").cast("double")
    n_col = F.lit(float(N))

    prr_expr = (a_col * (c_col + d_col)) / (c_col * (a_col + b_col))

    # Chi-square with Yates' correction or standard Pearson chi-square:
    # Prompt standard: chi_square = N * (ad - bc)^2 / ((a+b)(c+d)(a+c)(b+d))
    ad_minus_bc = (a_col * d_col) - (b_col * c_col)
    chi_numerator = n_col * (ad_minus_bc * ad_minus_bc)
    chi_denominator = (
        (a_col + b_col) * (c_col + d_col) * (a_col + c_col) * (b_col + d_col)
    )
    chi_square_expr = chi_numerator / chi_denominator

    # Fatality rate per pair
    fatality_rate_expr = F.when(
        F.col("a") > 0,
        F.round(F.col("death_count").cast("double") / a_col, 4),
    ).otherwise(F.lit(0.0))

    results_df = (
        valid_df
        .withColumn("prr", F.round(prr_expr, 4))
        .withColumn("chi_square", F.round(chi_square_expr, 4))
        .withColumn("fatality_rate", fatality_rate_expr)
        .withColumn(
            "is_signal",
            (F.col("prr") >= 2.0) & (F.col("a") >= 3) & (F.col("chi_square") >= 4.0),
        )
    )

    final_df = results_df.select(
        "drug_name",
        "reaction",
        "a",
        "b",
        "c",
        "d",
        "drug_total",
        "reaction_total",
        "total_reports",
        "prr",
        "chi_square",
        "is_signal",
        "death_count",
        "fatality_rate",
    ).orderBy(
        F.col("is_signal").desc(),
        F.col("prr").desc(),
        F.col("chi_square").desc(),
    )

    # 10. Summary statistics and persistence
    # Use checkpointing / cache if output_path is requested
    final_df = final_df.cache()
    total_pairs = final_df.count()
    signals_detected = final_df.filter(F.col("is_signal")).count()

    print("\n" + "=" * 60)
    print("           PHARMACOVIGILANCE PRR SIGNAL DETECTION           ")
    print("=" * 60)
    print(f" Total Drug-Reaction Pairs Evaluated : {total_pairs:,}")
    print(f" Statistically Significant Signals   : {signals_detected:,}")
    print(
        f" Signal Criteria Applied              : PRR >= 2.0 AND cases (a) >= 3 AND chi^2 >= 4.0"
    )
    print("=" * 60 + "\n")

    if output_path:
        logger.info(f"Writing PRR results to parquet at: {output_path}")
        final_df.write.mode("overwrite").parquet(output_path)
        logger.info("PRR results successfully persisted.")

    return final_df


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python prr_calculator.py <events_parquet_path> [output_parquet_path]")
        sys.exit(1)

    input_path = sys.argv[1]
    out_path = sys.argv[2] if len(sys.argv) > 2 else None

    spark_sess = get_spark_session()
    try:
        compute_prr(spark_sess, input_path, out_path)
    finally:
        spark_sess.stop()
