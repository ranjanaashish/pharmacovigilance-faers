"""
Fatality Analysis Module for FAERS Safety Signals.

Computes and ranks mortality risks across drugs, adverse reactions,
and statistically confirmed drug-reaction safety signals.
"""

import sys
import os
import logging
from typing import Optional, Union, Dict
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.window import Window
import pyspark.sql.functions as F
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    LongType,
    DoubleType,
    IntegerType,
)

logger = logging.getLogger("pharma_analytics.fatality")
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")


def get_spark_session(app_name: str = "FAERS-Fatality-Analysis") -> SparkSession:
    """Creates or retrieves an active SparkSession."""
    return (
        SparkSession.builder.appName(app_name)
        .config("spark.sql.shuffle.partitions", "200")
        .config("spark.sql.adaptive.enabled", "true")
        .enableHiveSupport()
        .getOrCreate()
    )


def analyze_fatality(
    spark: SparkSession,
    events_path: Union[str, DataFrame],
    signals_path: Union[str, DataFrame],
    output_path: Optional[str] = None,
) -> DataFrame:
    """
    Performs comprehensive fatality analysis:
    1. Overall death rate per drug (ranked).
    2. Overall death rate per adverse reaction (ranked).
    3. Death rate per (drug, reaction) pair among confirmed PRR safety signals (ranked).

    Args:
        spark: Active SparkSession.
        events_path: Path to parquet events dataset or existing DataFrame.
        signals_path: Path to parquet PRR signals dataset or existing DataFrame.
        output_path: Destination path for writing the primary ranked signal fatality dataset.

    Returns:
        DataFrame containing ranked safety signals with fatality statistics.
    """
    logger.info("Starting Fatality Analysis...")

    # 1. Load datasets
    if isinstance(events_path, str):
        logger.info(f"Loading events from: {events_path}")
        events_df = spark.read.parquet(events_path)
    else:
        events_df = events_path

    if isinstance(signals_path, str):
        logger.info(f"Loading signals from: {signals_path}")
        signals_df = spark.read.parquet(signals_path)
    else:
        signals_df = signals_path

    # Identify columns
    ev_cols = events_df.columns
    drug_col = "drug_name" if "drug_name" in ev_cols else "medicinalproduct"
    reaction_col = "reaction" if "reaction" in ev_cols else "reactionmeddrapt"
    role_col = "drug_role" if "drug_role" in ev_cols else ("role_cod" if "role_cod" in ev_cols else None)

    report_col = None
    for cand in ["safetyreportid", "report_id", "id", "caseid"]:
        if cand in ev_cols:
            report_col = cand
            break

    fatal_col = None
    for cand in ["is_fatal", "seriousnessdeath", "seriousness_death", "fatal", "death", "patient_death"]:
        if cand in ev_cols:
            fatal_col = cand
            break

    # 2. Filter Primary Suspects and standardize names
    clean_events = (
        events_df.filter(F.col(drug_col).isNotNull() & F.col(reaction_col).isNotNull())
        .withColumn("drug_name", F.upper(F.trim(F.col(drug_col))))
        .withColumn("reaction", F.upper(F.trim(F.col(reaction_col))))
    )

    if role_col:
        clean_events = clean_events.filter(
            (F.col(role_col) == "Primary Suspect")
            | (F.upper(F.trim(F.col(role_col))) == "PRIMARY SUSPECT")
            | (F.upper(F.trim(F.col(role_col))) == "PS")
        )

    # Standardize binary fatal flag
    if fatal_col:
        clean_events = clean_events.withColumn(
            "_fatal_flag",
            F.when(
                F.col(fatal_col).isin(1, "1", True, "Y", "yes", "YES"),
                F.lit(1),
            ).otherwise(F.lit(0)),
        )
    else:
        clean_events = clean_events.withColumn("_fatal_flag", F.lit(0))

    if not report_col:
        report_col = "_event_row_id"
        clean_events = clean_events.withColumn(report_col, F.monotonically_increasing_id())

    # 3. Overall Fatality Rate per Drug (Ranked)
    drug_patient_level = clean_events.dropDuplicates([report_col, "drug_name"])
    drug_fatality = (
        drug_patient_level.groupBy("drug_name")
        .agg(
            F.count(report_col).alias("total_drug_reports"),
            F.sum("_fatal_flag").alias("total_drug_deaths"),
        )
        .withColumn(
            "drug_fatality_rate",
            F.when(
                F.col("total_drug_reports") > 0,
                F.round(
                    F.col("total_drug_deaths").cast("double") / F.col("total_drug_reports").cast("double"),
                    4,
                ),
            ).otherwise(F.lit(0.0)),
        )
        .withColumn(
            "drug_mortality_rank",
            F.row_number().over(
                Window.orderBy(
                    F.col("drug_fatality_rate").desc(),
                    F.col("total_drug_deaths").desc(),
                )
            ),
        )
    )

    # 4. Overall Fatality Rate per Reaction (Ranked)
    reaction_patient_level = clean_events.dropDuplicates([report_col, "reaction"])
    reaction_fatality = (
        reaction_patient_level.groupBy("reaction")
        .agg(
            F.count(report_col).alias("total_reaction_reports"),
            F.sum("_fatal_flag").alias("total_reaction_deaths"),
        )
        .withColumn(
            "reaction_fatality_rate",
            F.when(
                F.col("total_reaction_reports") > 0,
                F.round(
                    F.col("total_reaction_deaths").cast("double")
                    / F.col("total_reaction_reports").cast("double"),
                    4,
                ),
            ).otherwise(F.lit(0.0)),
        )
        .withColumn(
            "reaction_mortality_rank",
            F.row_number().over(
                Window.orderBy(
                    F.col("reaction_fatality_rate").desc(),
                    F.col("total_reaction_deaths").desc(),
                )
            ),
        )
    )

    # 5. Fatality per (Drug, Reaction) among Confirmed PRR Signals
    # Standardize signals dataframe
    sig_cols = signals_df.columns
    clean_signals = signals_df.withColumn("drug_name", F.upper(F.trim(F.col("drug_name")))).withColumn(
        "reaction", F.upper(F.trim(F.col("reaction")))
    )

    if "is_signal" in sig_cols:
        signals_only = clean_signals.filter(F.col("is_signal") == True)  # noqa: E712
    else:
        signals_only = clean_signals.filter(
            (F.col("prr") >= 2.0) & (F.col("a") >= 3) & (F.col("chi_square") >= 4.0)
        )

    # Compute pair-level death counts from events if not already present in signals
    if "death_count" in sig_cols and "fatality_rate" in sig_cols:
        pair_fatal_signals = signals_only
    else:
        pair_deaths = (
            clean_events.groupBy("drug_name", "reaction")
            .agg(
                F.countDistinct(
                    F.when(F.col("_fatal_flag") == 1, F.col(report_col))
                ).alias("death_count")
            )
        )
        pair_fatal_signals = signals_only.join(
            pair_deaths, on=["drug_name", "reaction"], how="left"
        ).fillna({"death_count": 0})
        pair_fatal_signals = pair_fatal_signals.withColumn(
            "fatality_rate",
            F.when(
                F.col("a") > 0,
                F.round(F.col("death_count").cast("double") / F.col("a").cast("double"), 4),
            ).otherwise(F.lit(0.0)),
        )

    # Join with overall drug baseline fatality for clinical context
    ranked_signals = (
        pair_fatal_signals.join(
            drug_fatality.select("drug_name", "drug_fatality_rate", "total_drug_reports"),
            on="drug_name",
            how="left",
        )
        .withColumn(
            "signal_fatality_rank",
            F.row_number().over(
                Window.orderBy(
                    F.col("fatality_rate").desc(),
                    F.col("death_count").desc(),
                    F.col("prr").desc(),
                )
            ),
        )
        .select(
            "signal_fatality_rank",
            "drug_name",
            "reaction",
            F.col("a").alias("cases"),
            "death_count",
            "fatality_rate",
            "drug_fatality_rate",
            "prr",
            "chi_square",
            "total_drug_reports",
        )
        .orderBy(F.col("signal_fatality_rank").asc())
    )

    # Log summary statistics
    top_signals_count = ranked_signals.count()
    logger.info(f"Fatality Analysis completed. Evaluated {top_signals_count} confirmed safety signals.")

    # 6. Save datasets if output_path is provided
    if output_path:
        # Write main ranked signals table
        logger.info(f"Writing ranked signal fatality table to: {output_path}")
        ranked_signals.write.mode("overwrite").parquet(output_path)

        # Write auxiliary ranked drug and reaction tables
        drug_out = f"{output_path}_by_drug"
        reaction_out = f"{output_path}_by_reaction"
        logger.info(f"Writing drug fatality rankings to: {drug_out}")
        drug_fatality.write.mode("overwrite").parquet(drug_out)
        logger.info(f"Writing reaction fatality rankings to: {reaction_out}")
        reaction_fatality.write.mode("overwrite").parquet(reaction_out)

    return ranked_signals


if __name__ == "__main__":
    if len(sys.argv) < 3:
        print("Usage: python fatality_analysis.py <events_path> <signals_path> [output_path]")
        sys.exit(1)

    ev_path = sys.argv[1]
    sig_path = sys.argv[2]
    out_path = sys.argv[3] if len(sys.argv) > 3 else None

    spark_sess = get_spark_session()
    try:
        analyze_fatality(spark_sess, ev_path, sig_path, out_path)
    finally:
        spark_sess.stop()
