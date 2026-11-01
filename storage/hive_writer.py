"""
Apache Hive Storage Writer for FAERS Pharmacovigilance Data Warehouse.

Manages Hive metastore tables and parquet persistence for:
- Flattened adverse event reports (pharma_db.events)
- Statistical safety signals (pharma_db.signals)
- Drug-level surveillance summaries (pharma_db.drug_summary)
"""

import logging
from typing import Union
from pyspark.sql import DataFrame, SparkSession

logger = logging.getLogger("pharma_storage.hive_writer")
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

DATABASE_NAME = "pharma_db"


def create_hive_tables(spark: SparkSession) -> None:
    """
    Creates the Hive database 'pharma_db' and underlying analytical tables if they do not exist.

    Args:
        spark: Active SparkSession with Hive support enabled.
    """
    logger.info(f"Initializing Hive database '{DATABASE_NAME}' and DDL definitions...")

    # Create database
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {DATABASE_NAME}")

    # 1. Events Table DDL
    events_ddl = f"""
    CREATE TABLE IF NOT EXISTS {DATABASE_NAME}.events (
        safetyreportid STRING COMMENT 'Unique FAERS report identifier',
        received_date STRING COMMENT 'Report submission date (YYYYMMDD)',
        drug_name STRING COMMENT 'Standardized active ingredient / drug substance',
        medicinalproduct STRING COMMENT 'Reported commercial medicinal product name',
        drug_role STRING COMMENT 'Role of drug in event: Primary Suspect, Secondary Suspect, etc.',
        reaction STRING COMMENT 'MedDRA Preferred Term (PT) adverse reaction',
        patient_age DOUBLE COMMENT 'Standardized patient age in years',
        patient_sex STRING COMMENT 'Standardized patient sex: M, F, or UNK',
        is_fatal INT COMMENT '1 if event resulted in death, else 0',
        country STRING COMMENT 'Country of occurrence'
    )
    STORED AS PARQUET
    TBLPROPERTIES ('parquet.compression'='SNAPPY')
    """
    spark.sql(events_ddl)
    logger.info(f"Table '{DATABASE_NAME}.events' verified.")

    # 2. PRR Signals Table DDL
    signals_ddl = f"""
    CREATE TABLE IF NOT EXISTS {DATABASE_NAME}.signals (
        drug_name STRING COMMENT 'Standardized drug name',
        reaction STRING COMMENT 'Standardized MedDRA reaction term',
        a BIGINT COMMENT 'Count of reports with this drug AND this reaction',
        b BIGINT COMMENT 'Count of reports with this drug AND other reactions',
        c BIGINT COMMENT 'Count of reports with other drugs AND this reaction',
        d BIGINT COMMENT 'Count of reports with other drugs AND other reactions',
        drug_total BIGINT COMMENT 'Total reports for this drug (a + b)',
        reaction_total BIGINT COMMENT 'Total reports for this reaction (a + c)',
        total_reports BIGINT COMMENT 'Total reports cohort size (N)',
        prr DOUBLE COMMENT 'Proportional Reporting Ratio',
        chi_square DOUBLE COMMENT 'Pearson Chi-Square test statistic',
        is_signal BOOLEAN COMMENT 'True if PRR>=2, a>=3, and chi^2>=4',
        death_count BIGINT COMMENT 'Total fatal reports for this pair',
        fatality_rate DOUBLE COMMENT 'Mortality rate (death_count / a)'
    )
    STORED AS PARQUET
    TBLPROPERTIES ('parquet.compression'='SNAPPY')
    """
    spark.sql(signals_ddl)
    logger.info(f"Table '{DATABASE_NAME}.signals' verified.")

    # 3. Drug Summary Table DDL
    summary_ddl = f"""
    CREATE TABLE IF NOT EXISTS {DATABASE_NAME}.drug_summary (
        drug_name STRING COMMENT 'Standardized drug name',
        total_reports BIGINT COMMENT 'Total distinct adverse event reports',
        total_signals BIGINT COMMENT 'Total detected safety signals',
        avg_age DOUBLE COMMENT 'Average patient age',
        median_age DOUBLE COMMENT 'Approximate median patient age',
        min_age DOUBLE COMMENT 'Minimum patient age',
        max_age DOUBLE COMMENT 'Maximum patient age',
        pct_male DOUBLE COMMENT 'Percentage male patients',
        pct_female DOUBLE COMMENT 'Percentage female patients',
        pct_unknown DOUBLE COMMENT 'Percentage unknown sex',
        pct_pediatric DOUBLE COMMENT 'Percentage pediatric patients (0-18)',
        pct_adult DOUBLE COMMENT 'Percentage adult patients (18-65)',
        pct_elderly DOUBLE COMMENT 'Percentage elderly patients (65+)',
        total_deaths BIGINT COMMENT 'Total deaths reported for drug',
        fatality_rate DOUBLE COMMENT 'Overall drug mortality rate',
        top_reaction STRING COMMENT 'Most frequently reported adverse reaction',
        updated_at TIMESTAMP COMMENT 'Warehouse ingestion timestamp'
    )
    STORED AS PARQUET
    TBLPROPERTIES ('parquet.compression'='SNAPPY')
    """
    spark.sql(summary_ddl)
    logger.info(f"Table '{DATABASE_NAME}.drug_summary' verified.")
    logger.info("Hive DDL initialization complete.")


def write_events_to_hive(
    spark: SparkSession,
    events_df: Union[DataFrame, str],
) -> None:
    """
    Writes flattened adverse events dataset to Hive table 'pharma_db.events'.

    Args:
        spark: Active SparkSession.
        events_df: PySpark DataFrame or path to parquet files.
    """
    if isinstance(events_df, str):
        df = spark.read.parquet(events_df)
    else:
        df = events_df

    spark.sql(f"CREATE DATABASE IF NOT EXISTS {DATABASE_NAME}")
    table_name = f"{DATABASE_NAME}.events"

    logger.info(f"Writing {df.count():,} events to Hive table '{table_name}'...")
    df.write.mode("overwrite").format("parquet").saveAsTable(table_name)
    logger.info(f"Successfully saved events to '{table_name}'.")


def write_signals_to_hive(
    spark: SparkSession,
    signals_df: Union[DataFrame, str],
) -> None:
    """
    Writes PRR statistical safety signal results to Hive table 'pharma_db.signals'.

    Args:
        spark: Active SparkSession.
        signals_df: PySpark DataFrame or path to parquet files.
    """
    if isinstance(signals_df, str):
        df = spark.read.parquet(signals_df)
    else:
        df = signals_df

    spark.sql(f"CREATE DATABASE IF NOT EXISTS {DATABASE_NAME}")
    table_name = f"{DATABASE_NAME}.signals"

    logger.info(f"Writing {df.count():,} signal rows to Hive table '{table_name}'...")
    df.write.mode("overwrite").format("parquet").saveAsTable(table_name)
    logger.info(f"Successfully saved signals to '{table_name}'.")


def write_summary_to_hive(
    spark: SparkSession,
    summary_df: Union[DataFrame, str],
) -> None:
    """
    Writes aggregated drug demographic and safety summaries to Hive table 'pharma_db.drug_summary'.

    Args:
        spark: Active SparkSession.
        summary_df: PySpark DataFrame or path to parquet files.
    """
    if isinstance(summary_df, str):
        df = spark.read.parquet(summary_df)
    else:
        df = summary_df

    spark.sql(f"CREATE DATABASE IF NOT EXISTS {DATABASE_NAME}")
    table_name = f"{DATABASE_NAME}.drug_summary"

    logger.info(f"Writing {df.count():,} drug summaries to Hive table '{table_name}'...")
    df.write.mode("overwrite").format("parquet").saveAsTable(table_name)
    logger.info(f"Successfully saved drug summaries to '{table_name}'.")
