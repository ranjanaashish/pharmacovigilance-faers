"""
Predefined HiveQL Query Interface for FAERS Pharmacovigilance Warehouse.

Provides analytical queries against Hive tables in `pharma_db`:
- Drug-specific safety signals
- Global high-risk safety signals
- Drug epidemiological summaries
- Drug inventory lists
- Comparative multi-drug safety profiling
"""

import logging
from typing import List, Tuple
from pyspark.sql import DataFrame, SparkSession
import pyspark.sql.functions as F

logger = logging.getLogger("pharma_storage.hive_queries")
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

DATABASE_NAME = "pharma_db"


def get_drug_signals(
    spark: SparkSession,
    drug_name: str,
    limit: int = 100,
) -> DataFrame:
    """
    Retrieves statistically confirmed adverse safety signals for a specific drug.

    Args:
        spark: Active SparkSession.
        drug_name: Name of the drug substance (case-insensitive).
        limit: Maximum number of signals to retrieve.

    Returns:
        DataFrame of safety signals ordered by PRR and Chi-square.
    """
    cleaned_name = drug_name.strip().upper()
    logger.info(f"Querying Hive signals for drug: '{cleaned_name}'")

    query = f"""
    SELECT
        drug_name,
        reaction,
        a AS cases,
        b AS other_reaction_cases,
        c AS other_drug_reaction_cases,
        drug_total,
        reaction_total,
        prr,
        chi_square,
        is_signal,
        death_count,
        fatality_rate
    FROM {DATABASE_NAME}.signals
    WHERE UPPER(drug_name) = '{cleaned_name}'
      AND is_signal = true
    ORDER BY prr DESC, chi_square DESC, cases DESC
    LIMIT {limit}
    """
    try:
        return spark.sql(query)
    except Exception as e:
        logger.error(f"Error querying signals for '{cleaned_name}': {e}")
        # Return empty DataFrame with schema if table does not exist
        return spark.createDataFrame([], spark.table(f"{DATABASE_NAME}.signals").schema)


def get_top_signals(
    spark: SparkSession,
    limit: int = 50,
    min_cases: int = 5,
) -> DataFrame:
    """
    Retrieves global top safety signals across all monitored drugs.

    Args:
        spark: Active SparkSession.
        limit: Number of top signals to return (default: 50).
        min_cases: Minimum co-occurrence threshold 'a' to filter rare noise.

    Returns:
        DataFrame of top-ranked pharmacovigilance signals.
    """
    logger.info(f"Querying top {limit} global safety signals (min cases={min_cases})...")

    query = f"""
    SELECT
        drug_name,
        reaction,
        a AS cases,
        drug_total,
        reaction_total,
        prr,
        chi_square,
        death_count,
        fatality_rate
    FROM {DATABASE_NAME}.signals
    WHERE is_signal = true
      AND a >= {min_cases}
    ORDER BY prr DESC, chi_square DESC, cases DESC
    LIMIT {limit}
    """
    return spark.sql(query)


def get_drug_summary(
    spark: SparkSession,
    drug_name: str,
) -> DataFrame:
    """
    Retrieves the demographic and surveillance summary for a specific drug.

    Args:
        spark: Active SparkSession.
        drug_name: Name of the drug substance (case-insensitive).

    Returns:
        DataFrame with demographic breakdowns and safety totals.
    """
    cleaned_name = drug_name.strip().upper()
    logger.info(f"Querying drug summary for: '{cleaned_name}'")

    # Check if pharma_db.drug_summary exists
    table_exists = False
    try:
        spark.sql(f"DESCRIBE TABLE {DATABASE_NAME}.drug_summary")
        table_exists = True
    except Exception:
        table_exists = False

    if table_exists:
        query = f"""
        SELECT *
        FROM {DATABASE_NAME}.drug_summary
        WHERE UPPER(drug_name) = '{cleaned_name}'
        LIMIT 1
        """
        result = spark.sql(query)
        if result.count() > 0:
            return result

    # Fallback aggregation from signals and events if summary table is empty
    logger.info(f"Computing on-the-fly summary for '{cleaned_name}' from signals table...")
    fallback_query = f"""
    SELECT
        UPPER('{cleaned_name}') AS drug_name,
        MAX(drug_total) AS total_reports,
        COUNT(CASE WHEN is_signal = true THEN 1 END) AS total_signals,
        SUM(death_count) AS total_deaths,
        ROUND(SUM(death_count) / NULLIF(MAX(drug_total), 0), 4) AS fatality_rate
    FROM {DATABASE_NAME}.signals
    WHERE UPPER(drug_name) = '{cleaned_name}'
    GROUP BY drug_name
    """
    return spark.sql(fallback_query)


def get_drug_list(spark: SparkSession) -> List[str]:
    """
    Retrieves a sorted list of all unique drug names available in the data warehouse.

    Args:
        spark: Active SparkSession.

    Returns:
        Python list of unique drug names.
    """
    logger.info("Fetching complete drug catalog from Hive...")

    # Query signals table or summary table
    query = f"""
    SELECT DISTINCT drug_name
    FROM (
        SELECT drug_name FROM {DATABASE_NAME}.signals
        UNION
        SELECT drug_name FROM {DATABASE_NAME}.drug_summary
    ) combined
    WHERE drug_name IS NOT NULL AND drug_name != ''
    ORDER BY drug_name ASC
    """
    try:
        rows = spark.sql(query).collect()
        drugs = [row["drug_name"] for row in rows if row["drug_name"]]
        logger.info(f"Retrieved {len(drugs):,} distinct drugs.")
        return drugs
    except Exception as e:
        logger.warning(f"Could not fetch unified drug catalog, falling back to signals: {e}")
        try:
            rows = spark.sql(
                f"SELECT DISTINCT drug_name FROM {DATABASE_NAME}.signals WHERE drug_name IS NOT NULL ORDER BY drug_name"
            ).collect()
            return [row["drug_name"] for row in rows if row["drug_name"]]
        except Exception as e2:
            logger.error(f"Failed to fetch drug list: {e2}")
            return []


def compare_drugs(
    spark: SparkSession,
    drug1: str,
    drug2: str,
) -> Tuple[DataFrame, DataFrame]:
    """
    Retrieves adverse safety signals for two drugs for side-by-side comparative analysis.

    Args:
        spark: Active SparkSession.
        drug1: Primary comparative drug name.
        drug2: Secondary comparative drug name.

    Returns:
        Tuple of (DataFrame for drug1 signals, DataFrame for drug2 signals).
    """
    logger.info(f"Comparing safety profiles: '{drug1}' vs '{drug2}'")
    df1 = get_drug_signals(spark, drug1)
    df2 = get_drug_signals(spark, drug2)
    return df1, df2
