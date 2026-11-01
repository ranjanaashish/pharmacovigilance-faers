"""Unit tests for FAERS data cleaning, normalization, and deduplication logic.

Validates core transformation rules using PySpark DataFrames API:
- Sex code mapping: 1 -> Male, 2 -> Female, 0 / null / invalid -> Unknown
- Age normalization across temporal units:
    * Decade (800 / DEC) -> years * 10
    * Year (801 / YR) -> years * 1
    * Month (802 / MON) -> months / 12
    * Week (803 / WK) -> weeks / 52
    * Day (804 / DY) -> days / 365.25
    * Hour (805 / HR) -> hours / 8760
    * Biological plausibility filtering (0 to 125 years)
- Drug name normalization: uppercase, trim, prefer openfda.generic_name over brand/medicinalproduct
- Deduplication: groups by case_id / safetyreportid and retains latest version
"""

import pytest
from pyspark.sql import DataFrame, SparkSession
import pyspark.sql.functions as F
from pyspark.sql.types import (
    ArrayType,
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)
from pyspark.sql.window import Window


# ==============================================================================
# PYTEST FIXTURES
# ==============================================================================

@pytest.fixture(scope="session")
def spark_session():
    """Provides a local PySpark session for data cleaning unit tests."""
    spark = (
        SparkSession.builder.appName("TestFAERSCleaning")
        .master("local[1]")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.shuffle.partitions", "1")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")
    yield spark
    spark.stop()


# ==============================================================================
# CANONICAL CLEANING LOGIC (Tested directly to guarantee contract adherence)
# ==============================================================================

def map_sex_code(df: DataFrame, raw_col: str = "patientsex") -> DataFrame:
    """Applies sex code mapping rule."""
    return df.withColumn(
        "sex",
        F.when(F.col(raw_col) == "1", F.lit("Male"))
        .when(F.col(raw_col) == "2", F.lit("Female"))
        .otherwise(F.lit("Unknown")),
    )


def normalize_age_to_years(df: DataFrame, age_col: str = "age", unit_col: str = "unit") -> DataFrame:
    """Applies multi-unit age conversion rule to standardized decimal years."""
    raw_age = F.col(age_col).cast(DoubleType())
    unit = F.upper(F.trim(F.col(unit_col)))

    age_in_years = (
        F.when(unit.isin("800", "DEC", "DECADE", "DECADES"), raw_age * 10.0)
        .when(unit.isin("801", "YR", "YEAR", "YEARS", "YRS"), raw_age * 1.0)
        .when(unit.isin("802", "MON", "MONTH", "MONTHS", "MTH"), raw_age / 12.0)
        .when(unit.isin("803", "WK", "WEEK", "WEEKS"), raw_age / 52.0)
        .when(unit.isin("804", "DY", "DAY", "DAYS"), raw_age / 365.25)
        .when(unit.isin("805", "HR", "HOUR", "HOURS"), raw_age / 8760.0)
        .when(raw_age.isNotNull() & (raw_age >= 0) & (raw_age <= 125), raw_age)
        .otherwise(F.lit(None).cast(DoubleType()))
    )

    return df.withColumn(
        "age_years",
        F.when((age_in_years >= 0.0) & (age_in_years <= 125.0), F.round(age_in_years, 2))
        .otherwise(F.lit(None).cast(DoubleType())),
    )


def normalize_drug_name(
    df: DataFrame,
    generic_col: str = "generic_name",
    medicinal_col: str = "medicinalproduct",
) -> DataFrame:
    """Applies drug name standardization: generic priority, uppercase, whitespace trim."""
    return df.withColumn(
        "drug_name",
        F.upper(F.trim(F.coalesce(F.col(generic_col), F.col(medicinal_col)))),
    )


def deduplicate_cases(
    df: DataFrame,
    id_col: str = "safetyreportid",
    version_col: str = "safetyreportversion",
) -> DataFrame:
    """Keeps the latest version for each unique safety report ID."""
    window_spec = Window.partitionBy(id_col).orderBy(
        F.coalesce(F.col(version_col).cast(IntegerType()), F.lit(1)).desc()
    )
    return (
        df.withColumn("row_rank", F.row_number().over(window_spec))
        .filter(F.col("row_rank") == 1)
        .drop("row_rank")
    )


# ==============================================================================
# TEST CASES
# ==============================================================================

def test_sex_code_mapping(spark_session):
    """Test FAERS sex code mappings: 1 -> Male, 2 -> Female, 0/null/other -> Unknown."""
    schema = StructType([
        StructField("id", StringType(), False),
        StructField("patientsex", StringType(), True),
    ])

    test_data = [
        ("1", "1"),        # Male
        ("2", "2"),        # Female
        ("3", "0"),        # Unknown
        ("4", None),       # Missing -> Unknown
        ("5", "99"),       # Invalid -> Unknown
        ("6", "Male"),     # Non-numeric -> Unknown
    ]

    df = spark_session.createDataFrame(test_data, schema)
    result_df = map_sex_code(df, "patientsex")
    results = {row["id"]: row["sex"] for row in result_df.collect()}

    assert results["1"] == "Male"
    assert results["2"] == "Female"
    assert results["3"] == "Unknown"
    assert results["4"] == "Unknown"
    assert results["5"] == "Unknown"
    assert results["6"] == "Unknown"


def test_age_normalization(spark_session):
    """Test temporal age unit conversion to decimal years and boundary filtering."""
    schema = StructType([
        StructField("id", StringType(), False),
        StructField("age", StringType(), True),
        StructField("unit", StringType(), True),
    ])

    test_data = [
        ("1", "6", "800"),       # 6 Decades -> 60.0 years
        ("2", "5", "DEC"),       # 5 Decades string -> 50.0 years
        ("3", "45", "801"),      # 45 Years -> 45.0 years
        ("4", "30", "YR"),       # 30 Years string -> 30.0 years
        ("5", "24", "802"),      # 24 Months -> 2.0 years
        ("6", "18", "MON"),      # 18 Months -> 1.5 years
        ("7", "104", "803"),     # 104 Weeks -> 2.0 years
        ("8", "730.5", "804"),   # 730.5 Days -> 2.0 years
        ("9", "8760", "805"),    # 8760 Hours -> 1.0 year
        ("10", "-5", "801"),     # Negative age -> None (invalid)
        ("11", "150", "801"),    # Implausible age (>125) -> None
        ("12", None, "801"),     # Missing age -> None
    ]

    df = spark_session.createDataFrame(test_data, schema)
    result_df = normalize_age_to_years(df, "age", "unit")
    results = {row["id"]: row["age_years"] for row in result_df.collect()}

    assert results["1"] == pytest.approx(60.0, rel=1e-2)
    assert results["2"] == pytest.approx(50.0, rel=1e-2)
    assert results["3"] == pytest.approx(45.0, rel=1e-2)
    assert results["4"] == pytest.approx(30.0, rel=1e-2)
    assert results["5"] == pytest.approx(2.0, rel=1e-2)
    assert results["6"] == pytest.approx(1.5, rel=1e-2)
    assert results["7"] == pytest.approx(2.0, rel=1e-2)
    assert results["8"] == pytest.approx(2.0, rel=1e-2)
    assert results["9"] == pytest.approx(1.0, rel=1e-2)
    assert results["10"] is None
    assert results["11"] is None
    assert results["12"] is None


def test_drug_name_normalization(spark_session):
    """Test drug name normalization prioritizing generic name with brand fallback."""
    schema = StructType([
        StructField("id", StringType(), False),
        StructField("generic_name", StringType(), True),
        StructField("medicinalproduct", StringType(), True),
    ])

    test_data = [
        # 1. Both present -> Generic name preferred
        ("1", "Rosiglitazone", "Avandia 4mg"),
        # 2. Generic missing -> Fallback to medicinalproduct
        ("2", None, "  Lipitor 20mg  "),
        # 3. Uppercase conversion and whitespace trimming
        ("3", "  metformin hcl  ", "Glucophage"),
        # 4. Trailing spaces and lowercase medicinal product
        ("4", None, "aspirin"),
    ]

    df = spark_session.createDataFrame(test_data, schema)
    result_df = normalize_drug_name(df, "generic_name", "medicinalproduct")
    results = {row["id"]: row["drug_name"] for row in result_df.collect()}

    assert results["1"] == "ROSIGLITAZONE"
    assert results["2"] == "LIPITOR 20MG"
    assert results["3"] == "METFORMIN HCL"
    assert results["4"] == "ASPIRIN"


def test_deduplication(spark_session):
    """Test deduplication keeps the latest case version for duplicate safetyreportid."""
    schema = StructType([
        StructField("safetyreportid", StringType(), False),
        StructField("safetyreportversion", StringType(), False),
        StructField("receivedate", StringType(), False),
        StructField("payload", StringType(), False),
    ])

    test_data = [
        ("CASE-100", "1", "20230101", "Old initial report"),
        ("CASE-100", "2", "20230215", "Updated follow-up report with fatal outcome"),
        ("CASE-200", "1", "20230301", "Single version case"),
        ("CASE-300", "1", "20230101", "Version 1"),
        ("CASE-300", "3", "20230401", "Version 3 latest"),
        ("CASE-300", "2", "20230201", "Version 2 superseded"),
    ]

    df = spark_session.createDataFrame(test_data, schema)
    dedup_df = deduplicate_cases(df)

    assert dedup_df.count() == 3

    results = {
        row["safetyreportid"]: (row["safetyreportversion"], row["payload"])
        for row in dedup_df.collect()
    }

    # Verify CASE-100 retained Version 2
    assert results["CASE-100"][0] == "2"
    assert results["CASE-100"][1] == "Updated follow-up report with fatal outcome"

    # Verify CASE-200 retained Version 1
    assert results["CASE-200"][0] == "1"

    # Verify CASE-300 retained Version 3
    assert results["CASE-300"][0] == "3"
    assert results["CASE-300"][1] == "Version 3 latest"
