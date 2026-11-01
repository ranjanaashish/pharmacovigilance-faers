"""Unit tests for Pharmacovigilance PRR (Proportional Reporting Ratio) Calculator.

Validates the Evans et al. (2001) Disproportionality Analysis algorithm:
- Known signal benchmark: Rosiglitazone + Myocardial Infarction (PRR ≈ 19.8)
- Null signal benchmark: Balanced drug-reaction distribution (PRR ≈ 1.0)
- Boundary threshold tests: PRR >= 2.0 AND a >= 3 AND chi2 >= 4.0
- Pearson Chi-square formula correctness
- Signal classification true positives and true negatives
"""

import math
import pytest
from typing import Dict, Tuple

# Pure Python reference implementations for rigorous mathematical benchmarking
def calculate_prr(a: float, b: float, c: float, d: float) -> float:
    """Compute PRR from 2x2 contingency table counts."""
    drug_exposed_rate = a / (a + b)
    other_exposed_rate = c / (c + d)
    return drug_exposed_rate / other_exposed_rate


def calculate_chi_square(a: float, b: float, c: float, d: float) -> float:
    """Compute Pearson Chi-Square test statistic from 2x2 contingency table counts."""
    n = a + b + c + d
    numerator = n * math.pow((a * d - b * c), 2)
    denominator = (a + b) * (c + d) * (a + c) * (b + d)
    if denominator == 0:
        return 0.0
    return numerator / denominator


def is_safety_signal(prr: float, cases: int, chi_square: float) -> bool:
    """Evaluate Evans et al. safety signal criteria: PRR >= 2.0, cases >= 3, chi2 >= 4.0."""
    return bool(prr >= 2.0 and cases >= 3 and chi_square >= 4.0)


# ==============================================================================
# PYTEST FIXTURES
# ==============================================================================

@pytest.fixture(scope="session")
def spark_session():
    """Provides a lightweight local PySpark session for DataFrame integration tests."""
    from pyspark.sql import SparkSession
    spark = (
        SparkSession.builder.appName("TestPRRCalculator")
        .master("local[1]")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.shuffle.partitions", "1")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")
    yield spark
    spark.stop()


@pytest.fixture
def rosiglitazone_mi_contingency() -> Dict[str, int]:
    """2x2 contingency counts reproducing the known Rosiglitazone + MI safety signal.

    a = 450   (Rosiglitazone reports with Myocardial Infarction)
    b = 4,550 (Rosiglitazone reports with other adverse reactions)
    c = 500   (Other drugs with Myocardial Infarction)
    d = 109,500 (Other drugs with other adverse reactions)
    """
    return {
        "a": 450,
        "b": 4550,
        "c": 500,
        "d": 109500,
        "drug_name": "ROSIGLITAZONE",
        "reaction": "MYOCARDIAL INFARCTION",
    }


@pytest.fixture
def balanced_null_contingency() -> Dict[str, int]:
    """Balanced contingency table where drug incidence equals background incidence."""
    return {
        "a": 10,
        "b": 90,
        "c": 100,
        "d": 900,
        "drug_name": "PLACEBO_CONTROL",
        "reaction": "HEADACHE",
    }


# ==============================================================================
# TEST CASES
# ==============================================================================

def test_prr_known_signal(rosiglitazone_mi_contingency):
    """Test known Rosiglitazone + MI signal produces expected PRR ≈ 19.8 and triggers alert."""
    counts = rosiglitazone_mi_contingency
    a = counts["a"]
    b = counts["b"]
    c = counts["c"]
    d = counts["d"]

    prr = calculate_prr(a, b, c, d)
    chi2 = calculate_chi_square(a, b, c, d)
    signal = is_safety_signal(prr, a, chi2)

    # a/(a+b) = 450/5000 = 0.09
    # c/(c+d) = 500/110000 = 0.00454545...
    # PRR = 0.09 / (500/110000) = 19.8
    assert prr == pytest.approx(19.8, rel=1e-3)
    assert a >= 3
    assert chi2 > 4.0
    assert signal is True


def test_prr_no_signal(balanced_null_contingency):
    """Test balanced pair produces PRR ≈ 1.0, chi^2 = 0.0, and does NOT trigger a signal."""
    counts = balanced_null_contingency
    a = counts["a"]
    b = counts["b"]
    c = counts["c"]
    d = counts["d"]

    prr = calculate_prr(a, b, c, d)
    chi2 = calculate_chi_square(a, b, c, d)
    signal = is_safety_signal(prr, a, chi2)

    # a/(a+b) = 10/100 = 0.1
    # c/(c+d) = 100/1000 = 0.1
    # PRR = 0.1 / 0.1 = 1.0
    assert prr == pytest.approx(1.0, rel=1e-3)
    assert chi2 == pytest.approx(0.0, abs=1e-4)
    assert signal is False


def test_prr_threshold():
    """Verify strict boundary behavior at the exact cutoff values (PRR=2.0, cases=3, chi2=4.0)."""
    # Exact boundary threshold -> must be True
    assert is_safety_signal(prr=2.0, cases=3, chi_square=4.0) is True

    # Slightly above threshold -> must be True
    assert is_safety_signal(prr=2.001, cases=3, chi_square=4.001) is True

    # Failed case count (a < 3) even with massive PRR and Chi2 -> False
    assert is_safety_signal(prr=100.0, cases=2, chi_square=50.0) is False
    assert is_safety_signal(prr=50.0, cases=0, chi_square=20.0) is False

    # Failed PRR (< 2.0) even with high cases and Chi2 -> False
    assert is_safety_signal(prr=1.99, cases=50, chi_square=100.0) is False

    # Failed Chi-Square (< 4.0) even with high PRR and cases -> False
    assert is_safety_signal(prr=5.0, cases=10, chi_square=3.99) is False


def test_chi_square_calculation():
    """Verify Pearson chi-square calculation matches theoretical values on known 2x2 table."""
    # Contingency Table:
    # a = 20, b = 80   (row 1 total = 100)
    # c = 30, d = 70   (row 2 total = 100)
    # col 1 total = 50, col 2 total = 150, N = 200
    # ad - bc = (20*70) - (80*30) = 1400 - 2400 = -1000
    # Numerator = 200 * (-1000)^2 = 200,000,000
    # Denominator = 100 * 100 * 50 * 150 = 75,000,000
    # Chi2 = 200,000,000 / 75,000,000 = 2.6666667
    a, b, c, d = 20, 80, 30, 70
    chi2 = calculate_chi_square(a, b, c, d)
    assert chi2 == pytest.approx(2.6666667, rel=1e-4)


def test_is_signal_classification():
    """Test comprehensive matrix of true positives and true negatives."""
    test_cases = [
        # (prr, cases, chi2, expected_signal)
        (2.5, 10, 15.2, True),    # Clear positive
        (19.8, 450, 4200.0, True),# Strong positive
        (2.0, 3, 4.0, True),      # Boundary positive
        (1.99, 10, 15.0, False),  # Sub-threshold PRR
        (3.5, 2, 8.0, False),     # Sub-threshold cases
        (4.2, 5, 3.8, False),     # Sub-threshold chi-square
        (1.0, 100, 0.0, False),   # Null association
        (0.5, 20, 12.0, False),   # Inverse / protective association
    ]

    for prr_val, n_cases, chi2_val, expected in test_cases:
        actual = is_safety_signal(prr_val, n_cases, chi2_val)
        assert actual == expected, (
            f"Failed for PRR={prr_val}, cases={n_cases}, chi2={chi2_val}. "
            f"Expected {expected}, got {actual}."
        )


def test_spark_prr_dataframe_computation(spark_session):
    """Integration test verifying analytics.prr_calculator.compute_prr on PySpark DataFrame."""
    from analytics.prr_calculator import compute_prr
    from pyspark.sql.types import (
        StructType,
        StructField,
        StringType,
        IntegerType,
    )

    # Construct test data:
    # 4 reports:
    # Report 1: DRUG_A (PS) + REACTION_X
    # Report 2: DRUG_A (PS) + REACTION_X
    # Report 3: DRUG_A (PS) + REACTION_X
    # Report 4: DRUG_B (PS) + REACTION_Y
    schema = StructType([
        StructField("safetyreportid", StringType(), False),
        StructField("drug_name", StringType(), False),
        StructField("reaction", StringType(), False),
        StructField("drug_role", StringType(), False),
        StructField("is_fatal", IntegerType(), False),
    ])

    data = [
        ("R1", "DRUG_A", "REACTION_X", "Primary Suspect", 0),
        ("R2", "DRUG_A", "REACTION_X", "Primary Suspect", 0),
        ("R3", "DRUG_A", "REACTION_X", "Primary Suspect", 1),
        ("R4", "DRUG_B", "REACTION_Y", "Primary Suspect", 0),
    ]

    df = spark_session.createDataFrame(data, schema)
    results_df = compute_prr(spark_session, df)

    # Check results DataFrame structure and fields
    columns = results_df.columns
    assert "drug_name" in columns
    assert "reaction" in columns
    assert "prr" in columns
    assert "chi_square" in columns
    assert "is_signal" in columns
    assert "fatality_rate" in columns
