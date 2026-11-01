"""Apache Hive configuration and connection utilities for Pharmacovigilance data warehousing.

Manages HiveServer2 Thrift connectivity, database namespace conventions,
and provides query execution helpers via either PyHive or PySpark SQL catalog.
"""

import os
from typing import Any, Dict, List, Optional
from pathlib import Path


# Hive Server & Database Defaults
HIVE_HOST: str = os.getenv("HIVE_HOST", "localhost")
HIVE_PORT: int = int(os.getenv("HIVE_PORT", "10000"))
HIVE_DATABASE: str = os.getenv("HIVE_DATABASE", "faers_pharmacovigilance")
HIVE_USERNAME: str = os.getenv("HIVE_USERNAME", "hadoop")
HIVE_AUTH: str = os.getenv("HIVE_AUTH", "NOSASL")

# Schema & Table Name Definitions
TABLE_RAW_REPORTS: str = f"{HIVE_DATABASE}.raw_reports"
TABLE_DRUGS: str = f"{HIVE_DATABASE}.drugs"
TABLE_REACTIONS: str = f"{HIVE_DATABASE}.reactions"
TABLE_DRUG_REACTION_PAIRS: str = f"{HIVE_DATABASE}.drug_reaction_pairs"
TABLE_SAFETY_SIGNALS: str = f"{HIVE_DATABASE}.safety_signals"
TABLE_PATIENT_DEMOGRAPHICS: str = f"{HIVE_DATABASE}.patient_demographics"

# DDL Directory for reference
DDL_DIR: Path = Path(__file__).resolve().parent.parent / "src" / "storage" / "hive_ddl"


def get_hive_connection(
    host: Optional[str] = None,
    port: Optional[int] = None,
    database: Optional[str] = None,
    username: Optional[str] = None,
    auth: Optional[str] = None,
) -> Any:
    """Establish a Thrift connection to HiveServer2 using PyHive.

    If PyHive or thrift server is unavailable in the local environment,
    raises ImportError with installation guidance. For Spark-managed Hive
    catalog access without HiveServer2 daemon, use `execute_hive_via_spark()`.

    Parameters
    ----------
    host : str, optional
        HiveServer2 host address. Defaults to HIVE_HOST.
    port : int, optional
        HiveServer2 Thrift port. Defaults to HIVE_PORT.
    database : str, optional
        Initial Hive database namespace. Defaults to HIVE_DATABASE.
    username : str, optional
        Hive user identifier. Defaults to HIVE_USERNAME.
    auth : str, optional
        Authentication mechanism ('NOSASL', 'KERBEROS', 'LDAP', 'CUSTOM').

    Returns
    -------
    pyhive.hive.Connection
        Active DB-API 2.0 compliant Hive connection.
    """
    effective_host = host or HIVE_HOST
    effective_port = port or HIVE_PORT
    effective_db = database or HIVE_DATABASE
    effective_user = username or HIVE_USERNAME
    effective_auth = auth or HIVE_AUTH

    try:
        from pyhive import hive  # type: ignore

        connection = hive.Connection(
            host=effective_host,
            port=effective_port,
            database=effective_db,
            username=effective_user,
            auth=effective_auth,
        )
        return connection
    except ImportError as err:
        raise ImportError(
            "PyHive is not installed or sasl/thrift dependencies are missing. "
            "Install with: pip install pyhive thrift sasl thrift-sasl. "
            "Alternatively, use PySpark SQL for Hive catalog queries."
        ) from err


def execute_hive_via_spark(query: str, spark_session: Optional[Any] = None) -> List[Dict[str, Any]]:
    """Execute a HiveQL query directly through an active PySpark session.

    This enables executing DDL/DML statements against the Hive metastore
    without requiring a running HiveServer2 Thrift daemon.

    Parameters
    ----------
    query : str
        HiveQL or Spark SQL query string to execute.
    spark_session : SparkSession, optional
        Active SparkSession. If None, retrieves session via `config.spark_config`.

    Returns
    -------
    List[Dict[str, Any]]
        Query results as a list of dictionaries (for SELECT queries), or empty list.
    """
    if spark_session is None:
        from .spark_config import get_spark_session
        spark = get_spark_session(app_name="HiveQueryExecutor")
    else:
        spark = spark_session

    df = spark.sql(query)
    # Check if query returns rows or is a DDL/action statement
    if df.schema.fields:
        return [row.asDict() for row in df.collect()]
    return []


def init_hive_database(spark_session: Optional[Any] = None) -> None:
    """Create the FAERS pharmacovigilance database if it does not already exist."""
    create_db_sql = f"CREATE DATABASE IF NOT EXISTS {HIVE_DATABASE} COMMENT 'FAERS Pharmacovigilance Data Warehouse'"
    execute_hive_via_spark(create_db_sql, spark_session=spark_session)


if __name__ == "__main__":
    print(f"Hive Config - Host: {HIVE_HOST}:{HIVE_PORT}, Database: {HIVE_DATABASE}")
    try:
        conn = get_hive_connection()
        print("Connected to HiveServer2 successfully.")
        conn.close()
    except Exception as exc:
        print(f"Direct Hive connection note: {exc}")
        print("Using PySpark Hive integration for offline/local execution.")
