"""Apache Spark configuration and session factory for the Pharmacovigilance FAERS pipeline.

Provides a standardized PySpark SparkSession configured with:
- Apache Hive metastore and warehouse integration
- MongoDB Spark Connector configuration for read/write operations
- Optimized memory and partition settings for local workstation execution
- Adaptive Query Execution (AQE) enabled for dynamic partition coalescing
"""

import os
import sys
from pathlib import Path

# Ensure JAVA_HOME is configured for local JVM execution
if "JAVA_HOME" not in os.environ:
    candidate_jdk = Path.home() / ".jdk" / "jdk-17.0.10+7"
    if candidate_jdk.exists():
        os.environ["JAVA_HOME"] = str(candidate_jdk)
        os.environ["PATH"] = str(candidate_jdk / "bin") + os.pathsep + os.environ.get("PATH", "")
from typing import Optional
from pyspark.sql import SparkSession


# Base project paths
BASE_DIR = Path(__file__).resolve().parent.parent
DEFAULT_WAREHOUSE_DIR = str(BASE_DIR / "spark-warehouse")
DEFAULT_METASTORE_DIR = str(BASE_DIR / "metastore_db")

# Default MongoDB settings for Spark Connector
DEFAULT_MONGO_URI = os.getenv(
    "MONGO_URI",
    "mongodb://localhost:27017/pharmacovigilance"
)

# Mongo Spark Connector Maven coordinates compatible with Spark 3.5.x / Scala 2.12
MONGO_CONNECTOR_PACKAGE = "org.mongodb.spark:mongo-spark-connector_2.12:10.3.0"


def get_spark_session(
    app_name: str = "FAERS-Pharmacovigilance-Analytics",
    master: str = "local[*]",
    driver_memory: str = "4g",
    executor_memory: str = "4g",
    warehouse_dir: Optional[str] = None,
    mongo_uri: Optional[str] = None,
    enable_hive: bool = True,
) -> SparkSession:
    """Build and return an optimized SparkSession with Hive and MongoDB support.

    Parameters
    ----------
    app_name : str, default="FAERS-Pharmacovigilance-Analytics"
        Application name reported in Spark UI.
    master : str, default="local[*]"
        Spark cluster URL or local execution thread count.
    driver_memory : str, default="4g"
        Amount of memory to allocate to the driver process.
    executor_memory : str, default="4g"
        Amount of memory to allocate per executor process.
    warehouse_dir : str, optional
        Filesystem path for Spark SQL warehouse. Defaults to project/spark-warehouse.
    mongo_uri : str, optional
        Target MongoDB connection URI for reading and writing dataframes.
    enable_hive : bool, default=True
        Whether to attempt Hive metastore integration. Falls back gracefully if unavailable.

    Returns
    -------
    SparkSession
        Configured and active PySpark session.
    """
    effective_warehouse_dir = warehouse_dir or os.getenv("SPARK_WAREHOUSE_DIR", DEFAULT_WAREHOUSE_DIR)
    effective_mongo_uri = mongo_uri or DEFAULT_MONGO_URI

    # Ensure warehouse directory exists
    Path(effective_warehouse_dir).mkdir(parents=True, exist_ok=True)

    builder = (
        SparkSession.builder
        .appName(app_name)
        .master(master)
        # Memory and Resource Allocation
        .config("spark.driver.memory", os.getenv("SPARK_DRIVER_MEMORY", driver_memory))
        .config("spark.executor.memory", os.getenv("SPARK_EXECUTOR_MEMORY", executor_memory))
        .config("spark.driver.maxResultSize", "2g")
        # Partition tuning for local multi-core execution (avoid default 200 partitions for small shuffles)
        .config("spark.sql.shuffle.partitions", os.getenv("SPARK_SHUFFLE_PARTITIONS", "8"))
        .config("spark.default.parallelism", "8")
        # Adaptive Query Execution (AQE) for optimized joins and skew handling
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
        .config("spark.sql.adaptive.skewJoin.enabled", "true")
        # Warehouse and Metastore Paths
        .config("spark.sql.warehouse.dir", Path(effective_warehouse_dir).as_uri())
        .config("javax.jdo.option.ConnectionURL", f"jdbc:derby:;databaseName={DEFAULT_METASTORE_DIR};create=true")
        # MongoDB Spark Connector Read/Write Default URI
        .config("spark.mongodb.read.connection.uri", effective_mongo_uri)
        .config("spark.mongodb.write.connection.uri", effective_mongo_uri)
        # Packages for remote Maven resolution if dependencies not pre-installed
        .config("spark.jars.packages", MONGO_CONNECTOR_PACKAGE)
        # Serialization & Timezone consistency
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        .config("spark.sql.session.timeZone", "UTC")
    )

    if enable_hive:
        try:
            spark = builder.enableHiveSupport().getOrCreate()
        except Exception as exc:
            # Fallback to standard Spark SQL if Hive metastore client fails initialization
            sys.stderr.write(
                f"[WARNING] Hive support failed to initialize ({exc}). Falling back to in-memory Spark catalog.\n"
            )
            spark = builder.getOrCreate()
    else:
        spark = builder.getOrCreate()

    # Set log level to WARN to reduce verbosity in CLI/testing
    spark.sparkContext.setLogLevel(os.getenv("SPARK_LOG_LEVEL", "WARN"))

    return spark


if __name__ == "__main__":
    print("Testing SparkSession initialization...")
    session = get_spark_session(app_name="SparkConfigTest")
    print(f"Spark version: {session.version}")
    print(f"Master: {session.conf.get('spark.master')}")
    print(f"Warehouse dir: {session.conf.get('spark.sql.warehouse.dir')}")
    session.stop()
    print("SparkSession test completed successfully.")
