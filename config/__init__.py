"""Configuration package for the Pharmacovigilance FAERS Big Data Pipeline.

This package exposes centralized configuration helpers for:
- Apache Spark (session builder, memory tuning, MongoDB connector)
- Apache Hive (metastore, warehouse, HiveQL execution)
- MongoDB (client, database access, collection names)
- Apache Kafka (producer, consumer configs, topic definitions)
"""

from .spark_config import get_spark_session
from .hive_config import get_hive_connection, HIVE_DATABASE
from .mongo_config import get_mongo_client, get_database, COLLECTION_SIGNALS, COLLECTION_DRUGS
from .kafka_config import get_producer_config, get_consumer_config, FAERS_TOPIC

__all__ = [
    "get_spark_session",
    "get_hive_connection",
    "HIVE_DATABASE",
    "get_mongo_client",
    "get_database",
    "COLLECTION_SIGNALS",
    "COLLECTION_DRUGS",
    "get_producer_config",
    "get_consumer_config",
    "FAERS_TOPIC",
]
