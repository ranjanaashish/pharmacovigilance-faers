"""Apache Kafka configuration and client factory for real-time FAERS event streaming.

Manages connection parameters, topic namespaces, and serialization protocols
for both simulated event producers and downstream Spark/microservice consumers.
"""

import json
import os
from typing import Any, Callable, Dict, Optional


# Broker and Topic Definitions
KAFKA_BROKER: str = os.getenv("KAFKA_BROKER", "localhost:9092")
FAERS_TOPIC: str = os.getenv("FAERS_TOPIC", "faers-raw-reports")
SIGNALS_TOPIC: str = os.getenv("SIGNALS_TOPIC", "faers-detected-signals")
ALERTS_TOPIC: str = os.getenv("ALERTS_TOPIC", "faers-safety-alerts")
CONSUMER_GROUP_ID: str = os.getenv("CONSUMER_GROUP_ID", "faers-pharmacovigilance-group")


def json_serializer(data: Any) -> bytes:
    """Serialize Python dictionary or object to UTF-8 encoded JSON bytes."""
    return json.dumps(data, default=str).encode("utf-8")


def json_deserializer(raw_bytes: bytes) -> Any:
    """Deserialize UTF-8 encoded JSON bytes to Python data structures."""
    if raw_bytes is None:
        return None
    return json.loads(raw_bytes.decode("utf-8"))


def string_serializer(key: Optional[str]) -> Optional[bytes]:
    """Serialize string partition key to UTF-8 bytes."""
    if key is None:
        return None
    return key.encode("utf-8")


def string_deserializer(raw_bytes: Optional[bytes]) -> Optional[str]:
    """Deserialize UTF-8 bytes to string partition key."""
    if raw_bytes is None:
        return None
    return raw_bytes.decode("utf-8")


def get_producer_config(
    bootstrap_servers: Optional[str] = None,
    client_id: str = "faers-event-producer",
) -> Dict[str, Any]:
    """Generate configuration dictionary for KafkaProducer instances.

    Parameters
    ----------
    bootstrap_servers : str, optional
        Comma-separated list of Kafka broker hosts. Defaults to KAFKA_BROKER.
    client_id : str, default="faers-event-producer"
        Identifier string transmitted to broker for tracking.

    Returns
    -------
    Dict[str, Any]
        Dictionary compatible with kafka.KafkaProducer.
    """
    return {
        "bootstrap_servers": bootstrap_servers or KAFKA_BROKER,
        "client_id": client_id,
        "value_serializer": json_serializer,
        "key_serializer": string_serializer,
        "acks": "all",
        "retries": 5,
        "linger_ms": 20,
        "batch_size": 32768,  # 32 KB batching for throughput
        "compression_type": "gzip",
        "max_in_flight_requests_per_connection": 5,
    }


def get_consumer_config(
    group_id: Optional[str] = None,
    bootstrap_servers: Optional[str] = None,
    auto_offset_reset: str = "earliest",
    enable_auto_commit: bool = True,
) -> Dict[str, Any]:
    """Generate configuration dictionary for KafkaConsumer instances.

    Parameters
    ----------
    group_id : str, optional
        Consumer group ID. Defaults to CONSUMER_GROUP_ID.
    bootstrap_servers : str, optional
        Comma-separated broker list. Defaults to KAFKA_BROKER.
    auto_offset_reset : str, default="earliest"
        Offset strategy when no initial offset exists ('earliest' or 'latest').
    enable_auto_commit : bool, default=True
        Whether consumer offsets should be committed periodically.

    Returns
    -------
    Dict[str, Any]
        Dictionary compatible with kafka.KafkaConsumer.
    """
    return {
        "bootstrap_servers": bootstrap_servers or KAFKA_BROKER,
        "group_id": group_id or CONSUMER_GROUP_ID,
        "auto_offset_reset": auto_offset_reset,
        "enable_auto_commit": enable_auto_commit,
        "auto_commit_interval_ms": 5000,
        "value_deserializer": json_deserializer,
        "key_deserializer": string_deserializer,
        "session_timeout_ms": 30000,
        "max_poll_records": 500,
    }


def get_kafka_producer(bootstrap_servers: Optional[str] = None) -> Any:
    """Instantiate and return a KafkaProducer using standard project config."""
    try:
        from kafka import KafkaProducer  # type: ignore
        config = get_producer_config(bootstrap_servers=bootstrap_servers)
        return KafkaProducer(**config)
    except ImportError as exc:
        raise ImportError(
            "kafka-python package is required for streaming. Install with: pip install kafka-python"
        ) from exc


def get_kafka_consumer(*topics: str, **kwargs: Any) -> Any:
    """Instantiate and return a KafkaConsumer subscribed to the specified topics."""
    try:
        from kafka import KafkaConsumer  # type: ignore
        config = get_consumer_config(**kwargs)
        target_topics = topics if topics else (FAERS_TOPIC,)
        return KafkaConsumer(*target_topics, **config)
    except ImportError as exc:
        raise ImportError(
            "kafka-python package is required for streaming. Install with: pip install kafka-python"
        ) from exc


if __name__ == "__main__":
    print(f"Kafka Config - Broker: {KAFKA_BROKER}")
    print(f"FAERS Topic: {FAERS_TOPIC}, Signals Topic: {SIGNALS_TOPIC}")
    print(f"Producer Config: {get_producer_config()}")
