"""Apache Kafka Event Producer for FAERS Adverse Event Streaming Simulation.

This module simulates real-time ingestion of FDA Adverse Event Reporting System
(FAERS) reports into Apache Kafka. It iterates through raw JSON partition files
downloaded from OpenFDA, parses individual report objects from the top-level
'results' array, and publishes them sequentially to a Kafka broker with
configurable throttling.

Usage:
    python streaming/kafka_producer.py --data-dir data/raw --topic faers-raw-events --delay 0.001 --max-records 10000
"""

import argparse
import json
import logging
import os
import sys
import time
from pathlib import Path
from typing import Any, Dict, Generator, List, Optional

# Ensure project root is available on sys.path
PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

# Import centralized Kafka settings
try:
    from config.kafka_config import (
        KAFKA_BROKER,
        get_producer_config,
    )
except ImportError:
    KAFKA_BROKER = os.getenv("KAFKA_BROKER", "localhost:9092")

    def get_producer_config(bootstrap_servers: Optional[str] = None, client_id: str = "faers-producer") -> Dict[str, Any]:
        return {
            "bootstrap_servers": bootstrap_servers or KAFKA_BROKER,
            "client_id": client_id,
            "value_serializer": lambda v: json.dumps(v, default=str).encode("utf-8"),
            "key_serializer": lambda k: str(k).encode("utf-8") if k is not None else None,
            "acks": "all",
            "retries": 3,
        }

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("faers.streaming.producer")


def find_faers_json_files(data_dir: Path) -> List[Path]:
    """Discover all raw FAERS JSON files in the specified directory.

    Args:
        data_dir: Filesystem path to raw FAERS directory.

    Returns:
        List of Path objects matching *.json files, sorted alphabetically.
    """
    if not data_dir.exists():
        logger.warning(f"Data directory '{data_dir}' does not exist.")
        return []

    # Find JSON files directly in directory or subdirectories, excluding checkpoint/meta files
    json_files = sorted(
        [
            p for p in data_dir.rglob("*.json")
            if not p.name.startswith(".") and not p.name.endswith(".tmp")
        ]
    )
    return json_files


def stream_records_from_file(file_path: Path) -> Generator[Dict[str, Any], None, None]:
    """Stream individual report records from an OpenFDA JSON file.

    OpenFDA files are structured as:
    {
        "meta": {...},
        "results": [
            { "safetyreportid": "...", "patient": {...}, ... },
            ...
        ]
    }

    Args:
        file_path: Path to the target JSON file.

    Yields:
        Individual report dictionary objects extracted from the 'results' list.
    """
    logger.info(f"Opening FAERS JSON file: {file_path.name} ({file_path.stat().st_size / (1024 * 1024):.2f} MB)")
    try:
        with open(file_path, "r", encoding="utf-8") as f:
            data = json.load(f)

        if isinstance(data, dict):
            # Standard OpenFDA format
            results = data.get("results", [])
            if not isinstance(results, list):
                logger.warning(f"Expected 'results' key to be a list in {file_path.name}, got {type(results).__name__}")
                return
            for record in results:
                if isinstance(record, dict):
                    yield record
        elif isinstance(data, list):
            # Array-of-records format
            for record in data:
                if isinstance(record, dict):
                    yield record
        else:
            logger.warning(f"Unrecognized JSON structure in {file_path.name}")
    except json.JSONDecodeError as exc:
        logger.error(f"Failed to parse JSON file {file_path.name}: {exc}")
    except Exception as exc:
        logger.error(f"Error reading file {file_path.name}: {exc}")


def extract_record_key(record: Dict[str, Any]) -> str:
    """Extract a unique report key to use for Kafka message partitioning.

    Args:
        record: FAERS adverse event report dictionary.

    Returns:
        String report ID or fallback unique identifier.
    """
    for key_name in ("safetyreportid", "report_id", "safetyreportversion", "caseid"):
        val = record.get(key_name)
        if val is not None and str(val).strip():
            return str(val).strip()
    return f"anon-{time.time_ns()}"


def run_producer(
    data_dir: Path,
    topic: str = "faers-raw-events",
    delay_seconds: float = 0.001,
    max_records: Optional[int] = None,
    bootstrap_servers: Optional[str] = None,
) -> int:
    """Publish FAERS JSON reports to the specified Kafka topic.

    Args:
        data_dir: Directory containing raw FAERS JSON files.
        topic: Destination Kafka topic name.
        delay_seconds: Sleep duration in seconds between consecutive messages (throttling).
        max_records: Maximum total records to publish before terminating. None = unlimited.
        bootstrap_servers: Comma-separated list of Kafka broker endpoints.

    Returns:
        Total count of messages successfully dispatched to Kafka.
    """
    # Initialize KafkaProducer
    try:
        from kafka import KafkaProducer  # type: ignore
    except ImportError:
        logger.error(
            "kafka-python is not installed. Please install it via: pip install kafka-python"
        )
        raise

    kafka_config = get_producer_config(bootstrap_servers=bootstrap_servers)
    logger.info(f"Connecting to Kafka brokers at: {kafka_config['bootstrap_servers']}...")

    try:
        producer = KafkaProducer(**kafka_config)
    except Exception as exc:
        logger.error(f"Failed to initialize KafkaProducer: {exc}")
        raise

    json_files = find_faers_json_files(data_dir)
    if not json_files:
        logger.warning(f"No FAERS JSON files found in {data_dir}. Exiting producer.")
        producer.close()
        return 0

    logger.info(
        f"Found {len(json_files)} FAERS file(s) to stream. Target Topic: '{topic}', "
        f"Throttle: {delay_seconds:.4f}s, Max Records: {max_records or 'ALL'}"
    )

    records_published = 0
    start_time = time.time()
    last_progress_time = start_time

    try:
        for file_path in json_files:
            if max_records is not None and records_published >= max_records:
                break

            for record in stream_records_from_file(file_path):
                if max_records is not None and records_published >= max_records:
                    logger.info(f"Reached requested limit of {max_records} records.")
                    break

                key = extract_record_key(record)
                producer.send(topic, key=key, value=record)
                records_published += 1

                # Periodic progress output every 1000 records
                if records_published % 1000 == 0:
                    current_time = time.time()
                    elapsed = max(0.001, current_time - last_progress_time)
                    rate = 1000.0 / elapsed
                    total_elapsed = max(0.001, current_time - start_time)
                    overall_rate = records_published / total_elapsed
                    print(
                        f"[Streaming Producer] Dispatched {records_published:,} records "
                        f"| Instant: {rate:7.1f} rec/s | Avg: {overall_rate:7.1f} rec/s "
                        f"| Last Key: {key}"
                    )
                    last_progress_time = current_time

                # Configurable throttling
                if delay_seconds > 0:
                    time.sleep(delay_seconds)

    except KeyboardInterrupt:
        logger.info("Kafka streaming producer interrupted by user.")
    except Exception as exc:
        logger.error(f"Unexpected error during streaming publication: {exc}")
        raise
    finally:
        logger.info(f"Flushing remaining messages to Kafka topic '{topic}'...")
        producer.flush(timeout=30)
        producer.close()

    total_time = max(0.001, time.time() - start_time)
    avg_throughput = records_published / total_time
    print("\n" + "=" * 65)
    print("               KAFKA STREAMING PRODUCER SUMMARY              ")
    print("=" * 65)
    print(f" Target Topic          : {topic}")
    print(f" Total Records Sent    : {records_published:,}")
    print(f" Total Time Elapsed    : {total_time:.2f} seconds")
    print(f" Average Throughput    : {avg_throughput:.2f} records/second")
    print("=" * 65 + "\n")

    return records_published


def main() -> None:
    """CLI entrypoint for Kafka producer."""
    parser = argparse.ArgumentParser(
        description="Stream FAERS adverse event reports to Apache Kafka.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument(
        "--data-dir",
        type=str,
        default=str(PROJECT_ROOT / "data" / "raw"),
        help="Path to folder containing raw FAERS JSON files",
    )
    parser.add_argument(
        "--topic",
        type=str,
        default="faers-raw-events",
        help="Target Kafka topic",
    )
    parser.add_argument(
        "--delay",
        type=float,
        default=0.001,
        help="Throttle delay in seconds between messages",
    )
    parser.add_argument(
        "--max-records",
        type=int,
        default=None,
        help="Maximum number of records to dispatch (None for all)",
    )
    parser.add_argument(
        "--bootstrap-servers",
        type=str,
        default=None,
        help="Comma-separated Kafka broker list (overrides default config)",
    )

    args = parser.parse_args()

    run_producer(
        data_dir=Path(args.data_dir),
        topic=args.topic,
        delay_seconds=args.delay,
        max_records=args.max_records,
        bootstrap_servers=args.bootstrap_servers,
    )


if __name__ == "__main__":
    main()
