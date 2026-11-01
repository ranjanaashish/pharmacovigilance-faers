"""
MongoDB Storage Writer for Pharmacovigilance Operational Store.

Persists structured drug safety profiles and nested signal collections into MongoDB:
- Groups signal metrics per drug as nested subdocuments
- Incorporates patient demographic summaries
- Executes high-performance bulk upserts via PyMongo
"""

import logging
from datetime import datetime
from typing import Optional, Union, Dict, Any, List
from pymongo import MongoClient, UpdateOne
import pyspark.sql.functions as F
from pyspark.sql import DataFrame

logger = logging.getLogger("pharma_storage.mongo_writer")
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

BATCH_SIZE = 500


def get_mongo_client(mongo_uri: str) -> MongoClient:
    """Creates a MongoClient with connection pooling and timeouts."""
    return MongoClient(
        mongo_uri,
        serverSelectionTimeoutMS=5000,
        connectTimeoutMS=5000,
        socketTimeoutMS=10000,
    )


def write_signals_to_mongo(
    prr_df: Union[DataFrame, Any],
    mongo_uri: str,
    db_name: str,
    demo_df: Optional[Union[DataFrame, Any]] = None,
) -> int:
    """
    Groups PRR safety signals by drug name, nests signals as subdocuments,
    merges optional demographic profiles, and performs bulk upserts into MongoDB.

    Args:
        prr_df: PySpark DataFrame (or pandas DataFrame) of PRR signal results.
        mongo_uri: MongoDB connection string (e.g. mongodb://localhost:27017/).
        db_name: Target MongoDB database name (e.g. pharma_db).
        demo_df: Optional demographic summary DataFrame to embed into drug documents.

    Returns:
        Total number of drug documents upserted.
    """
    client = get_mongo_client(mongo_uri)
    db = client[db_name]
    signals_coll = db["drug_signals"]

    # Ensure indexes
    signals_coll.create_index("drug_name", unique=True)
    signals_coll.create_index("total_signals")
    signals_coll.create_index("signals.reaction")
    signals_coll.create_index("signals.prr")

    logger.info(f"Grouping PRR signals for MongoDB ingestion into '{db_name}.drug_signals'...")

    # Build demographic lookup dictionary if demo_df is provided
    demo_lookup: Dict[str, Dict[str, Any]] = {}
    if demo_df is not None:
        logger.info("Extracting demographics to merge with drug signals...")
        if hasattr(demo_df, "collect"):
            demo_rows = demo_df.collect()
            for r in demo_rows:
                d = r.asDict()
                drug = str(d.get("drug_name", "")).strip().upper()
                if drug:
                    demo_lookup[drug] = {
                        k: v for k, v in d.items() if k not in ["drug_name", "_id"]
                    }
        elif hasattr(demo_df, "to_dict"):
            for d in demo_df.to_dict(orient="records"):
                drug = str(d.get("drug_name", "")).strip().upper()
                if drug:
                    demo_lookup[drug] = {
                        k: v for k, v in d.items() if k not in ["drug_name", "_id"]
                    }

    # If PySpark DataFrame, perform scalable aggregation in Spark
    if hasattr(prr_df, "groupBy"):
        cols = prr_df.columns

        # Ensure required columns
        signal_struct_cols = [
            F.col("reaction").alias("reaction"),
            F.col("a").cast("long").alias("a"),
            F.col("b").cast("long").alias("b"),
            F.col("c").cast("long").alias("c"),
            F.col("d").cast("long").alias("d"),
            F.col("drug_total").cast("long").alias("drug_total"),
            F.col("reaction_total").cast("long").alias("reaction_total"),
            F.col("prr").cast("double").alias("prr"),
            F.col("chi_square").cast("double").alias("chi_square"),
            F.col("is_signal").cast("boolean").alias("is_signal"),
        ]

        if "death_count" in cols:
            signal_struct_cols.append(F.col("death_count").cast("long").alias("death_count"))
        if "fatality_rate" in cols:
            signal_struct_cols.append(F.col("fatality_rate").cast("double").alias("fatality_rate"))

        signal_struct = F.struct(*signal_struct_cols)

        grouped_df = (
            prr_df.withColumn("signal_subdoc", signal_struct)
            .groupBy("drug_name")
            .agg(
                F.collect_list("signal_subdoc").alias("signals"),
                F.count(F.when(F.col("is_signal") == True, 1)).alias("total_signals"),  # noqa: E712
                F.max("drug_total").alias("total_cases"),
                (
                    F.sum("death_count").alias("total_deaths")
                    if "death_count" in cols
                    else F.lit(0).alias("total_deaths")
                ),
            )
        )

        rows = grouped_df.collect()
        drug_records = []
        for row in rows:
            d = row.asDict(recursive=True)
            drug_name = str(d["drug_name"]).strip().upper()
            d["_id"] = drug_name
            d["drug_name"] = drug_name
            d["updated_at"] = datetime.utcnow()

            # Attach demographics if available
            if drug_name in demo_lookup:
                d["demographics"] = demo_lookup[drug_name]

            drug_records.append(d)

    else:
        # Fallback for pandas DataFrame / list of records
        records = prr_df.to_dict(orient="records") if hasattr(prr_df, "to_dict") else list(prr_df)
        from collections import defaultdict

        grouped = defaultdict(list)
        for r in records:
            drug = str(r["drug_name"]).strip().upper()
            grouped[drug].append(r)

        drug_records = []
        for drug_name, sig_list in grouped.items():
            sig_count = sum(1 for s in sig_list if s.get("is_signal", False))
            max_cases = max((s.get("drug_total", 0) for s in sig_list), default=0)
            total_deaths = sum(s.get("death_count", 0) for s in sig_list)

            doc = {
                "_id": drug_name,
                "drug_name": drug_name,
                "total_signals": sig_count,
                "total_cases": max_cases,
                "total_deaths": total_deaths,
                "signals": sig_list,
                "updated_at": datetime.utcnow(),
            }
            if drug_name in demo_lookup:
                doc["demographics"] = demo_lookup[drug_name]

            drug_records.append(doc)

    logger.info(f"Executing MongoDB bulk upsert for {len(drug_records):,} drug documents...")

    operations: List[UpdateOne] = []
    total_upserted = 0

    for doc in drug_records:
        operations.append(
            UpdateOne(
                {"_id": doc["_id"]},
                {"$set": doc},
                upsert=True,
            )
        )

        if len(operations) >= BATCH_SIZE:
            res = signals_coll.bulk_write(operations, ordered=False)
            total_upserted += (res.upserted_count + res.modified_count)
            operations = []

    if operations:
        res = signals_coll.bulk_write(operations, ordered=False)
        total_upserted += (res.upserted_count + res.modified_count)

    logger.info(f"Successfully upserted {len(drug_records):,} drug records to '{db_name}.drug_signals'.")
    client.close()
    return len(drug_records)


def write_drug_summary_to_mongo(
    summary_df: Union[DataFrame, Any],
    mongo_uri: str,
    db_name: str,
) -> int:
    """
    Writes aggregated drug demographic summaries to MongoDB.
    Updates the 'drug_summaries' collection and syncs 'demographics' subdocument in 'drug_signals'.

    Args:
        summary_df: PySpark or pandas DataFrame containing drug demographic summaries.
        mongo_uri: MongoDB connection string.
        db_name: Target MongoDB database name.

    Returns:
        Total number of summary records upserted.
    """
    client = get_mongo_client(mongo_uri)
    db = client[db_name]
    summary_coll = db["drug_summaries"]
    signals_coll = db["drug_signals"]

    summary_coll.create_index("drug_name", unique=True)

    if hasattr(summary_df, "collect"):
        rows = [r.asDict() for r in summary_df.collect()]
    elif hasattr(summary_df, "to_dict"):
        rows = summary_df.to_dict(orient="records")
    else:
        rows = list(summary_df)

    logger.info(f"Writing {len(rows):,} drug summaries to '{db_name}.drug_summaries'...")

    summary_ops: List[UpdateOne] = []
    signal_sync_ops: List[UpdateOne] = []

    for r in rows:
        drug_name = str(r.get("drug_name", "")).strip().upper()
        if not drug_name:
            continue

        doc = dict(r)
        doc["_id"] = drug_name
        doc["drug_name"] = drug_name
        doc["updated_at"] = datetime.utcnow()

        summary_ops.append(
            UpdateOne(
                {"_id": drug_name},
                {"$set": doc},
                upsert=True,
            )
        )

        # Also sync demographics subdocument into drug_signals collection
        demo_fields = {k: v for k, v in doc.items() if k not in ["_id", "drug_name"]}
        signal_sync_ops.append(
            UpdateOne(
                {"_id": drug_name},
                {"$set": {"demographics": demo_fields, "updated_at": datetime.utcnow()}},
                upsert=True,
            )
        )

        if len(summary_ops) >= BATCH_SIZE:
            summary_coll.bulk_write(summary_ops, ordered=False)
            signals_coll.bulk_write(signal_sync_ops, ordered=False)
            summary_ops = []
            signal_sync_ops = []

    if summary_ops:
        summary_coll.bulk_write(summary_ops, ordered=False)
        signals_coll.bulk_write(signal_sync_ops, ordered=False)

    logger.info(f"Successfully upserted {len(rows):,} drug summaries into MongoDB.")
    client.close()
    return len(rows)
