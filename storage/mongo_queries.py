"""
MongoDB Query Helper for Pharmacovigilance Operational Dashboard.

Provides high-speed operational querying for Streamlit UI and REST services:
- Drug safety signal profiles with embedded demographics
- Global top PRR signals
- Case-insensitive regex drug name search
- Full drug catalog enumeration

Uses connection parameters from `config.mongo_config`.
"""

import os
import sys
import re
import logging
from typing import List, Dict, Any, Optional
from pymongo import MongoClient

# Ensure project root is accessible for importing config
PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

logger = logging.getLogger("pharma_storage.mongo_queries")
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")


def _resolve_mongo_config() -> tuple:
    """
    Extracts mongo connection string and database name from config.mongo_config
    with fallback to environment variables or local defaults.
    """
    uri = "mongodb://localhost:27017/"
    db_name = "pharma_db"

    try:
        import config

        mc = getattr(config, "mongo_config", None)
        if mc is None:
            from config import mongo_config  # type: ignore

            mc = mongo_config

        if isinstance(mc, dict):
            uri = mc.get("uri", mc.get("mongo_uri", uri))
            db_name = mc.get("db_name", mc.get("database", db_name))
        elif hasattr(mc, "uri") or hasattr(mc, "MONGO_URI"):
            uri = getattr(mc, "uri", getattr(mc, "MONGO_URI", uri))
            db_name = getattr(mc, "db_name", getattr(mc, "MONGO_DB", db_name))
    except Exception as e:
        logger.debug(f"config.mongo_config not directly loaded ({e}); using environment/defaults.")
        uri = os.getenv("MONGO_URI", uri)
        db_name = os.getenv("MONGO_DB", db_name)

    return uri, db_name


def get_mongo_db() -> Any:
    """Returns connected MongoDB database instance."""
    uri, db_name = _resolve_mongo_config()
    client = MongoClient(uri, serverSelectionTimeoutMS=5000, connectTimeoutMS=5000)
    return client[db_name]


def get_drug_signals(drug_name: str) -> Dict[str, Any]:
    """
    Retrieves the complete safety signal document and nested demographics for a given drug.

    Args:
        drug_name: Name of the drug (case-insensitive).

    Returns:
        Dictionary containing drug details, nested signals, and demographic profile.
        Returns empty dict if no matching record is found.
    """
    cleaned_name = drug_name.strip()
    logger.info(f"Retrieving MongoDB signals for drug: '{cleaned_name}'")

    db = get_mongo_db()
    coll = db["drug_signals"]

    # Match exact uppercase ID or case-insensitive regex
    pattern = re.compile(f"^{re.escape(cleaned_name)}$", re.IGNORECASE)
    doc = coll.find_one({
        "$or": [
            {"_id": cleaned_name.upper()},
            {"drug_name": pattern},
        ]
    })

    if not doc:
        # Check drug_summaries as fallback
        summary_coll = db["drug_summaries"]
        s_doc = summary_coll.find_one({
            "$or": [
                {"_id": cleaned_name.upper()},
                {"drug_name": pattern},
            ]
        })
        if s_doc:
            s_doc.pop("_id", None)
            return {"drug_name": cleaned_name.upper(), "demographics": s_doc, "signals": []}
        return {}

    # Clean ObjectId / non-serializable fields if needed
    if "_id" in doc:
        doc["_id"] = str(doc["_id"])
    if "updated_at" in doc:
        doc["updated_at"] = doc["updated_at"].isoformat() if hasattr(doc["updated_at"], "isoformat") else str(doc["updated_at"])

    # Sort signals inside document descending by PRR
    if "signals" in doc and isinstance(doc["signals"], list):
        doc["signals"] = sorted(
            doc["signals"],
            key=lambda x: (x.get("is_signal", False), x.get("prr", 0.0), x.get("a", 0)),
            reverse=True,
        )

    return doc


def get_top_signals(limit: int = 50) -> List[Dict[str, Any]]:
    """
    Retrieves global top adverse drug reactions across all monitored drugs,
    ordered by Proportional Reporting Ratio (PRR) and case volume.

    Args:
        limit: Maximum number of signals to retrieve (default: 50).

    Returns:
        List of signal dictionaries sorted by statistical significance.
    """
    logger.info(f"Querying top {limit} global safety signals from MongoDB...")

    db = get_mongo_db()
    coll = db["drug_signals"]

    pipeline = [
        {"$unwind": "$signals"},
        {"$match": {"signals.is_signal": True}},
        {
            "$sort": {
                "signals.prr": -1,
                "signals.a": -1,
                "signals.chi_square": -1,
            }
        },
        {"$limit": limit},
        {
            "$project": {
                "_id": 0,
                "drug_name": "$drug_name",
                "reaction": "$signals.reaction",
                "cases": "$signals.a",
                "drug_total": "$signals.drug_total",
                "reaction_total": "$signals.reaction_total",
                "prr": "$signals.prr",
                "chi_square": "$signals.chi_square",
                "is_signal": "$signals.is_signal",
                "death_count": "$signals.death_count",
                "fatality_rate": "$signals.fatality_rate",
            }
        },
    ]

    try:
        results = list(coll.aggregate(pipeline))
        return results
    except Exception as e:
        logger.error(f"Error aggregating top signals: {e}")
        return []


def search_drugs(query_string: str) -> List[Dict[str, Any]]:
    """
    Performs case-insensitive regex search on monitored drug names.

    Args:
        query_string: Substring or prefix to search for.

    Returns:
        List of matching drug summaries with report and signal counts.
    """
    cleaned_query = query_string.strip()
    logger.info(f"Searching drugs with query pattern: '{cleaned_query}'")

    db = get_mongo_db()
    coll = db["drug_signals"]

    pattern = re.compile(re.escape(cleaned_query), re.IGNORECASE)
    cursor = (
        coll.find(
            {"drug_name": pattern},
            {
                "_id": 0,
                "drug_name": 1,
                "total_signals": 1,
                "total_cases": 1,
                "total_deaths": 1,
            },
        )
        .sort("total_signals", -1)
        .limit(50)
    )

    results = []
    for doc in cursor:
        results.append({
            "drug_name": doc.get("drug_name", ""),
            "total_signals": doc.get("total_signals", 0),
            "total_cases": doc.get("total_cases", 0),
            "total_deaths": doc.get("total_deaths", 0),
        })

    return results


def get_drug_list() -> List[str]:
    """
    Retrieves an alphabetically sorted list of all unique drug names in MongoDB.

    Returns:
        List of distinct drug name strings.
    """
    logger.info("Fetching complete distinct drug list from MongoDB...")

    db = get_mongo_db()
    coll = db["drug_signals"]

    try:
        distinct_names = coll.distinct("drug_name")
        cleaned_list = sorted([str(name).strip().upper() for name in distinct_names if name])
        logger.info(f"Retrieved {len(cleaned_list):,} unique drugs from MongoDB.")
        return cleaned_list
    except Exception as e:
        logger.error(f"Failed to fetch drug list from MongoDB: {e}")
        return []
