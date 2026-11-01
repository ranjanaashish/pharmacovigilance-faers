"""MongoDB configuration and client management for the Pharmacovigilance serving layer.

Provides connection pooling, database retrieval, and index initialization
for real-time dashboard queries and analytical retrieval.
"""

import os
from typing import Optional
from pymongo import MongoClient, ASCENDING, DESCENDING
from pymongo.database import Database


# Connection URI and Database Name
MONGO_URI: str = os.getenv("MONGO_URI", "mongodb://localhost:27017")
MONGO_DB: str = os.getenv("MONGO_DB", "pharmacovigilance")

# Collection Names
COLLECTION_SIGNALS: str = "safety_signals"
COLLECTION_DRUGS: str = "drug_summaries"
COLLECTION_REACTIONS: str = "reaction_summaries"
COLLECTION_ALERTS: str = "critical_alerts"
COLLECTION_DEMOGRAPHICS: str = "patient_demographics"
COLLECTION_RAW_REPORTS: str = "raw_sample_reports"


def get_mongo_client(
    uri: Optional[str] = None,
    server_selection_timeout_ms: int = 5000,
    connect_timeout_ms: int = 5000,
) -> MongoClient:
    """Create and return a thread-safe PyMongo MongoClient.

    Parameters
    ----------
    uri : str, optional
        MongoDB connection URI. Defaults to MONGO_URI env or localhost.
    server_selection_timeout_ms : int, default=5000
        Server selection timeout in milliseconds.
    connect_timeout_ms : int, default=5000
        Socket connection timeout in milliseconds.

    Returns
    -------
    pymongo.MongoClient
        Active MongoDB client with connection pooling.
    """
    effective_uri = uri or MONGO_URI
    return MongoClient(
        effective_uri,
        serverSelectionTimeoutMS=server_selection_timeout_ms,
        connectTimeoutMS=connect_timeout_ms,
    )


def get_database(
    db_name: Optional[str] = None,
    client: Optional[MongoClient] = None,
) -> Database:
    """Retrieve a reference to the target Pharmacovigilance MongoDB database.

    Parameters
    ----------
    db_name : str, optional
        Database name to access. Defaults to MONGO_DB.
    client : MongoClient, optional
        Existing MongoClient instance. If None, instantiates a new one.

    Returns
    -------
    pymongo.database.Database
        MongoDB database instance.
    """
    effective_client = client or get_mongo_client()
    effective_db_name = db_name or MONGO_DB
    return effective_client[effective_db_name]


def init_mongo_indexes(db: Optional[Database] = None) -> None:
    """Create optimal query indexes on all core collections to support low-latency dashboard queries.

    Indexes Created:
    - `safety_signals`:
        - Compound Unique Index: `(drug_name, reaction_meddra)`
        - Filter Index: `(is_signal, prr)` descending
        - Search Indexes: `drug_name`, `reaction_meddra`, `chi_square`
    - `drug_summaries`:
        - Unique Index: `drug_name`
        - Metric Index: `total_cases` descending
    - `reaction_summaries`:
        - Unique Index: `reaction_meddra`
    - `critical_alerts`:
        - Date Index: `detected_at` descending
    """
    target_db = db or get_database()

    # Safety signals indexes
    signals_col = target_db[COLLECTION_SIGNALS]
    signals_col.create_index(
        [("drug_name", ASCENDING), ("reaction_meddra", ASCENDING)],
        unique=True,
        name="idx_unique_drug_reaction"
    )
    signals_col.create_index(
        [("is_signal", ASCENDING), ("prr", DESCENDING)],
        name="idx_signal_prr"
    )
    signals_col.create_index([("drug_name", ASCENDING)], name="idx_drug_name")
    signals_col.create_index([("reaction_meddra", ASCENDING)], name="idx_reaction")
    signals_col.create_index([("chi_square", DESCENDING)], name="idx_chi_square")

    # Drug summaries indexes
    drugs_col = target_db[COLLECTION_DRUGS]
    drugs_col.create_index([("drug_name", ASCENDING)], unique=True, name="idx_unique_drug")
    drugs_col.create_index([("total_cases", DESCENDING)], name="idx_drug_cases")

    # Reaction summaries indexes
    reactions_col = target_db[COLLECTION_REACTIONS]
    reactions_col.create_index([("reaction_meddra", ASCENDING)], unique=True, name="idx_unique_reaction")

    # Critical alerts index
    alerts_col = target_db[COLLECTION_ALERTS]
    alerts_col.create_index([("detected_at", DESCENDING)], name="idx_alerts_timestamp")


if __name__ == "__main__":
    print(f"Connecting to MongoDB at {MONGO_URI} (DB: {MONGO_DB})...")
    try:
        test_client = get_mongo_client()
        server_info = test_client.server_info()
        print(f"MongoDB connected successfully! Version: {server_info.get('version')}")
        test_db = get_database(client=test_client)
        init_mongo_indexes(test_db)
        print("Indexes successfully ensured.")
    except Exception as exc:
        print(f"MongoDB connection notice: {exc}")
        print("Ensure MongoDB is running via `docker-compose up -d mongo`")
