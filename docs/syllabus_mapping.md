# ED547 Big Data Analytics: Syllabus Mapping & Academic Alignment

This document outlines the direct mapping between the **ED547 Big Data Analytics** course syllabus (Modules I through V) and the technical implementations of the **FAERS Pharmacovigilance Big Data Platform**.

---

## Master Course Mapping Table

| Module | Syllabus Topic Covered | Project Component | Implementation File(s) | How It Demonstrates the Concept |
| :---: | :--- | :--- | :--- | :--- |
| **Module I** | **Foundations of Big Data & the 5 V's**<br>• High-volume unstructured data<br>• Data ingestion architectures<br>• Heterogeneous schemas | Bulk Data Acquisition & Ingestion Pipeline | • [`data/download_faers.py`](../data/download_faers.py)<br>• [`docs/data_dictionary.md`](../docs/data_dictionary.md) | Ingests multi-gigabyte OpenFDA FAERS quarterly partitions (~16M reports). Demonstrates **Volume** (millions of records), **Velocity** (streaming updates), **Variety** (deeply nested JSON with polymorphic arrays), **Veracity** (spontaneous reporting data cleansing), and **Value** (life-saving drug safety signal discovery). |
| **Module II** | **Distributed Computing & Apache Spark**<br>• PySpark DataFrames API<br>• Catalyst query optimization<br>• Distributed transformations & actions<br>• Memory management & caching | Distributed ETL & Pharmacovigilance Analytics Engine | • [`analytics/prr_calculator.py`](../analytics/prr_calculator.py)<br>• [`config/spark_config.py`](../config/spark_config.py)<br>• [`tests/test_cleaning.py`](../tests/test_cleaning.py) | Leverages pure PySpark DataFrames API (avoiding slow RDDs). Applies columnar operations (`explode`, `groupBy`, `join`, `when`), window functions for deduplication, and in-memory caching (`df.cache()`) to evaluate pairwise $2 \times 2$ contingency tables across millions of rows without memory exhaustion. |
| **Module III** | **Big Data Warehousing & Apache Hive**<br>• Columnar formats (Parquet)<br>• Hive metastore & HiveQL<br>• Star/Snowflake modeling & partitioning | Analytical Data Warehouse & Query Serving | • [`config/hive_config.py`](../config/hive_config.py)<br>• [`pipeline/run_full_pipeline.py`](../pipeline/run_full_pipeline.py) | Implements columnar Parquet storage with dictionary encoding and Snappy compression. Registers Hive tables (`faers_pharmacovigilance.flattened_events`, `safety_signals`, `patient_demographics`) via HiveServer2 and PySpark catalog, enabling large-scale analytical OLAP queries. |
| **Module IV** | **NoSQL Databases & Semi-Structured Data**<br>• Document data model (BSON/JSON)<br>• Low-latency serving layer<br>• Secondary indexing & compound keys | Operational Document Store (MongoDB) | • [`config/mongo_config.py`](../config/mongo_config.py)<br>• [`pipeline/run_full_pipeline.py`](../pipeline/run_full_pipeline.py) | Bridges distributed big data output to an operational NoSQL store. Sets up compound unique indexes on `(drug_name, reaction_meddra)` and descending metric indexes on `prr` and `chi_square` to deliver sub-millisecond query responses for interactive clinician lookups. |
| **Module V** | **Real-Time Streaming & Interactive Visual Analytics**<br>• Distributed pub-sub (Apache Kafka)<br>• Structured Streaming micro-batches<br>• Checkpointing & fault tolerance<br>• Real-time web dashboards | Real-Time Ingestion & Clinician Dashboard | • [`streaming/kafka_producer.py`](../streaming/kafka_producer.py)<br>• [`streaming/spark_streaming_consumer.py`](../streaming/spark_streaming_consumer.py)<br>• [`docs/prr_methodology.md`](../docs/prr_methodology.md) | Simulates live adverse event reports published to Kafka broker topic `faers-raw-events`. PySpark Structured Streaming processes micro-batches every 30 seconds with fault-tolerant checkpointing. Output feeds interactive visualization dashboards for real-time safety monitoring. |

---

## Detailed Module Demonstrations

### Module I: Big Data Landscape & Ingestion Systems
- **Core Concept**: Efficiently capturing and cataloging high-velocity, high-volume semi-structured payloads.
- **Application in Project**: 
  - The script [`data/download_faers.py`](../data/download_faers.py) queries the live OpenFDA API download manifest (`download.json`), orchestrates multi-part HTTP downloads with byte chunk streaming and exponential backoff retry mechanisms, and automatically unpacks zipped JSON archives into `data/raw/`.
  - Schema heterogeneity is addressed by providing a formal data dictionary ([`docs/data_dictionary.md`](../docs/data_dictionary.md)) standardizing ICH E2B international regulatory terminology.

### Module II: Scalable Distributed Analytics with PySpark
- **Core Concept**: Parallel processing, distributed DAG construction, in-memory transformations, and statistical calculation over partitions.
- **Application in Project**:
  - Evans et al. Disproportionality Signal Detection ([`analytics/prr_calculator.py`](../analytics/prr_calculator.py)) converts an exhaustive $O(M \times N)$ combinatorial explosion of millions of drugs and adverse reactions into vectorized Spark SQL aggregation pipelines.
  - Deduping across case revisions via window specifications:
    ```python
    Window.partitionBy("safetyreportid").orderBy(col("safetyreportversion").desc())
    ```
  - Vectorized calculation of Pearson Chi-square ($\chi^2$) and PRR without resorting to row-by-row iteration or single-machine memory constraints.

### Module III: Enterprise Columnar Storage & Hive Warehousing
- **Core Concept**: Analytical query engines, schema-on-read, partition pruning, and file format optimization.
- **Application in Project**:
  - The orchestrator ([`pipeline/run_full_pipeline.py`](../pipeline/run_full_pipeline.py)) serializes intermediate and final analytical products to columnar Apache Parquet.
  - Integration with Apache Hive ([`config/hive_config.py`](../config/hive_config.py)) registers database schemas, external table pointers, and views, enabling business intelligence tools and downstream SQL analysts to query curated pharmacovigilance data tables directly.

### Module IV: NoSQL Document Storage & Serving Layer (Lambda Architecture)
- **Core Concept**: Hybrid big data architectures, decoupled serving layers, horizontal scaling, and index optimization for OLTP workloads.
- **Application in Project**:
  - High-throughput batch results are exported into MongoDB collections ([`config/mongo_config.py`](../config/mongo_config.py)).
  - Compound indexing on `(drug_name, reaction_meddra)` supports instantaneous search for safety signals, enabling front-end web applications (such as Streamlit) to display drug profiles without triggering slow distributed map-reduce queries.

### Module V: Distributed Event Streaming & Continuous Analytics
- **Core Concept**: Pub-sub architectures, streaming ingestion, windowed triggers, stateful stream processing, and checkpoint guarantees.
- **Application in Project**:
  - [`streaming/kafka_producer.py`](../streaming/kafka_producer.py) publishes raw adverse event records into Kafka topic `faers-raw-events` with fine-grained throttling.
  - [`streaming/spark_streaming_consumer.py`](../streaming/spark_streaming_consumer.py) runs PySpark Structured Streaming with a 30-second micro-batch trigger, schema parsing with `from_json()`, real-time normalization, and continuous append writes to Parquet with fault-tolerant checkpointing.

---

## Pedagogical Assessment Rubric

| Criterion | Target Performance | Demonstrated Proof in Codebase |
| :--- | :--- | :--- |
| **System Integration** | Seamless end-to-end execution across components | [`pipeline/run_full_pipeline.py`](../pipeline/run_full_pipeline.py) with shell/batch wrappers ([`pipeline/run_pipeline.sh`](../pipeline/run_pipeline.sh), [`pipeline/run_pipeline.bat`](../pipeline/run_pipeline.bat)). |
| **Code Quality & Testing** | Comprehensive unit tests, fixtures, and documentation | Pytest test suite with statistical benchmarks ([`tests/test_prr_calculator.py`](../tests/test_prr_calculator.py), [`tests/test_cleaning.py`](../tests/test_cleaning.py)) and realistic sample data ([`tests/test_data/sample_faers.json`](../tests/test_data/sample_faers.json)). |
| **Theoretical Rigor** | Accurate mathematical and epidemiological derivations | Detailed PRR methodology guide with LaTeX formulas ([`docs/prr_methodology.md`](../docs/prr_methodology.md)). |
| **Production Readiness** | Error handling, telemetry, and configurable logging | Granular step timing, try/except safeguards, and progress tracking in all modules. |
