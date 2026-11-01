# Automated Early Detection of Adverse Drug Interactions
### *A Distributed Pharmacovigilance Big Data Analytics Platform on FDA FAERS*

[![Python 3.10+](https://img.shields.io/badge/python-3.10%2B-blue.svg)](https://www.python.org/)
[![PySpark 3.5](https://img.shields.io/badge/PySpark-3.5-orange.svg)](https://spark.apache.org/)
[![Apache Hive](https://img.shields.io/badge/Apache%20Hive-3.1-yellow.svg)](https://hive.apache.org/)
[![MongoDB](https://img.shields.io/badge/MongoDB-7.0-green.svg)](https://www.mongodb.com/)
[![Apache Kafka](https://img.shields.io/badge/Apache%20Kafka-3.5-black.svg)](https://kafka.apache.org/)
[![Streamlit](https://img.shields.io/badge/Streamlit-1.30-red.svg)](https://streamlit.io/)
[![License: MIT](https://img.shields.io/badge/License-MIT-lightgrey.svg)](LICENSE)

---

## 1. Executive Summary & Problem Statement

Adverse Drug Events (ADEs) represent the **4th leading cause of death in hospitalized patients**, exceeding pulmonary disease, diabetes, and automobile fatalities. In the United States alone, ADEs cause over **100,000 deaths annually** and incur more than **$136 billion** in healthcare costs.

Pre-market randomized controlled clinical trials typically monitor 1,000–3,000 patients under strict inclusion criteria. Consequently, they are statistically underpowered to detect:
1. **Rare adverse reactions** (incidence < 1:10,000)
2. **Long-latency chronic toxicities**
3. **Complex multi-drug interactions (polypharmacy DDIs)**
4. **Vulnerable sub-population vulnerabilities** (geriatric, pediatric, multi-morbid cohorts)

Post-marketing spontaneous reporting databases—principally the **FDA Adverse Event Reporting System (FAERS)**—aggregate reports from physicians, pharmacists, pharmaceutical manufacturers, and patients worldwide (>16 million cumulative records, expanding by >2 million reports annually).

Processing, cleansing, harmonizing, and analyzing this heterogeneous volume requires an enterprise-grade **Big Data Analytics Architecture** combining distributed computing (**PySpark**), analytical data warehousing (**Apache Hive**), low-latency document stores (**MongoDB**), real-time message brokering (**Apache Kafka**), and interactive clinical surveillance dashboards (**Streamlit**).

---

## 2. High-Level System Architecture

```mermaid
flowchart TD
    subgraph S1["1. DATA INGESTION LAYER"]
        D1["OpenFDA FAERS API<br/>(Bulk Quarterly JSON / 16M+ Reports)"]
        DL["data/download_faers.py<br/>(HTTP Stream + Unzip)"]
        RAW["Raw Landing Zone<br/>(HDFS / Local Data Lake)"]
        K_PROD["streaming/kafka_producer.py<br/>(Replay / Simulate Live Events)"]
        KAFKA["Apache Kafka<br/>Topic: faers-raw-events"]
        
        D1 --> DL --> RAW
        RAW -.-> K_PROD -.-> KAFKA
    end

    subgraph S2["2. DISTRIBUTED ETL & PROCESSING ENGINE"]
        INGEST["01_ingest.py<br/>• Schema Extraction<br/>• Multi-line JSON Parsing"]
        CLEAN["02_clean.py<br/>• Windowed Deduplication<br/>• Age Normalization (800-805)<br/>• Sex Code Standardization"]
        FLATTEN["03_flatten.py<br/>• Explode patient.drug[]<br/>• Explode patient.reaction[]<br/>• Generic Name Resolution"]
        PARQUET["Optimized Storage<br/>(Snappy-Compressed Parquet)"]

        RAW --> INGEST --> CLEAN --> FLATTEN --> PARQUET
        KAFKA -.-> SPARK_STREAM["spark_streaming_consumer.py<br/>(Micro-batch Streaming)"] -.-> PARQUET
    end

    subgraph S3["3. STATISTICAL SIGNAL ENGINE"]
        PRR_CALC["analytics/prr_calculator.py<br/>• 2x2 Contingency Matrix<br/>• PRR Ratio & Chi-Square (χ²)<br/>• Threshold: PRR ≥ 2, n ≥ 3, χ² ≥ 4"]
        DEMO["analytics/demographic_analysis.py<br/>• Age & Gender Distributions<br/>• Underrepresented Cohort Detection"]
        FATAL["analytics/fatality_analysis.py<br/>• Mortality Rate per Pair<br/>• Lethal Signal Ranking"]

        PARQUET --> PRR_CALC
        PARQUET --> DEMO
        PARQUET --> FATAL
    end

    subgraph S4["4. DUAL PERSISTENCE & WAREHOUSING LAYER"]
        direction TB
        subgraph OLAP["OLAP Warehouse"]
            HIVE["Apache Hive (pharma_db)<br/>• faers_events (Fact)<br/>• drug_reaction_signals<br/>• drug_summary"]
        end
        subgraph OLTP["NoSQL Serving Store"]
            MONGO["MongoDB (pharmacovigilance)<br/>• drug_signals Collection<br/>• Indexed on drug_name & PRR"]
        end

        PRR_CALC --> HIVE
        DEMO --> HIVE
        FATAL --> HIVE

        PRR_CALC --> MONGO
        DEMO --> MONGO
        FATAL --> MONGO
    end

    subgraph S5["5. SERVING & PRESENTATION LAYER (Streamlit)"]
        LOADER["data_loader.py<br/>(Tiered: Mongo ➔ Parquet ➔ Cached Demo)"]
        UI1["01_drug_search.py<br/>(Risk Signals & Demographics)"]
        UI2["02_drug_compare.py<br/>(Head-to-head Safety Profiling)"]
        UI3["03_top_signals.py<br/>(Global Prioritization Matrix)"]
        UI4["04_demographics.py<br/>(Vulnerable Cohort Blind Spots)"]

        MONGO --> LOADER
        HIVE -.-> LOADER
        LOADER --> UI1
        LOADER --> UI2
        LOADER --> UI3
        LOADER --> UI4
    end

    style S1 fill:#E3F2FD,stroke:#1565C0,stroke-width:2px
    style S2 fill:#FFF3E0,stroke:#E65100,stroke-width:2px
    style S3 fill:#FCE4EC,stroke:#880E4F,stroke-width:2px
    style S4 fill:#E8F5E9,stroke:#2E7D32,stroke-width:2px
    style S5 fill:#F3E5F5,stroke:#4A148C,stroke-width:2px
```

---

## 3. Disproportionality Signal Detection Theory

The core algorithmic engine detects unexpected adverse drug associations via the **Proportional Reporting Ratio (PRR)** (Evans et al., 2001), evaluating whether an adverse reaction is reported disproportionately more often for a specific drug than across all other pharmaceutical compounds in the repository.

### The 2x2 Contingency Matrix
For a given target drug $D$ and target adverse reaction $R$:

| | Adverse Reaction $R$ | All Other Reactions ($\neg R$) | Total Drug Reports |
|---|:---:|:---:|:---:|
| **Target Drug $D$** | **$a$** | **$b$** | $a + b$ |
| **All Other Drugs ($\neg D$)** | **$c$** | **$d$** | $c + d$ |
| **Total Population** | $a + c$ | $b + d$ | $N = a + b + c + d$ |

### Mathematical Formulations

1. **Proportional Reporting Ratio (PRR)**:
   $$\text{PRR} = \frac{a / (a + b)}{c / (c + d)}$$

2. **Standard Error & 95% Confidence Interval**:
   $$\text{SE}(\ln \text{PRR}) = \sqrt{\frac{1}{a} - \frac{1}{a + b} + \frac{1}{c} - \frac{1}{c + d}}$$
   $$\text{PRR}_{95\%\text{ CI}} = \left[ \exp\left(\ln(\text{PRR}) - 1.96 \cdot \text{SE}\right), \; \exp\left(\ln(\text{PRR}) + 1.96 \cdot \text{SE}\right) \right]$$

3. **Chi-Square ($\chi^2$) with Yates' Continuity Correction**:
   $$\chi^2 = \frac{N \left( \left| a \cdot d - b \cdot c \right| - \frac{N}{2} \right)^2}{(a + b)(c + d)(a + c)(b + d)}$$

### Validated Signal Detection Criteria (Evans Criteria)
An association between drug $D$ and reaction $R$ is classified as a **Statistically Validated Safety Signal** if and only if:
- $\text{PRR} \ge 2.0$
- Co-occurrence case count $a \ge 3$
- $\chi^2 \ge 4.0$ ($p < 0.05$ with 1 degree of freedom)

---

## 4. Technology Stack

| Component | Technology | Version | Purpose |
|---|---|---|---|
| **Distributed Processing** | Apache Spark / PySpark | 3.5.x | Distributed JSON parsing, array explosion, large-scale 2x2 matrix aggregation |
| **Data Warehousing** | Apache Hive | 3.1.x | Columnar Parquet analytical storage, HiveQL queries, schema metastore |
| **NoSQL Serving Store** | MongoDB | 7.0.x | Secondary compound indexed BSON store for sub-millisecond dashboard queries |
| **Message Brokering** | Apache Kafka | 3.5.x | High-throughput pub-sub queue for streaming incoming spontaneous ADE reports |
| **Coordination** | Apache Zookeeper | 7.5.x | Distributed cluster synchronization and Kafka broker consensus |
| **Interactive UI** | Streamlit + Plotly | 1.30+ | Healthcare-grade clinical analytics dashboard with high-contrast data visualization |
| **Runtime Environment** | Python | 3.10+ | Unified language across ETL, statistical engines, storage APIs, and UI |
| **Containerization** | Docker Compose | v2+ | Orchestration for multi-container infrastructure (MongoDB, Kafka, Zookeeper) |

---

## 5. Repository Structure

```
pharmacovigilance-faers/
|-- analytics/                  # Statistical Signal Detection & Epidemiological Analysis
|   |-- demographic_analysis.py  # Population stratification & age/gender disparity analytics
|   |-- fatality_analysis.py     # Case fatality rate (CFR) & severity scoring
|   |-- prr_calculator.py        # Evans PRR, Chi-square w/ Yates correction & 95% CI
|-- config/                     # Centralized cluster & infrastructure connectors
|   |-- hive_config.py           # Apache Hive metastore & HiveQL configuration
|   |-- kafka_config.py          # Kafka producer/consumer brokers & topic parameters
|   |-- mongo_config.py          # MongoDB client, compound index builders & TTL settings
|   |-- spark_config.py          # PySpark SparkSession factory & JVM tuning parameters
|-- dashboard/                  # Interactive Clinical Pharmacovigilance Platform
|   |-- app.py                   # Executive overview & multi-page navigation
|   |-- data_loader.py           # Multi-tier data loader (MongoDB -> Parquet -> Fallback)
|   |-- components/              # Reusable clinical UI design system
|   |   |-- charts.py            # Clinical Plotly visualizers (PRR bars, histograms, pies)
|   |   |-- signal_table.py      # Triage badges & interactive signal data tables
|   |   |-- theme.py             # Custom CSS, institutional typography & layout styles
|   |-- pages/                   # Surveillance domain sub-pages
|       |-- 01_drug_search.py    # Compound safety surveillance & Evans criteria audit
|       |-- 02_drug_compare.py   # Head-to-head comparative safety profiling
|       |-- 03_top_signals.py    # Global disproportionality signal triage registry
|       |-- 04_demographics.py   # Population disparity audit & age cohort breakdown
|-- data/                       # Local data directory
|   |-- raw/                     # Raw OpenFDA FAERS JSON archives (git-ignored)
|   |-- processed/               # Standardized Parquet partitions (signals & reports)
|-- docs/                       # Technical specifications & documentation
|   |-- data_dictionary.md       # ICH E2B & MedDRA regulatory data dictionary
|   |-- prr_methodology.md       # Mathematical & statistical derivation of Evans criteria
|-- etl/                        # Distributed Data Ingestion & Transformation
|   |-- 01_ingest.py             # JSON streaming reader & raw schema validation
|   |-- 02_clean.py              # Deduplication, age harmonization & sex mapping
|   |-- 03_flatten.py            # Exploding nested 1-to-N drug & adverse reaction arrays
|   |-- 04_validate.py           # Biological plausibility validation & quality gates
|-- pipeline/                   # End-to-End Orchestration & Execution Drivers
|   |-- run_full_pipeline.py     # Master orchestrator chaining Ingest -> PRR -> Storage
|-- storage/                    # Data Lakehouse & Operational Document Store
|   |-- hive_queries.py          # Analytical HiveQL queries & OLAP aggregations
|   |-- hive_writer.py           # Parquet partitioning & Hive external table registration
|   |-- mongo_queries.py         # Sub-millisecond aggregation pipeline queries
|   |-- mongo_writer.py          # Document upserting & compound indexing
|-- streaming/                  # Real-Time Event Ingestion & Micro-batch Processing
|   |-- kafka_producer.py        # Synthetic & replay adverse event Kafka streaming producer
|   |-- spark_streaming_consumer.py # Structured Streaming micro-batch processor w/ checkpointing
|-- tests/                      # Automated Verification & Statistical Test Suites
|   |-- test_cleaning.py         # Data harmonization & deduplication validation
|   |-- test_prr_calculator.py   # Statistical contingency table & Evans criteria verification
|   |-- test_data/               # Canonical FAERS test fixtures
|-- docker-compose.yml          # Infrastructure orchestration (MongoDB, Kafka, Zookeeper)
|-- requirements.txt            # Python environment specifications
|-- .gitignore                  # Production Git ignore rules
`-- README.md                   # System documentation & architectural reference
```

---

## 6. Installation & Quick Start

### 6.1 Clone Repository & Setup Environment
```bash
git clone https://github.com/ranjanaashish/pharmacovigilance-faers.git
cd pharmacovigilance-faers

# Create virtual environment
python -m venv venv

# Activate virtual environment
# Windows:
.\venv\Scripts\activate
# Linux / macOS:
source venv/bin/activate

# Install dependencies
pip install --upgrade pip
pip install -r requirements.txt
```

### 6.2 Start Supporting Infrastructure (Optional)
To run live MongoDB and Apache Kafka:
```bash
docker-compose up -d
```
Verify running containers:
```bash
docker-compose ps
```

### 6.3 Download FAERS Quarterly Data
Download raw OpenFDA FAERS JSON partitions:
```bash
python data/download_faers.py --partitions 5
```

### 6.4 Execute Data Processing & Signal Detection
Run the master distributed processing pipeline:
```bash
python pipeline/run_full_pipeline.py
```
*Note: For environments without local JVM/Hadoop binary configurations, the pipeline includes a high-performance vectorized pure-Python/Pandas fallback (`data/build_real_processed_data.py`) generating identical Parquet schemas.*

### 6.5 Launch Interactive Clinical Dashboard
```bash
python -m streamlit run dashboard/app.py
```
Open your browser to `http://localhost:8501`.

---

## 7. Real-Time Streaming Telemetry

To test real-time spontaneous adverse drug report ingestion:

```bash
# Terminal 1: Publish simulated ADE reports to Kafka topic 'faers-raw-events'
python streaming/kafka_producer.py --rate 5

# Terminal 2: Ingest, standardize, and append micro-batches via Structured Streaming
python streaming/spark_streaming_consumer.py
```

---

## 8. References

1. **Evans, S. J., Waller, P. C., & Davis, S.** (2001). *Use of proportional reporting ratios (PRRs) for signal generation from spontaneous adverse drug reaction reports*. **Pharmacoepidemiology and Drug Safety**, 10(6), 483-486.
2. **Bate, A., & Evans, S. J.** (2009). *Quantitative signal detection using spontaneous ADR reporting*. **Investigational New Drugs**, 27(6), 527-536.
3. **U.S. Food and Drug Administration (FDA)**. *FDA Adverse Event Reporting System (FAERS) Public Dashboard and OpenFDA API Documentation*. https://open.fda.gov/data/faers/
4. **Zaharia, M., et al.** (2016). *Apache Spark: A unified engine for big data processing*. **Communications of the ACM**, 59(11), 56-65.
