# FAERS Pharmacovigilance Data Dictionary

This document provides a comprehensive technical reference for the data schemas across all stages of the **FDA Adverse Event Reporting System (FAERS)** Big Data Analytics Pipeline.

---

## 1. Raw OpenFDA FAERS JSON Schema

Raw FAERS adverse event reports are downloaded as partitioned JSON archives from the OpenFDA API (`https://api.fda.gov/download.json`). The top-level document contains `meta` and an array of `results`.

### 1.1 Report Header & Administrative Fields

| Field Name | Data Type | Nullable | Description | Sample Value |
| :--- | :--- | :--- | :--- | :--- |
| `safetyreportid` | `STRING` | No | Globally unique identifier assigned to the individual adverse event report. Primary key. | `"10004128"` |
| `safetyreportversion` | `STRING` | No | Version number of the safety report. Increments with follow-up case submissions. | `"1"`, `"2"` |
| `receivedate` | `STRING` | Yes | Date on which the FDA initially received the adverse event report (`YYYYMMDD`). | `"20230415"` |
| `occurcountry` | `STRING` | Yes | ISO 3166-1 alpha-2 two-letter code for the country where the adverse event took place. | `"US"`, `"GB"`, `"DE"` |
| `serious` | `STRING` | Yes | High-level binary indicator of adverse event seriousness (`1` = Serious, `2` = Not Serious). | `"1"` |
| `seriousnessdeath` | `STRING` | Yes | Seriousness criterion indicating the reaction resulted in patient death (`1` = Yes). | `"1"` |
| `seriousnesshospitalization` | `STRING` | Yes | Seriousness criterion indicating initial or prolonged inpatient hospitalization (`1` = Yes). | `"1"` |
| `seriousnesslifethreatening` | `STRING` | Yes | Seriousness criterion indicating the event placed the patient at immediate risk of death (`1` = Yes). | `"1"` |
| `seriousnessdisabling` | `STRING` | Yes | Seriousness criterion indicating substantial disability or permanent damage (`1` = Yes). | `"1"` |
| `seriousnesscongenitalanomali`| `STRING` | Yes | Seriousness criterion indicating a congenital anomaly or birth defect (`1` = Yes). | `"1"` |
| `seriousnessother` | `STRING` | Yes | Medically significant event requiring intervention to prevent one of the other outcomes (`1` = Yes). | `"1"` |

### 1.2 Patient Demographics (`patient`)

| Field Name | Data Type | Nullable | Description | Sample Value |
| :--- | :--- | :--- | :--- | :--- |
| `patientonsetage` | `STRING` | Yes | Numerical age of the patient at the onset of the reported adverse event. | `"65"`, `"18"` |
| `patientonsetageunit` | `STRING` | Yes | ICH E2B coded unit of measurement for `patientonsetage` (see Code Mappings). | `"801"`, `"800"` |
| `patientsex` | `STRING` | Yes | Biological sex of the patient reported using ICH E2B numeric codes. | `"1"`, `"2"`, `"0"` |
| `patientweight` | `STRING` | Yes | Patient body weight measured in kilograms (kg). | `"72.5"` |

### 1.3 Nested Drug Array (`patient.drug[]`)

| Field Name | Data Type | Nullable | Description | Sample Value |
| :--- | :--- | :--- | :--- | :--- |
| `medicinalproduct` | `STRING` | No | Reported trade or commercial name of the administered pharmaceutical product. | `"AVANDIA 4MG"` |
| `drugcharacterization` | `STRING` | No | Role assigned to the drug by the reporter in relation to the adverse event (see Code Mappings). | `"1"` (Primary Suspect) |
| `drugdosagetext` | `STRING` | Yes | Text description of drug dosage, frequency, and administration route. | `"4 MG DAILY ORAL"` |
| `drugindication` | `STRING` | Yes | Medical indication or underlying diagnosis for which the drug was prescribed. | `"TYPE 2 DIABETES"` |
| `openfda.generic_name` | `ARRAY<STRING>` | Yes | Harmonized FDA generic/active chemical ingredient name(s). | `["ROSIGLITAZONE"]` |
| `openfda.brand_name` | `ARRAY<STRING>` | Yes | Standardized commercial brand name(s) indexed by OpenFDA. | `["AVANDIA"]` |
| `openfda.substance_name`| `ARRAY<STRING>` | Yes | Chemical substance name(s) identified in drug composition. | `["ROSIGLITAZONE MALEATE"]` |

### 1.4 Nested Reaction Array (`patient.reaction[]`)

| Field Name | Data Type | Nullable | Description | Sample Value |
| :--- | :--- | :--- | :--- | :--- |
| `reactionmeddrapt` | `STRING` | No | MedDRA (Medical Dictionary for Regulatory Activities) Preferred Term (PT) describing the adverse event. | `"MYOCARDIAL INFARCTION"` |
| `reactionoutcome` | `STRING` | Yes | Coded clinical resolution outcome of the reaction (recovered, fatal, resolving, etc.). | `"1"`, `"2"`, `"5"` |

---

## 2. Cleaned & Flattened Intermediate Schema (`flattened_events`)

Generated during the distributed ETL phase and saved as compressed Apache Parquet. Nested arrays of drugs and reactions are exploded into 1-to-1 event rows with standardized data types.

| Column Name | PySpark Type | Storage Nullable | Description |
| :--- | :--- | :--- | :--- |
| `report_id` | `StringType` | No | Cleaned case report identifier (`safetyreportid`). |
| `report_version` | `StringType` | No | Latest case version retained after deduplication. |
| `received_date` | `StringType` | Yes | Ingestion receipt date (`YYYYMMDD`). |
| `country` | `StringType` | No | ISO country code or `"UNKNOWN"`. |
| `sex` | `StringType` | No | Standardized categorical sex: `"Male"`, `"Female"`, or `"Unknown"`. |
| `age_years` | `DoubleType` | Yes | Age converted to standardized decimal years (0.00 – 125.00). |
| `drug_name` | `StringType` | No | Uppercase normalized generic drug name (or brand name if generic unavailable). |
| `drug_role` | `StringType` | No | Categorical drug role: `"Primary Suspect"`, `"Secondary Suspect"`, `"Concomitant"`, `"Interacting"`. |
| `indication` | `StringType` | Yes | Uppercase normalized prescribing indication. |
| `reaction` | `StringType` | No | Uppercase normalized MedDRA Preferred Term (PT). |
| `reaction_outcome` | `StringType` | Yes | Raw coded reaction outcome value. |
| `is_serious` | `BooleanType`| No | Flag denoting overall seriousness (`serious == "1"`). |
| `is_fatal` | `BooleanType`| No | Flag denoting patient death (`seriousnessdeath == "1"`). |
| `is_hospitalization` | `BooleanType`| No | Flag denoting hospitalization (`seriousnesshospitalization == "1"`). |
| `is_life_threatening`| `BooleanType`| No | Flag denoting life-threatening event (`seriousnesslifethreatening == "1"`). |

---

## 3. PRR Analytical Output Schema (`safety_signals`)

Output generated by the Disproportionality Signal Detection engine (`analytics.prr_calculator`) and indexed in Apache Hive and MongoDB.

| Field Name | Data Type | Formula / Origin | Description |
| :--- | :--- | :--- | :--- |
| `drug_name` | `STRING` | Normalized generic drug name | Name of target primary suspect drug ($X$). |
| `reaction` | `STRING` | Normalized MedDRA PT | Name of target adverse event reaction ($Y$). |
| `a` | `BIGINT` | Co-occurrence count | Reports containing **both** Drug $X$ and Reaction $Y$. |
| `b` | `BIGINT` | `drug_total - a` | Reports containing Drug $X$ with **other** reactions. |
| `c` | `BIGINT` | `reaction_total - a` | Reports containing Reaction $Y$ with **other** drugs. |
| `d` | `BIGINT` | $N - (a + b + c)$ | Reports containing **other** drugs and **other** reactions. |
| `drug_total` | `BIGINT` | $a + b$ | Total distinct reports involving Drug $X$ as Primary Suspect. |
| `reaction_total` | `BIGINT` | $a + c$ | Total distinct reports involving Reaction $Y$. |
| `total_reports` | `BIGINT` | $N = a + b + c + d$ | Overall primary suspect reports in study population. |
| `prr` | `DOUBLE` | $\frac{a / (a + b)}{c / (c + d)}$ | Proportional Reporting Ratio point estimate (rounded to 4 decimals). |
| `chi_square` | `DOUBLE` | $\frac{N(ad - bc)^2}{(a+b)(c+d)(a+c)(b+d)}$ | Pearson Chi-square test statistic (1 degree of freedom). |
| `is_signal` | `BOOLEAN` | $\text{PRR} \ge 2 \land a \ge 3 \land \chi^2 \ge 4$ | Flag denoting a statistically significant pharmacovigilance safety signal. |
| `death_count` | `BIGINT` | Fatal case count | Distinct reports for pair $(X, Y)$ resulting in patient death. |
| `fatality_rate`| `DOUBLE` | $\frac{\text{death\_count}}{a}$ | Proportion of adverse event pair reports that were fatal. |

---

## 4. Code Value Mappings

### 4.1 Biological Sex Codes (`patientsex`)

Based on the International Council for Harmonisation (ICH) E2B(R2/R3) guidelines:

| Raw Code | Normalized String | Definition |
| :---: | :--- | :--- |
| `1` | `Male` | Patient is biological male |
| `2` | `Female` | Patient is biological female |
| `0` | `Unknown` | Sex was not reported or is unknown |
| `null` / Other | `Unknown` | Missing or invalid code value |

### 4.2 Age Unit Codes (`patientonsetageunit`)

Standard conversion factors applied to derive `age_years`:

| Raw Code | String Equivalent | Unit Name | Multiplier to Years | Calculation Formula |
| :---: | :--- | :--- | :---: | :--- |
| `800` | `DEC`, `DECADE` | Decade | $10.0$ | $\text{age} \times 10.0$ |
| `801` | `YR`, `YEAR` | Year | $1.0$ | $\text{age} \times 1.0$ |
| `802` | `MON`, `MONTH` | Month | $\frac{1}{12} \approx 0.08333$ | $\text{age} / 12.0$ |
| `803` | `WK`, `WEEK` | Week | $\frac{1}{52} \approx 0.01923$ | $\text{age} / 52.0$ |
| `804` | `DY`, `DAY` | Day | $\frac{1}{365.25} \approx 0.002737$ | $\text{age} / 365.25$ |
| `805` | `HR`, `HOUR` | Hour | $\frac{1}{8760} \approx 0.000114$ | $\text{age} / 8760.0$ |

> [!NOTE]
> Biologically implausible values (resulting in $\text{age\_years} < 0.0$ or $\text{age\_years} > 125.0$) are sanitized to `NULL` to prevent distortion in demographic stratifications.

### 4.3 Drug Characterization Codes (`drugcharacterization`)

Designates the causal suspicion level assigned to each medication:

| Raw Code | Normalized Descriptor | Epidemiological Definition |
| :---: | :--- | :--- |
| `1` / `PS` | `Primary Suspect` | The primary medicine suspected of causing the adverse event. Exclusively utilized for PRR signal detection. |
| `2` / `SS` | `Secondary Suspect` | Other suspect medicines co-administered and potentially contributing. |
| `3` / `C` | `Concomitant` | Medicines taken concurrently but not suspected of causing the adverse event. |
| `4` / `I` | `Interacting` | Medicines suspected of interacting with another agent to precipitate the event. |

### 4.4 Reaction Clinical Outcome Codes (`reactionoutcome`)

| Raw Code | Clinical Definition |
| :---: | :--- |
| `1` | Recovered / Resolved |
| `2` | Recovering / Resolving |
| `3` | Not recovered / Not resolved |
| `4` | Recovered with sequelae (permanent residual damage) |
| `5` | Fatal |
| `6` | Unknown |
