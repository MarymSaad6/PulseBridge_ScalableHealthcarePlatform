# 🫀 PulseBridge
### Cloud-Based Healthcare Analytics Platform & Data Lakehouse



> **PulseBridge** is an end-to-end, cloud-native healthcare data lakehouse built on AWS that transforms raw clinical records from the MIMIC-III database into actionable, FHIR-compliant analytics. It combines a medallion lakehouse architecture (Bronze → Silver → Gold) with a real-time streaming pipeline to power operational ICU monitoring dashboards in Grafana and executive-level insights in Power BI.

---

## 📋 Table of Contents

- [Overview](#-overview)
- [Architecture](#-architecture)
  - [Primary Pipeline — Medallion Lakehouse](#primary-pipeline--medallion-lakehouse-aws-glue--athena--power-bi)
  - [Secondary Pipeline — Streaming & ETL](#secondary-pipeline--streaming--etl-kafka--spark--postgresql--grafana)
- [Key Features](#-key-features)
- [Dashboards & Visualizations](#-dashboards--visualizations)
  - [Grafana — Real-Time ICU Monitoring](#grafana--real-time-icu-monitoring)
  - [Power BI — Executive Analytics](#power-bi--executive-analytics)
- [Tech Stack](#-tech-stack)
- [Data Source — MIMIC-III](#-data-source--mimic-iii)
- [Data Model](#-data-model)


---

## 🔍 Overview

Healthcare data is fragmented, high-volume, and time-critical. PulseBridge solves this by implementing a **dual-pipeline architecture** on AWS that:

1. **Ingests** raw MIMIC-III clinical CSVs from a PhysioNet source database.
2. **Transforms** them through a FHIR-compliant medallion lakehouse (Bronze → Silver → Gold) using AWS Glue, producing structured fact and dimension tables queryable via Amazon Athena.
3. **Streams** selected high-frequency clinical events (output events, chart events) through Kafka → Spark ETL → PostgreSQL for sub-minute operational latency.
4. **Visualizes** the results through two complementary BI layers: Grafana for real-time ICU monitoring and Power BI for strategic population-level analytics.

The platform processes data for **~47K+ patients**, **59K+ admissions**, **212+ unique clinical measures**, and supports multi-year, multi-quarter filtering with drill-down to the individual ICU stay level.

---

## 🏗️ Architecture


<img width="1365" height="531" alt="WhatsApp Image 2026-06-21 at 8 41 21 AM" src="https://github.com/user-attachments/assets/929b616c-9fb3-4c7a-8f0d-ad28ce00860e" />



### Primary Pipeline — Medallion Lakehouse (AWS Glue + Athena + Power BI)

The primary pipeline implements a classic **Bronze → Silver → Gold** data lakehouse pattern, adapted for healthcare data with HL7 FHIR compliance at the Silver layer.

| Layer | Storage | Transformation | Output |
|---|---|---|---|
| **Bronze** | S3 Raw Bucket | Raw ingestion, no changes | MIMIC-III CSVs as-is |
| **Silver** | S3 Mapped Bucket | AWS Glue FHIR Mapping Job | 26 FHIR-standardized tables (Relational → FHIR) |
| **Gold** | S3 Ready Bucket | AWS Glue DWH Modeling Job | Galaxy Schema — Fact Tables + Dimension Tables |
| **Query** | Glue Data Catalog | Athena Serverless SQL | Ad-hoc SQL interface for Power BI |

**FHIR Mapping** at the Silver layer converts MIMIC-III relational tables into HL7 FHIR R4 resource representations, enabling interoperability and standards compliance. The 26 mapped tables cover Patient, Encounter, Observation, Condition, MedicationAdministration, Procedure, and more.

**Galaxy Schema** at the Gold layer organizes data into a query-optimized star/galaxy schema with central fact tables (`fact_admissions`, `fact_chartevents`, `fact_outputevents`) linked to shared dimension tables (`dim_patients`, `dim_diagnoses`, `dim_icu_stays`, `dim_items`, `dim_time`).

### Secondary Pipeline — Streaming & ETL (Kafka + Spark + PostgreSQL + Grafana)

The secondary pipeline handles the **high-frequency, low-latency** clinical data streams that require near-real-time availability for ICU monitoring.


Key design decisions in the secondary pipeline:

- **Kafka on EC2** provides durable, ordered, partitioned event streaming. Selected MIMIC-III tables (chartevents, outputevents) are chunked by a Glue Notebook and published as data batches to Kafka topics.
- **Spark ETL in Docker** runs on EC2 with containerized isolation, performing data cleaning (type coercion, null handling, deduplication), feature engineering (rolling averages, derived vitals), and direct load into PostgreSQL.
- **PostgreSQL on EC2** serves as the operational analytical store optimized for Grafana's time-series and per-patient queries, with indexes on `(subject_id, icustay_id, charttime)`.

---

## ✨ Key Features

- **Dual-pipeline architecture** — batch lakehouse for historical/strategic analytics, streaming pipeline for operational/real-time monitoring
- **FHIR R4 compliance** — Silver layer produces HL7 FHIR-mapped tables, enabling interoperability with EHR systems
- **Galaxy schema data warehouse** — Gold layer optimized for BI tools with pre-computed aggregations
- **Per-patient, per-ICU-stay drill-down** — Grafana dashboards filter to individual `subject_id`, `icustay_id`, and `Output_item` with dynamic variable injection
- **212+ unique clinical measures** tracked across vital signs, fluid outputs, medications, and procedures
- **Multi-year, multi-quarter filtering** in Power BI (2100, 2101, 2102 across Q1–Q4)
- **Real-time ICU event monitoring** with hourly trend lines, output distribution charts, and gauge indicators
- **Population-level analytics** covering 47K+ patients, 59K+ admissions, diagnosis distributions, mortality rates, and ICU performance
- **Containerized Spark ETL** for reproducible, portable processing
- **Serverless querying** via Amazon Athena — no cluster management for the primary analytical workload

---

## 📊 Dashboards & Visualizations

### Grafana — Real-Time ICU Monitoring

Two operational Grafana dashboards provide per-patient, per-stay clinical visibility:

#### ChartEvents Dashboard
Monitors high-frequency vital sign recordings for a specific ICU stay.

<img src="Streaming Pipeline/Grafana Dashboards/Chartevents/Dashboard1.1.jpeg" alt="Chartevents Dashboard 1" width="800">

<img src="Streaming Pipeline/Grafana Dashboards/Chartevents/Dashboard1.2.jpeg" alt="Chartevents Dashboard 2" width="800">


- **Top Output Items** — ranked horizontal bar chart showing the most recorded chart items (Heart Rate: 19,276 events; Non-Invasive Blood Pressure systolic: 16,183; O2 saturation: 16,106)
- **Total Events** — real-time KPI card (e.g., **5,560** events for the filtered stay)
- **Unique Measures** — count of distinct clinical measures recorded (**212**)
- **Hourly Output Trend by Item** — time-series line chart across the ICU stay window
- **Output Distribution** — normalized stacked bar chart showing avg/max/min output per selected item

**Dashboard Variables:** `query0`, `subject_id`, `icustay_id`, `Output_item` — all dynamically injectable via Grafana template variables.

**Example Query Window:** `2104-09-24 03:00:00` to `2104-09-26 03:00:00`

#### OutputEvents Dashboard
Monitors fluid and clinical output measurements.

<img src="Streaming Pipeline/Grafana Dashboards/Outputevents/Dashboard2.1.jpeg" alt="Outputevents Dashboard 1" width="800">

<img src="Streaming Pipeline/Grafana Dashboards/Outputevents/Dashboard2.2.jpeg" alt="Outputevents Dashboard 2" width="800">


- **Top Output Items** — Urine Out Foley (10,902), Chest Tubes CTICU CT 2 (1,390), Chest Tubes CTICU CT 1 (760)
- **Total Events** — **244** for the filtered stay
- **Monitored Days / Hours** — **7.14 days / 171 hours**
- **Average Hourly Output Gauge** — radial gauge showing **87.7** avg hourly output
- **Hourly Output Trend** — time-series over the full monitoring window (March–November span for this stay)
- **Output Distribution** — per-item avg/max/min comparison

**Example:** `subject_id: 10027`, `icustay_id: 286020`, monitoring window `2190-03-21` to `2190-11-08`



### Power BI — Executive Analytics

Four interlinked Power BI report pages provide population-level strategic insight, all filterable by **Year** (2100–2102) and **Quarter** (Q1–Q4).


#### Patient Page
High-level demographic overview of the patient population.

<img src="Batch Pipeline/Dashboard/Dash1.jpg" alt="Patient Page Dashboard" width="800">


| Metric | Value |
|---|---|
| Average Age | 66 |
| Total Patients | 47K |
| Total Diagnoses | 16K |

- **Patient Distribution by Race** — pie chart (White: 80.53%, African American: 11.26%, Hispanic/Latino: 4.17%, Asian: 3.91%, Native American: 0.14%)
- **Patient Distribution by Status** — pie chart (Married: 49.99%, Single: 27.33%, Widowed: 14.87%, Divorced: 6.63%, Separated: 1.18%)
- **Patient Distribution by Gender** — donut chart (Female: 56.15% / 26.1K; Male: 43.85% / 20.4K)
- **Patient Distribution by Age Group** — bar chart (Elderly/Old: 31.1K, Adult: 30.9K, Child: 8.2K)

#### Diagnosis Distribution Page
Clinical diagnosis analysis broken down by demographic cohorts.

<img src="Batch Pipeline/Dashboard/Dash2.jpg" alt="Patient Page Dashboard" width="800">

- **Top 10 Diagnoses by Marital Status** — stacked bar chart. Leading diagnoses: Pneumonia (1,500+), Sepsis (~1,100), Congestive Heart Failure, Coronary Artery Disease, Chest Pain
- **Top 10 Diagnoses by Race** — stacked bar chart showing racial distribution within each diagnostic category; White patients dominate across all diagnoses reflecting population demographics

#### Admissions Page
Operational admissions analytics.

<img src="Batch Pipeline/Dashboard/Dash3.jpg" alt="Patient Page Dashboard" width="800">

| Metric | Value |
|---|---|
| Total Admissions | 59K |
| Average Length of Stay (LOS) | 5 days |

- **Patient Mortality Rate** — donut chart (Alive: 89.71% / 65.84K; Dead: 10.29% / 7.56K)
- **Total Admissions by Type** — bar chart (Emergency: ~40K dominant; Newborn, Elective, Urgent in descending order)
- **Admissions Over Time** — area line chart by month (range: 5,822–6,435; peak in August at 6,435)
- **Top 10 Used Medications** — horizontal bar chart by patient percentage (NaCl 0.9% and Dextrose 5% are the most administered, followed by propofol, Insulin-Regular, Norepinephrine, fentanyl)

#### ICU Page
ICU-specific operational performance metrics.

<img src="Batch Pipeline/Dashboard/Dash4.jpg" alt="Patient Page Dashboard" width="800">


- **Top 10 Charts Produced in ICUs** — horizontal bar chart by total patients (Heart Rate, Respiratory Rate, O2 Saturation, and Non-Invasive Blood Pressure variants dominate; range 0.1M–0.4M)
- **Average LOS by ICU Unit** — combo chart (bar = Average LOS, line = Total Patients). Units: NICU (highest LOS ~10), SICU (~5), TSICU (~3.5), MICU (~3.5, highest patient volume), CSRU (~3), CCU (~2.5)
- **ICU Rush Hours** — hourly patient activity line chart; activity peaks between 09:00–22:00 (~73.4K at peak hours), drops to minimum around 05:00–06:00
- **Heart Rate vs. ICU Length of Stay** — scatter plot showing relationship between avg heart rate (50–150 bpm) and avg LOS (0–45 days); median heart rate cluster around 75–100 bpm with most stays under 10 days; outliers extend to 40+ days

---

## 🛠️ Tech Stack

| Category | Technology | Role |
|---|---|---|
| **Cloud Platform** | AWS | Primary infrastructure |
| **Object Storage** | Amazon S3 | Bronze / Silver / Gold data layers |
| **ETL / Data Integration** | AWS Glue | FHIR mapping, DWH modeling, notebook prep |
| **Serverless Query** | Amazon Athena | SQL interface over Gold layer |
| **Metadata** | AWS Glue Data Catalog | Schema registry and table metadata |
| **Event Streaming** | Apache Kafka (EC2) | High-frequency clinical event streaming |
| **Batch Processing** | Apache Spark (Docker, EC2) | Data cleaning, feature prep, warehouse load |
| **Analytical Store** | PostgreSQL (EC2) | Operational store for Grafana |
| **BI / Visualization** | Power BI | Population analytics, executive dashboards |
| **BI / Monitoring** | Grafana | Real-time ICU monitoring dashboards |
| **Data Standard** | HL7 FHIR R4 | Clinical data interoperability standard |
| **Data Source** | MIMIC-III (PhysioNet) | De-identified clinical database |
| **Containerization** | Docker | Spark ETL isolation and portability |

---

## 📁 Data Source — MIMIC-III

MIMIC-III (Medical Information Mart for Intensive Care III) is a large, freely available de-identified health database maintained by MIT Lab for Computational Physiology. It contains data for **over 40,000 patients** admitted to the Beth Israel Deaconess Medical Center ICUs between 2001 and 2012.

> **Access Requirement:** MIMIC-III requires completing a credentialing process and signing a data use agreement through [PhysioNet](https://physionet.org/content/mimiciii/). You must complete CITI training and obtain approval before accessing the data.

Key MIMIC-III tables used in PulseBridge:

| Table | Description | Pipeline |
|---|---|---|
| `PATIENTS` | Demographics, DOB, gender | Both |
| `ADMISSIONS` | Hospital admissions, discharge disposition | Both |
| `ICUSTAYS` | ICU stay records per admission | Both |
| `CHARTEVENTS` | Vital signs and clinical observations (~330M rows) | Secondary (Streaming) |
| `OUTPUTEVENTS` | Fluid outputs (urine, drains, etc.) | Secondary (Streaming) |
| `DIAGNOSES_ICD` | ICD-9 diagnosis codes per admission | Primary |
| `PRESCRIPTIONS` | Medication administrations | Primary |
| `D_ITEMS` | Dictionary of chart/output item definitions | Both |
| `D_ICD_DIAGNOSES` | ICD-9 diagnosis code descriptions | Primary |
| `LABEVENTS` | Laboratory measurements | Primary |
| `PROCEDUREEVENTS_MV` | Procedure events from MetaVision | Primary |


## 📐 Data Model (Dimensional Modeling & FHIR Compliance)

PulseBridge transitions healthcare data from highly normalized, siloed relational tables into an optimized analytical **Galaxy Schema** (multiple fact tables sharing conformed dimensions) structured around **HL7 FHIR R4** concepts. 

The dimension tables implement **SCD Type 1 (Slowly Changing Dimensions)** to guarantee that patient demographics and clinical metadata remain updated without generating redundant historical rows, maintaining strict referential integrity across both batch and streaming layers.

<img width="1231" height="630" alt="WhatsApp Image 2026-06-21 at 7 51 11 AM" src="https://github.com/user-attachments/assets/57d5f69e-f720-490e-8046-871bae5620ec" />

### 👥 Shared Dimension Tables (SCD Type 1)
- **`dim_patient`** *(mapped to FHIR Patient)*: Contains unique patient demographics including `subject_id` (Natural Key), `gender`, `dob`, `dod` (date of death), `age`, `age_group`, and `race`.
- **`dim_admission`** *(mapped to FHIR Encounter)*: Tracks encounter metadata including `hadm_id`, `admission_type`, `admission_location`, `discharge_location`, `insurance`, `language`, `religion`, `marital_status`, and `ethnicity`.
- **`dim_icustays`** *(mapped to FHIR Encounter - Part II)*: Captures unit-level operational details including `icustay_id`, `dbsource` (CareVue/MetaVision), first/last care units (`first_careunit`, `last_careunit`), and detailed timing strings.
- **`dim_items`** *(mapped to FHIR Observation Definition)*: A conformed lookup dictionary for clinical events (`itemid`, `label`, `abbreviation`, `category`).
- **`dim_drug`** *(mapped to FHIR Medication)*: Tracks unique medication profiles, mapping formulary codes to generic drug names.
- **`dim_diagnosis` / `dim_procedure`**: Dimension tables storing ICD-9 clinical codes and descriptive text.
- **`dim_date` / `dim_time`**: Conformed temporal tables supporting sub-hour, daily, quarterly, and multi-year time-series granularities.

### 📊 Fact Tables (Analytical & Operational Layers)
- **`fact_admission`**: Aggregates admission metrics, tracking precise total length of stay (LOS) and mortality flags.
- **`fact_chartevents`** *(Streaming & Batch)*: Houses high-frequency patient vitals (Heart Rate, Blood Pressure, Respiratory Rate, O2 Saturation) with precise execution time metrics.
- **`fact_outputevents`** *(Streaming & Batch)*: Tracks quantitative fluid balances, chest tube drainages, and excretion volumes.
- **`fact_prescriptions`**: Captures patient-level medication events, dosages, and active administration windows.
- **`fact_diag_procedure`**: Maps complex multi-diagnosis and clinical procedure events per ICU encounter.

---

## 🚀 How to Run & Deploy

### 1. Prerequisites & Access
- Obtain credentialed access to the **MIMIC-III** dataset via [PhysioNet](https://physionet.org/content/mimiciii/) after completing your CITI training.
- Set up an AWS Account with full permissions for IAM, S3, Glue, Athena, and EC2.

### 2. Primary Pipeline Deployment (Batch)
1. **S3 Lakehouse Setup:** Create three distinct buckets on Amazon S3 representing your Data Lakehouse layers: `s3://pulsebridge-bronze-raw/`, `s3://pulsebridge-silver-fhir-mapped/`, and `s3://pulsebridge-gold-analytical-ready/`.
2. **Glue Data Catalog:** Define your database schemas within the AWS Glue Data Catalog.
3. **Run ETL Jobs:** Deploy and execute the PySpark scripts in AWS Glue:
   - Run the **FHIR Mapping Job** to transform raw CSVs into standard JSON structures inside the Silver layer.
   - Run the **DWH Modeling Job** to read from Silver, apply SCD Type 1 logic, generate relational surrogate keys, and save the final analytical tables into the Gold layer.

### 3. Secondary Pipeline Deployment (Streaming)
1. **Infrastructure Provisioning:** Launch an AWS EC2 instance running Docker.
2. **Kafka Initialization:** Spin up your Apache Kafka broker inside EC2, and define topics for high-frequency events (`chartevents`, `outputevents`).
3. **Event Ingestion:** Execute the streaming chunking notebook to publish live micro-batches into Kafka.
4. **Spark Streaming ETL:** Start the containerized PySpark application. The script will actively subscribe to Kafka topics, apply deduplication/windowed logic, and stream data directly into the target PostgreSQL operational tables.
5. **Monitoring Layer:** Launch Grafana, connect it to your PostgreSQL database, and import the target dashboard configuration JSONs located under `Streaming Pipeline/Grafana Dashboards/`.

---

