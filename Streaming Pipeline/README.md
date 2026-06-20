# PulseBridge: Real-Time Healthcare Data Streaming Pipeline

## Overview

PulseBridge is a healthcare data engineering project that demonstrates how historical healthcare records can be transformed into a real-time streaming pipeline.

Since the original dataset is stored as batch files, AWS Glue is used to process and split the data into smaller chunks. The transformed chunks are stored back in Amazon S3 and then continuously published to Apache Kafka, simulating a real-time stream of healthcare events.

For the streaming pipeline, two clinically significant tables from the MIMIC dataset were selected: Chartevents and Outputevents. These tables contain high-frequency patient monitoring data, including vital signs, measurements, and fluid output records, making them particularly suitable for real-time healthcare monitoring and analytics.

The streaming data is processed using Apache Spark Structured Streaming, stored in PostgreSQL, and visualized through Grafana dashboards for real-time monitoring and analytics.

---

## Architecture
<img width="1032" height="382" alt="image" src="https://github.com/user-attachments/assets/2ce981d2-ebc5-4e9a-a9cc-cbcbba63c600" />


---

## Project Objectives

* Simulate real-time healthcare data streaming using historical datasets.
* Build an end-to-end streaming data pipeline.
* Process healthcare events in near real time.
* Store processed data for analytical workloads.
* Create monitoring dashboards for healthcare metrics.
* Demonstrate modern data engineering practices using AWS and open-source technologies.

---

## Technologies Used

* AWS S3
* AWS Glue
* AWS EC2
* Apache Kafka
* Apache Spark Structured Streaming
* Docker
* PostgreSQL
* Python
* Grafana

---

## Pipeline Components

### 1. AWS Glue ETL

AWS Glue performs preprocessing on the raw healthcare dataset.

Responsibilities:

* Splitting large datasets into smaller chunks
* Writing processed chunks back to Amazon S3

This stage enables the simulation of streaming behavior from static healthcare records.

---

### 2. Amazon S3 (Processed Chunks)

The processed chunk files are stored in a separate S3 location.

Responsibilities:

* Store transformed data
* Serve as the source for streaming simulation
* Preserve processed datasets for reproducibility

---

### 3. Kafka Producer

The producer reads chunk files incrementally and publishes records to Kafka topics.

Responsibilities:

* Simulate real-time event generation
* Control event publishing rate

---

### 4. Apache Kafka

Kafka acts as the streaming backbone of the architecture.

Topics:

* chartevents-stream
* outputevents-stream

Responsibilities:

* Event buffering
* Reliable message delivery
* Decoupling producers and consumers
* High-throughput event streaming

---

### 5. Spark Structured Streaming

Spark consumes events from Kafka and performs real-time processing.

Responsibilities:

* Stream ingestion
* Data transformation
* Data validation
* Missing value handling
* Event enrichment
* Aggregation and analytics

---

### 6. PostgreSQL

Processed streaming data is persisted into PostgreSQL.

Responsibilities:

* Structured storage
* Analytical querying
* Dashboard data source

---

### 7. Grafana Dashboard

Grafana provides real-time monitoring and visualization.

Example Metrics:

* Number of healthcare events over time
* Average patient measurements
* Event frequency trends
* Output event monitoring
* Streaming throughput


