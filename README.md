# US Flight Delays — Data Engineering Project

![Architecture](assets/architecture.svg)

---

## Table of Contents

- [Problem Statement](#problem-statement)
- [Architecture Overview](#architecture-overview)
- [Tech Stack](#tech-stack)
- [Project Structure](#project-structure)
- [Prerequisites](#prerequisites)
- [Quickstart](#quickstart)
  - [1. Clone & Setup](#1-clone--setup)
  - [2. Configure GCP](#2-configure-gcp)
  - [3. Provision Infrastructure (Terraform)](#3-provision-infrastructure-terraform)
  - [4. Start Services (Docker Compose)](#4-start-services-docker-compose)
  - [5. Trigger the Pipeline (Airflow)](#5-trigger-the-pipeline-airflow)
  - [6. Run dbt Models](#6-run-dbt-models)
  - [7. View the Dashboard](#7-view-the-dashboard)
- [Pipeline Details](#pipeline-details)
  - [Data Ingestion](#data-ingestion)
  - [Spark Transformation](#spark-transformation)
  - [BigQuery Schema](#bigquery-schema)
  - [dbt Models](#dbt-models)
  - [Dashboard Tiles](#dashboard-tiles)
- [Evaluation Criteria Coverage](#evaluation-criteria-coverage)
- [Development](#development)
- [Troubleshooting](#troubleshooting)

---

## Problem Statement

The US Bureau of Transportation Statistics (BTS) publishes monthly flight on-time performance data covering every domestic flight operated by major carriers. This dataset captures departure/arrival delays, cancellations, and delay causes — but in its raw form it is not readily queryable for trend analysis or carrier comparison.

**This project builds a fully automated batch pipeline that:**

1. Downloads monthly BTS On-Time Performance CSV files
2. Stores raw data in a GCS data lake
3. Cleans, enriches, and converts data to Parquet using PySpark on Dataproc
4. Loads processed data into BigQuery (partitioned + clustered for efficiency)
5. Transforms the data into analytics-ready mart tables using dbt
6. Visualises insights on a two-tile Streamlit dashboard

The result is a reproducible, cloud-native pipeline that answers questions like:  
*"Which airlines are most delayed? How do delays vary by season?"*

---

