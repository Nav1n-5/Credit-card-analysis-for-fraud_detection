Credit Card Transaction Analysis for Fraud Detection

This project implements an event-driven data pipeline for processing credit card transaction data and loading it into BigQuery for analytical fraud detection. The pipeline is triggered by the arrival of transaction files and performs scalable batch processing using cloud-native services on Google Cloud Platform.

Tech Stack

PySpark – Distributed data processing and transformation

Google Cloud Storage (GCS) – Storage for customer data, transaction data, and archived files

Apache Airflow (Cloud Composer) – Event-based orchestration for sensing file arrivals, triggering Spark jobs, and archiving processed data

Dataproc Serverless – Serverless execution environment for PySpark workloads

BigQuery – Cloud data warehouse for analytical fraud detection and reporting

PyTest – Unit testing framework for PySpark transformation logic

GitHub – Source code management

GitHub Actions – CI/CD pipeline for automated testing and deployment
