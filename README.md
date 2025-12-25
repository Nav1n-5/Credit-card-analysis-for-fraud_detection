Credit Card Transaction Analysis for Fraud Detection

This project implements an event-driven data pipeline for processing credit card transaction data and loading it into BigQuery for analytical fraud detection. The pipeline is triggered by the arrival of transaction files and performs scalable batch processing using cloud-native services on Google Cloud Platform.

Tech Stack

PySpark – Distributed data processing and transformation

Google Cloud Storage (GCS) – Storage for customer data, transaction data, and archived files

Apache Airflow (Cloud Composer) – Event-based orchestration for sensing file arrivals, triggering Spark jobs, and archiving processed data

Dataproc Serverless – Serverless execution environment for PySpark workloads

BigQuery – Cloud data warehouse for analytical fraud detection and reporting

PyTest – Unit testing framework for PySpark transformation logic

Unit test cases for PySpark code and test logic using PyTest


make sure to install pytest and the following dependencies in your env before run this in your machine 
pip install pyspark==3.5.1 \
            google-cloud-bigquery \
            google-cloud-storage \
            pandas \
            numpy \
            pytest \
            pytest-mock \
            apache-airflow[gcp]==2.7.1 \
            google-auth google-auth-oauthlib

    
