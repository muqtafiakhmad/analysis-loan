# ingestion-prototype

This repo contains codes to ingest and profile a credit application sample data. 

## File list
1. process-credit.yml: a conda environment file.
2. inspect.ipynb: a notebook contains preliminary look into the sample of ingested data. Contains simple data check and contains prototype code for ingestion script.
3. ddl.sql: a script which contains table definition in MySQL which stores ingested data.
4. loan-ingestion.py: an Airflow DAG for data ingestion.
5. profile.ipynb: a notebook which contains exploratory data analysis to understand data sample better.
6. slide.pdf