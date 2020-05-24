# Data Engineering Udacity
This repo contains the four projects of the Data Engineering Nanodegree of Udacity.
The projects' code is written in **Python** using the corresponding libraries and sdks for each
tool used: Postgres, Cassandra, Redshift, Spark and Airflow.

## 1 - Data Modelling
#### Data Modelling with **Postgres**
Modelling of user activity data to create a database and ETL pipeline in Postgres for a musing straming app.

#### Data Modelling with Apache **Cassandra**
Modeling of event data to create a non-relational database and ETL pipeline for a music streaming app.

## 2 - Cloud Data Warehouses
Construction of an ETL pipeline that extracts data from S3, stages them in **Redshift** and transforms data into a set of dimensional tables for the analytics team.

## 3 - Cloud Data Lakes
Construction of an ETL pipeline that extracts data from an S3 bucket, processes it with **Spark** and loads it back into S3 as a set of dimensional tables in partitioned parquet files.

## 4 - Data Pipelines with Airflow
Construction of an ETL pipeline in an **Airflow** dag that extracts data from S3, stages it in **Redshift** and transforms it into a set of dimensional tables for the analytics team. The last step of the dag consists on executing a series of **Data Quality** checks into the final tables.
