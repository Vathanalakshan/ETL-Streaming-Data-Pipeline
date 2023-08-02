# Streaming-ETL-Pipeline

## Overview

In this personal project, I've been exploring the fantastic world of data streaming and orchestration. The goal of this project is to experiment and learn Apache Spark Streaming, Apache Kafka, Apache Airflow, and Docker to create a real-time data processing pipeline.

Velib is a bike-sharing system that allows users to rent bicycles from various stations located throughout the city of Paris.The API provides access to data and functionalities related to the Velib bike-sharing service, enabling them to integrate Velib services into their own applications or websites.

## Project Pipeline
![Alt text](image.png)

## Technologies Used
- **Apache Spark Streaming:** For processing real-time data streams with lightning speed.

- **Apache Kafka:** To build a robust and flexible messaging system for handling data flow.

- **Apache Airflow:** To schedule and manage the data workflows.

- **Docker:** For containerizing different components and avoid the hassle of install/configuration !

## Setup
- Install docker and docker-compose
- Create and setup your Snowflake account 
- Create a config.yaml file based on the config-template file in Spark/spark-apps to configure the snowflake connection

## Usage
- Create and start containers : 
```
docker compose up -d
```
- Wait till Spark and Airflow is running (http://localhost:8080/ and http://localhost:8081/ are running and accessible).
- Connect to the Airflow Webserver( username : user and password : bitnami).
- Turn on the VelibServiceCall Dag.
- Install Spark requirements and Launch the spark Job :
```
docker exec -it spark-master pip install -r /bitnami/python/requirements.txt
```                                    
```
docker exec -it spark-master spark-submit --master spark://spark:7077 --deploy-mode client  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.1,net.snowflake:snowflake-jdbc:3.13.22,net.snowflake:spark-snowflake_2.12:2.11.0-spark_3.3 /opt/spark-apps/transform.py
```

## Improvements to do in the future :
- Plug BI tool on snowflake
- Optimize Spark streaming join performance check && Explore Watermarks 