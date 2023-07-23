#!/bin/bash
pip install -r /bitnami/python/requirements.txt
docker exec -it spark-master spark-submit --master spark://spark:7077 --deploy-mode client  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.1,net.snowflake:snowflake-jdbc:3.13.22,net.snowflake:spark-snowflake_2.12:2.11.0-spark_3.3 /opt/spark-apps/transform.py
