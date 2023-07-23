# Import the necessary modules
import json
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    FloatType,
    LongType,
    BooleanType,
)
import yaml

# Config file
with open("/opt/spark-apps/config.yaml", "r") as f:
    config = yaml.load(f, Loader=yaml.FullLoader)

# Create a SparkSession
spark = SparkSession.builder.appName("Api_Velib").getOrCreate()

SNOWFLAKE_SOURCE_NAME = "net.snowflake.spark.snowflake"

# Snowflake connection options
sfOptions = {
    "sfURL": config["account_identifier"] + ".snowflakecomputing.com",
    "sfUser": config["user_name"],
    "sfPassword": config["password"],
    "sfDatabase": config["database"],
    "sfSchema": config["schema"],
    "sfWarehouse": config["warehouse"],
}

station_status_schema = ArrayType(
    StructType(
        [
            StructField("stationCode", StringType(), True),
            StructField("station_id", LongType(), True),
            StructField("is_installed", IntegerType(), True),
            StructField("is_renting", IntegerType(), True),
            StructField("is_returning", IntegerType(), True),
            StructField("last_reported", IntegerType(), True),
            StructField("numBikesAvailable", IntegerType(), True),
            StructField("numDocksAvailable", IntegerType(), True),
            StructField("num_bikes_available", IntegerType(), True),
            StructField("num_bikes_available_types", StringType(), True),
        ]
    )
)

station_information_schema = ArrayType(
    StructType(
        [
            StructField("station_id", LongType(), False),
            StructField("lat", FloatType(), True),
            StructField("lon", FloatType(), True),
            StructField("capacity", IntegerType(), True),
            StructField("name", StringType(), True),
        ]
    )
)

df_status = (
    spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", "kafka:9092")
    .option("startingOffsets", "earliest")
    .option("subscribe", "station_status")
    .load()
)

df_information = (
    spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", "kafka:9092")
    .option("startingOffsets", "earliest")
    .option("subscribe", "station_information")
    .load()
)

df_status = (
    df_status.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
    .withColumn("value", from_json("value", station_status_schema))
    .select("key", explode_outer("value").alias("value"))
    .select("key", col("value.*"))
    .withColumns(
        {
            "is_installed": col("is_installed").cast(BooleanType()),
            "is_renting": col("is_renting").cast(BooleanType()),
            "is_returning": col("is_returning").cast(BooleanType()),
        }
    )
    .withColumn(
        "num_bikes_mechanic",
        regexp_extract(
            col("num_bikes_available_types"), r"\{\"mechanical\":?([,\d]+)\]?\}", 1
        ),
    )
    .withColumn(
        "num_bikes_ebike",
        regexp_extract(
            col("num_bikes_available_types"), r"\{\"ebike\":?([,\d]+)\]?\}", 1
        ),
    )
    .drop(col("num_bikes_available_types"))
    .withColumn("timestamp", from_unixtime("key"))
    .withColumn("last_reported", from_unixtime("last_reported"))
    .drop("key")
)
df_information = (
    df_information.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
    .withColumn("value", from_json("value", station_information_schema))
    .select("key", explode_outer("value").alias("value"))
    .select("key", col("value.*"))
    .withColumn("timestamp", from_unixtime("key"))
    .drop("key")
)


def write_to_snowflake(df, epochId):
    df.write.format(SNOWFLAKE_SOURCE_NAME).options(**sfOptions).option(
        "dbtable", config["dbtable"]
    ).mode("append").save()


df_information.join(
    df_status, ["timestamp", "station_id"], "inner"
).writeStream.trigger(processingTime="5 minutes").foreachBatch(
    write_to_snowflake
).start().awaitTermination()
