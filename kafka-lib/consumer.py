from kafka import KafkaConsumer
import json
import datetime as dt
import pyspark as ps
import pandas as pd
from pyspark.sql import SparkSession
import sys
from pyspark.sql.types import StructType, StructField, StringType, DoubleType

from pyspark.sql.functions import from_json
import os 

# consumer = KafkaConsumer(
#     'windpowerproject',
#     group_id = 'WPP_Streamer',
#     bootstrap_servers=[
#     # "ip-172-31-13-101.eu-west-2.compute.internal:9092",
#     # "ip-172-31-3-80.eu-west-2.compute.internal:9092",
#     # "ip-172-31-5-217.eu-west-2.compute.internal:9092",
#     # "ip-172-31-9-237.eu-west-2.compute.internal:9092"
#     # ],
#     # "localhost:9092"],
#     value_deserializer = lambda x: json.loads(x.decode('UTF-8'))
# )

spark = SparkSession.builder.appName("WPP_Kafka_Streamer").master("local[*]").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

TOPIC = "windpowerproject"

# allow for testing local vs non local
BOOSTRAP_SERVER = "localhost:9092" if sys.argv[1] == "local" else [
    "ip-172-31-13-101.eu-west-2.compute.internal:9092",
    "ip-172-31-3-80.eu-west-2.compute.internal:9092",
    "ip-172-31-5-217.eu-west-2.compute.internal:9092",
    "ip-172-31-9-237.eu-west-2.compute.internal:9092"
    ]

print(BOOSTRAP_SERVER)

df = spark.readStream.format("kafka").option("kafka.boostrap.servers", BOOSTRAP_SERVER).option(
    "subscribe", TOPIC).load()

new_df = df.selectExpr("CAST(value AS STRING)","timestamp")

# Schema. -> unnamed needs to be removed
schema = StructType().add("unnamed", StringType()).add("datetime", StringType()).add("wa_c", DoubleType()).add("tempt_c", DoubleType()).add("wind_speed","double")
new_df.printSchema()

# From producer -> if fits schema then append to df. 

while True:
    print(new_df.select(from_json()))

# Predict(df) -> Returns datetime, active power, reactive power -> Save to HBase with HappyBase