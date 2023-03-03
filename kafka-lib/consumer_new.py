#from kafka import *
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

spark = SparkSession.builder.config("spark.jars","C:\\Users\\ehsan\\OneDrive\\Desktop\\Wind_Project\\spark-sql-kafka-0-10_2.12-2.4.7.jar,C:\\Users\\ehsan\\OneDrive\\Desktop\\Wind_Project\\jar_files_2\\kafka-clients-0.8.2.1.jar").\
appName("WPP_Kafka_Streamer").master("local[*]").getOrCreate()
#spark.sparkContext.setLogLevel("ERROR")

TOPIC = "ruipali"#"windpowerproject"

# allow for testing local vs non local
BOOSTRAP_SERVER = "ip-172-31-13-101.eu-west-2.compute.internal:9092"
#"localhost:9092"


print(BOOSTRAP_SERVER)

df = spark.read.format("kafka").option("kafka.boostrap.servers", BOOSTRAP_SERVER).\
option("subscribe", TOPIC).option("startingOffsets", "earliest") \
.option("endingOffsets", "latest").load()

new_df = df.selectExpr("CAST(value AS STRING)","timestamp")

# Schema. -> unnamed needs to be removed
schema = StructType().add("unnamed", StringType()).add("datetime", StringType()).add("wa_c", DoubleType()).add("tempt_c", DoubleType()).add("wind_speed","double")
new_df.printSchema()

# From producer -> if fits schema then append to df. 

while True:
    print(new_df.select(from_json()))

# Predict(df) -> Returns datetime, active power, reactive power -> Save to HBase with HappyBase
