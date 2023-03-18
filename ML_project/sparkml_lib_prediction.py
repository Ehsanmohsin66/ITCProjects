#import happybase
from pyspark.sql import SparkSession
import logging
from pyspark.ml.classification import RandomForestClassifier
from pyspark.ml.classification import MultilayerPerceptronClassifier
from pyspark.ml.regression import RandomForestRegressionModel,RandomForestRegressor
from pyspark.ml import Pipeline
from pyspark.ml.linalg import Vectors
from pyspark.sql import Row
from pyspark.ml.linalg import Vectors
import json
from pyspark.sql.functions import date_format
from pyspark.sql.functions import col
from pyspark.sql.types import IntegerType, DoubleType, StructType, StructField, StringType
import pprint

def transData(data):
    return data.rdd.map(lambda r: [r[4], Vectors.dense(r[0:4])]). \
        toDF(['label', 'features'])


class VarcharType:
    pass


if __name__ == '__main__':
    s_logger = logging.getLogger('py4j.java_gateway')
    s_logger.setLevel(logging.ERROR)
    spark = SparkSession.builder.appName(
        "Spark_ML_train_model").getOrCreate()  # ("local[*]", "FirstDemo")
    spark.sparkContext.setLogLevel("ERROR")

    TOPIC = "windpowerproject"


    #for testing local vs non local
    BOOSTRAP_SERVER = "ip-172-31-13-101.eu-west-2.compute.internal:9092"
    df = spark.read.format("kafka").option("kafka.bootstrap.servers", BOOSTRAP_SERVER).\
    option("subscribe", TOPIC).option("startingOffsets", "earliest")\
    .option("endingOffsets", "latest").load()

    new_df = df.selectExpr("CAST(value AS STRING)")
    i=0
    cont=df.count()
    model_loaded = RandomForestRegressionModel().load("ml_model")
    #connection = happybase.Connection('ip-172-31-3-80.eu-west-2.compute.internal', table_prefix='ml_windpowerpred')
    #schema1 = StructType([StructField('feature1', StringType(), True),StructField('feature2', DoubleType(), True),StructField('feature3', DoubleType(), True),StructField('feature4', DoubleType(), True)])

    for i in range(cont-20,cont):
        str1=new_df.collect()[i][0]
        json_data=json.loads(str1)
        pred_data_inp_list = [(json_data["datetime"], json_data["tempt_c"],json_data["wa_c"], json_data["tempt_c"])]
        pred_data_inp_df = spark.createDataFrame(data=pred_data_inp_list,schema=["feature1","feature2","feature3","feature4"])
        pred_data_inp_1=pred_data_inp_df.withColumn("feature1", date_format(col("feature1"), "D"))\
        .withColumn("feature1",col("feature1").cast(IntegerType()))
        testData=pred_data_inp_1.rdd.map(lambda r:[0,Vectors.dense(r[0:4])]).toDF(['label', 'features'])
        pred=model_loaded.transform(testData)
        pred.show()
        date_time=json_data["datetime"]
        #df_forhbase=spark.createDataFrame([[date_time, pred_power]], ["datetime", "active_power"])
        #df_forhbase.show()






#connection = happybase.Connection('hostname')
#table = connection.table('ml_windpowerpred')

#table.put(b'row-key', {b'family:datetime': b'datetime',
    #                   b'family:power': b'power'})

#row = table.row(b'row-key')
#print(row[b'family:qual1'])  # prints 'value1'

#for key, data in table.rows([b'row-key-1', b'row-key-2']):
#    print(key, data)  # prints row key and data for each row

#for key, data in table.scan(row_prefix=b'row'):
 #   print(key, data)  # prints 'value1' and 'value2'

#row = table.delete(b'row-key')