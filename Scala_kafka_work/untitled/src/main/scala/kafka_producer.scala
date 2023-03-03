import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types._

object kafka_producer extends App{
  val spark = SparkSession.builder.appName("WPP_Kafka_producer_testdata").master("local[*]").getOrCreate()
  spark.sparkContext.setLogLevel("ERROR")

  val TOPIC = "ruipali1"//"windpowerproject"

  //allow for testing local vs non local
  val BOOSTRAP_SERVER = "ip-172-31-13-101.eu-west-2.compute.internal:9092";  //"localhost:9092"


  println(BOOSTRAP_SERVER)

  /*val df = spark.read.format("kafka").option("kafka.bootstrap.servers", BOOSTRAP_SERVER).
    option("subscribe", TOPIC).option("startingOffsets", "earliest")
    .option("endingOffsets", "latest").load()

  val new_df = df.selectExpr("CAST(value AS STRING)","value")*/
    val simpleData = Seq(Row("2022-11-09 12-00", "Germany", "munich", 89.0, 91.9, 91.0,89.3,100.1,110.3),
    Row("2022-11-10 12-00", "france", "paris", 89.0, 91.9, 91.0,89.3,100.1,110.3),
    Row("2022-11-10 12-00", "UK", "london", 89.0, 91.9, 91.0,89.3,100.1,110.3),
    Row("2022-11-10 12-00", "UK", "manchester", 89.0, 91.9, 91.0,89.3,100.1,110.3),
    Row("2022-11-10 12-00", "Italy", "milan", 89.0, 91.9, 91.0,89.3,100.1,110.3)
  )
  //datetime(string:YYYY-MM-DD HH-MM),country (string), city (or site)(String),
  // wind_kph (float),wind_degree (Float),wind_dir (Float),gust_kph (float),wind_ms (Float),gust_ms (Float)
  val simpleSchema = StructType(Array(
    StructField("datetime", StringType, true),
    StructField("country", StringType, true),
    StructField("city", StringType, true),
    StructField("wind_kph", DoubleType, true),
    StructField("wind_degree", DoubleType, true),
    StructField("wind_dir", DoubleType, true),
    StructField("gust_kph", DoubleType, true),
    StructField("wind_ms", DoubleType, true),
    StructField("gust_ms", DoubleType, true)
  ))
  val df_test = spark.createDataFrame(spark.sparkContext.parallelize(simpleData), simpleSchema)
  //kafka-console-producer --bootstrap-server ip-172-31-13-101.eu-west-2.compute.internal:9092, ip-172-31-3-80.eu-west-2.compute.internal:9092, ip-172-31-5-217.eu-west-2.compute.internal:9092, ip-172-31-9-237.eu-west-2.compute.internal:9092 --topic wpp_kafka_testdata
  df_test.toJSON.write.format("kafka").option("kafka.bootstrap.servers", BOOSTRAP_SERVER).option("topic","wpp_kafka_testdata").save()


}
