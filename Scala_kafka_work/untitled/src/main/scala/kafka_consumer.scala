import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._

object kafka_consumer extends App{
  val spark = SparkSession.builder.appName("WPP_Kafka_consumer").master("local[*]").getOrCreate()
  spark.sparkContext.setLogLevel("ERROR")

  val TOPIC = "ruipali1"//"windpowerproject"

  //allow for testing local vs non local
  val BOOSTRAP_SERVER = "ip-172-31-13-101.eu-west-2.compute.internal:9092";  //"localhost:9092"


  println(BOOSTRAP_SERVER)

  val df = spark.read.format("kafka").option("kafka.bootstrap.servers", BOOSTRAP_SERVER).
  option("subscribe", "wpp_kafka_testdata").option("startingOffsets", "earliest")
  .option("endingOffsets", "latest").load()

  val new_df = df.selectExpr("CAST(value AS STRING)")


  df.printSchema()

  println(new_df.count())
 // }


}
