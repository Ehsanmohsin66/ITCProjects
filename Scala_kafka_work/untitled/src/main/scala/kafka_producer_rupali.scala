import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}

import java.util.Properties
object kafka_producer extends App{
  val props = new Properties()
  //props.put("bootstrap.servers", "ip-172-31-13-101.eu-west-2.compute.internal:9092, ip-172-31-3-80.eu-west-2.compute.internal:9092, ip-172-31-5-217.eu-west-2.compute.internal:9092, ip-172-31-9-237.eu-west-2.compute.internal:9092")
  props.put("bootstrap.servers", "localhost:9092")
  props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
  props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")


  val producer: KafkaProducer[Nothing, String] = new KafkaProducer[Nothing, String](props)


  val topic = "rupali1"

  println(s"Sending Records in Kafka Topic [$topic]")

  for (i <- 1 to 50) {
    val record: ProducerRecord[Nothing, String] = new ProducerRecord(topic, i.toString)
    println(s"$record")
    producer.send(record)
  }

  producer.close()

}
