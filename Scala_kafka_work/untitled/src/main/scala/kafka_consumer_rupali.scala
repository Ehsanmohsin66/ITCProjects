
import java.util.{Collections, Properties}
import scala.collection.JavaConverters._
import org.apache.kafka.clients.consumer.{ConsumerRecords, KafkaConsumer}
object kafka_consumer_rupali extends App{
  val props = new Properties()
  props.put("bootstrap.servers", "0.0.0.0:9092")
  props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
  props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
  props.put("group.id", "consumer-group-1")

  val topic = "rupali1"
  val consumer: KafkaConsumer[Nothing, String] = new KafkaConsumer[Nothing, String](props)
  consumer.subscribe(Collections.singletonList(topic))
  var cont=1
  while (cont > 0) {
    val records: ConsumerRecords[Nothing, String] = consumer.poll(100)
    cont=records.count()
    for (record <- records.asScala) {
      println(record)
    }
  }

}
