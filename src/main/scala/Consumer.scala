import java.util.{Collections, Properties}
import scala.collection.JavaConverters._
import org.apache.kafka.clients.consumer.KafkaConsumer

object Consumer extends App {
  val topic = "thet"
  val props = new Properties

  val brokers = List("10.0.0.26:9092", "10.0.0.33:9092").mkString(",")
  props.put("bootstrap.servers", brokers)
  props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
  props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
  props.put("group.id", "something")

  val consumer = new KafkaConsumer[String, String](props)

  consumer.subscribe(Collections.singletonList(topic))

  while (true) {
    val records = consumer.poll(100)
    for (record <- records.asScala) {
      println(record.value())
    }
  }
}