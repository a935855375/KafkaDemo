import java.util.Properties

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}

object Producer extends App {
  val topic = "thet"
  val brokers = List("10.0.0.26:9092", "10.0.0.33:9092")

  val props = new Properties
  props.put("bootstrap.servers", brokers.mkString(","))
  props.put("client.id", "producer test")
  props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
  props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")

  val producer = new KafkaProducer[String, String](props)


  for (idx <- 1 to 100000) {
    val msg = s"hello, I am $idx"
    val record = new ProducerRecord[String, String](topic, "key", msg)
    producer.send(record)
    Thread.sleep(1000)
  }

  producer.close()
}
