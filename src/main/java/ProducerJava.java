import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;

public class ProducerJava {
    public static void main(String[] args) throws InterruptedException {
        String topic = "thet";
        String brokers = "10.0.0.26:9092,10.0.0.33:9092";

        Properties props = new Properties();

        props.put("bootstrap.servers", brokers);
        props.put("client.id", "producer test");
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        KafkaProducer<String, String> producer = new KafkaProducer<>(props);

        for (int i = 0; i < 100; i++) {
            String msg = "hello, I am " + i;
            ProducerRecord<String, String> record = new ProducerRecord<>(topic, "key", msg);
            producer.send(record);
            Thread.sleep(1000);
        }

        producer.close();
    }
}
