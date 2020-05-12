package basics;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Objects;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class ProducerDemoWithKeys {
    public static void main(String[] args) throws ExecutionException, InterruptedException {

        Logger logger = LoggerFactory.getLogger(ProducerDemoWithKeys.class);

        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);

        String topic = "first_topic";

        for (int i = 1; i < 10; i++) {
            String value = "Hello Kafka from Java" + String.valueOf(i);
            String key = "id_" + String.valueOf(i);

            ProducerRecord<String, String> record = new ProducerRecord<>(topic, key, value);

            producer.send(record, (recordMetadata, e) -> {
                if (Objects.nonNull(e)) {
                    logger.error("Error while producing: {}", e);
                } else {
                    logger.info("Topic: {} Key: {} Partition: {} Offset: {} Timestamp: {}", recordMetadata.topic(), key, recordMetadata.partition(), recordMetadata.offset(), recordMetadata.timestamp());
                }
            }).get();
        }
        producer.flush();
    }
}
