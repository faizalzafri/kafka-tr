package com.github.faizalzafri.kafkatr.basics;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class ConsumerAssignSeek {
    public static void main(String[] args) {

        Logger logger = LoggerFactory.getLogger(ConsumerAssignSeek.class);

        String topic = "first_topic";

        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "seventh-app");
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);
        TopicPartition partition = new TopicPartition(topic, 0);
        long offset = 5L;

        int toRead = 5;
        int alreadyRead = 0;
        boolean canRead = true;
        //assign
        consumer.assign(Collections.singleton(partition));

        //seek
        consumer.seek(partition, offset);

        while (canRead) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));

            for (ConsumerRecord record : records) {
                alreadyRead++;
                logger.info("Topic: {} Key: {} Partition: {} Offset: {} Timestamp: {}", record.topic(), record.key(), record.partition(), record.offset(), record.timestamp());
                if (alreadyRead >= toRead) {
                    canRead = false;
                    break;
                }
            }
        }
        logger.info("Exiting");
    }
}
