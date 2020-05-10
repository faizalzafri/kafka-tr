package com.github.faizalzafri.kafkatr.basics;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

public class ConsumerWithThreads {
    private ConsumerWithThreads() {
    }

    public static void main(String[] args) {
        new ConsumerWithThreads().init();
    }

    public void init() {
        Logger logger = LoggerFactory.getLogger(ConsumerWithThreads.class);

        String topic = "first_topic";
        String bootstrapServer = "127.0.0.1:9092";
        String groupId = "my-sixth-app";
        CountDownLatch latch = new CountDownLatch(1);

        //creating consumer thread
        Runnable consumerRunnable = new ConsumerRunnable(bootstrapServer, topic, groupId, latch);

        //add a shutdown hook
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            logger.info("Caught shutdown hook");
            ((ConsumerRunnable) consumerRunnable).shutdown();
            try {
                latch.await();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            logger.info("App exited");
        }));

        //starts a thread
        Thread consumerThread = new Thread(consumerRunnable);
        consumerThread.start();
        try {
            latch.await();
        } catch (InterruptedException e) {
            logger.error("App interrupted {}", e);
        } finally {
            logger.info("App closing");
        }


    }

    public class ConsumerRunnable implements Runnable {

        private CountDownLatch latch;
        private KafkaConsumer<String, String> consumer;

        public ConsumerRunnable(String bootstrapServer, String topic, String groupId, CountDownLatch latch) {

            this.latch = latch;
            consumer = new KafkaConsumer<>(getProperties(bootstrapServer, groupId));
            consumer.subscribe(Collections.singleton(topic));
        }

        private Properties getProperties(String bootstrapServer, String groupId) {
            Properties properties = new Properties();
            properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
            properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
            properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
            properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
            properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
            return properties;
        }

        @Override
        public void run() {
            Logger logger2 = LoggerFactory.getLogger(ConsumerRunnable.class);

            try {
                while (true) {
                    ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));

                    for (ConsumerRecord record : records) {
                        logger2.info("Topic: {} Key: {} Partition: {} Offset: {} Timestamp: {}", record.topic(), record.key(), record.partition(), record.offset(), record.timestamp());
                    }
                }
            } catch (WakeupException e) {
                logger2.info("Shutdown signal received!");
            } finally {
                consumer.close();
                latch.countDown();
            }
        }


        public void shutdown() {
            // interrupts consumer.poll(), throws WakeUpException
            consumer.wakeup();
        }
    }
}
