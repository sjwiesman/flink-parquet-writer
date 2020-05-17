package com.ververica.datagen;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DataGenerator {

    private static final Logger LOG = LoggerFactory.getLogger(DataGenerator.class);

    private static final String KAFKA = "kafka:9092";

    private static final String TOPIC = "users";

    private static final String REGISTRY = "http://schema-registry:8085";

    public static void main(String[] args) {
        Producer producer = new Producer(KAFKA, TOPIC, REGISTRY);

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            LOG.info("Shutting down");
            producer.close();
        }));

        producer.run();
    }
}
