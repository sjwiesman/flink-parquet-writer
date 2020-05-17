package com.ververica.datagen;

import com.ververica.avro.generated.User;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.Iterator;
import java.util.Properties;
import java.util.Random;
import java.util.function.UnaryOperator;
import java.util.stream.Stream;

public class Producer implements Runnable, AutoCloseable {

    private volatile boolean isRunning;

    private final String brokers;

    private final String topic;

    private final String registry;

    public Producer(String brokers, String topic, String registry) {
        this.brokers = brokers;
        this.topic = topic;
        this.registry = registry;
        this.isRunning = true;
    }

    @Override
    public void run() {
        KafkaProducer<String, User> producer = new KafkaProducer<>(getProperties());

        Throttler throttler = new Throttler(100);

        Random generator = new Random();

        Iterator<String> names = Stream.generate(() -> Stream.of("John", "Paul", "George", "Ringo"))
                .flatMap(UnaryOperator.identity())
                .iterator();

        Iterator<String> colors = Stream.generate(() -> Stream.of("red", "orange", "yellow", "green", "blue", "indigo", "violet"))
                .flatMap(UnaryOperator.identity())
                .iterator();

        Iterator<Long> timestamps = Stream.iterate(LocalDateTime.of(2000, 1, 1, 1, 0), time -> time.plusMinutes(5))
                .map(dateTime -> dateTime.atZone(ZoneOffset.UTC).toInstant().toEpochMilli())
                .iterator();

        while (isRunning) {
            User user = User.newBuilder()
                    .setName(names.next())
                    .setFavoriteColor(colors.next())
                    .setFavoriteNumber(generator.nextInt(1000))
                    .build();

            long timestamp = timestamps.next();

            ProducerRecord<String, User> record = new ProducerRecord<>(topic, null, timestamp, user.getName().toString(), user);
            producer.send(record);

            try {
                throttler.throttle();
            } catch (InterruptedException e) {
                isRunning = false;
            }
        }

        producer.close();
    }

    @Override
    public void close() {
        isRunning = false;
    }

    private Properties getProperties() {
        final Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers);
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.RETRIES_CONFIG, 0);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class);
        props.put(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, registry);

        return props;
    }
}
