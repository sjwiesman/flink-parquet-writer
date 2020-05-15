package com.ververica.datagen;

import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.kafka.clients.producer.KafkaProducer;
import com.ververica.avro.generated.User;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.Iterator;
import java.util.Map;
import java.util.Properties;
import java.util.Random;
import java.util.function.UnaryOperator;
import java.util.stream.Stream;

public class DataGenerator {

    private static final Logger LOG = LoggerFactory.getLogger(DataGenerator.class);

    private static final String KAFKA = "kafka:9092";

    private static final String TOPIC = "users";

    private static volatile boolean isRunning = true;

    public static void main(String[] args) {
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            LOG.info("Shutting down");
            isRunning = false;
        }));

        LOG.info("Connecting to Kafka brokers");
        KafkaProducer<String, User> producer = new KafkaProducer<>(getProperties());

        Throttler throttler = new Throttler(100);
        LocalDateTime dateTime = LocalDateTime.of(2000, 1, 1, 1, 0);

        Random generator = new Random();
        Iterator<String> names = Stream.generate(() -> Stream.of("John", "Paul", "George", "Ringo"))
                .flatMap(UnaryOperator.identity())
                .iterator();

        Iterator<String> colors = Stream.generate(() -> Stream.of("red", "orange", "yellow", "green", "blue", "indigo", "violet"))
                .flatMap(UnaryOperator.identity())
                .iterator();

        while (isRunning) {
            User user = User.newBuilder()
                    .setName(names.next())
                    .setFavoriteColor(colors.next())
                    .setFavoriteNumber(generator.nextInt(1000))
                    .build();

            long timestamp = dateTime.atZone(ZoneId.of("UTC")).toInstant().toEpochMilli();
            dateTime = dateTime.plusMinutes(5);

            ProducerRecord<String, User> record = new ProducerRecord<>(TOPIC, null, timestamp, user.getName().toString(), user);
            producer.send(record);

            try {
                throttler.throttle();
            } catch (InterruptedException e) {
                isRunning = false;
            }
        }

        LOG.info("Closing Kafka producer");
        producer.close();
    }

    private static Properties getProperties() {
        final Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA);
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.RETRIES_CONFIG, 0);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, UserSerializer.class);

        return props;
    }

    public static class UserSerializer implements Serializer<User> {

        private transient GenericDatumWriter<User> datumWriter;

        private transient BinaryEncoder encoder;

        private transient ByteArrayOutputStream arrayOutputStream;

        public UserSerializer() {}

        @Override
        public void configure(Map<String, ?> configs, boolean isKey) {
            datumWriter = new SpecificDatumWriter<>(User.getClassSchema());
            arrayOutputStream = new ByteArrayOutputStream();
            encoder = EncoderFactory.get().directBinaryEncoder(arrayOutputStream, null);
        }

        @Override
        public byte[] serialize(String topic, User data) {
            try {
                datumWriter.write(data, encoder);
                encoder.flush();
                byte[] bytes = arrayOutputStream.toByteArray();
                arrayOutputStream.reset();
                return bytes;
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }

        @Override
        public void close() {

        }
    }
}
