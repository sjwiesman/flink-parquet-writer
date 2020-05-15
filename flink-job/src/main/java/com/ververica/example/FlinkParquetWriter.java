package com.ververica.example;

import com.ververica.avro.generated.User;
import com.ververica.example.util.UserGenerator;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.core.fs.Path;
import org.apache.flink.formats.avro.AvroDeserializationSchema;
import org.apache.flink.formats.avro.AvroRowSerializationSchema;
import org.apache.flink.formats.parquet.avro.ParquetAvroWriters;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.kafka.clients.CommonClientConfigs;

import java.util.Properties;

/**
 * A simple Flink Job that consumes Avro data from Kafka
 * and writes it out as Parquet. The output will be Hive
 * partitioned based on the event time of the record.
 */
public class FlinkParquetWriter {

    private static final String KAFKA = "kafka:9092";

    private static final String TOPIC = "users";

    private static final String OUTPUT = "s3://parquet/";

    public static void main(String[] args) throws Exception {
        ParameterTool tool = ParameterTool.fromArgs(args);

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.getConfig().enableObjectReuse();
        env.enableCheckpointing(10000);

        String output = tool.get("output", OUTPUT);

        env.addSource(createSourceFunction(tool))
                .name("kafka-source")
                .addSink(StreamingFileSink
                        .forBulkFormat(new Path(output), ParquetAvroWriters.forSpecificRecord(User.class))
                        .withBucketAssigner(new UserBucketAssigner())
                        .build())
                .name("parquet-writer");

        env.execute("Flink Parquet Writer");
    }

    public static SourceFunction<User> createSourceFunction(ParameterTool tool) {
        if (tool.has("data-generator")) {
            return new UserGenerator(100);
        }

        Properties properties = new Properties();
        properties.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, tool.get(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, KAFKA));
        properties.put("group.id", "flink-parquet-writer");

        String topic = tool.get("topic", TOPIC);

        DeserializationSchema<User> schema = AvroDeserializationSchema.forSpecific(User.class);
        return new FlinkKafkaConsumer<>(topic, schema, properties)
            .assignTimestampsAndWatermarks(new UserTimestampAssigner());
    }
}
