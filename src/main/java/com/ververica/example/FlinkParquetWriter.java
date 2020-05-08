package com.ververica.example;

import com.ververica.avro.generated.User;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.core.fs.Path;
import org.apache.flink.formats.avro.AvroDeserializationSchema;
import org.apache.flink.formats.parquet.avro.ParquetAvroWriters;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.kafka.clients.CommonClientConfigs;

import java.util.Properties;

public class FlinkParquetWriter {

	public static void main(String[] args) throws Exception {
	    ParameterTool parameters = ParameterTool.fromArgs(args);

        Properties properties = new Properties();
        properties.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, parameters.get(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG));
        properties.put("group.id", "flink-parquet-writer");

        String topic = parameters.get("topic");
        Path output  = new Path(parameters.get("output"));

		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.getConfig().enableObjectReuse();

        DeserializationSchema<User> schema = AvroDeserializationSchema.forSpecific(User.class);
        env
                .addSource(new FlinkKafkaConsumer<>(topic, schema, properties))
                .name("kafka-source")
                .addSink(StreamingFileSink
                        .forBulkFormat(output, ParquetAvroWriters.forSpecificRecord(User.class))
                        .build())
                .name("parquet-writer");

		env.execute("Flink Parquet Writer");
	}
}
