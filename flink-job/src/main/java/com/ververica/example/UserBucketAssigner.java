package com.ververica.example;

import com.ververica.avro.generated.User;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.streaming.api.functions.sink.filesystem.BucketAssigner;
import org.apache.flink.streaming.api.functions.sink.filesystem.bucketassigners.SimpleVersionedStringSerializer;

import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;

/**
 * Buckets users based on its event time in a date and hour format:
 *
 * <code>
 * date=YYYY-MM-DD/hour=HH
 * </code>
 */
public class UserBucketAssigner implements BucketAssigner<User, String> {

    private static final String BUCKET_FORMAT = "'date='YYYY-MM-dd'/hour='HH";

    private static final String UNKNOWN_BUCKET = "date=__HIVE_DEFAULT_PARTITION__/hour=__HIVE_DEFAULT_PARTITION__";

    private transient DateTimeFormatter formatter;

    @Override
    public String getBucketId(User user, Context context) {
        if (formatter == null) {
            formatter = DateTimeFormatter.ofPattern(BUCKET_FORMAT);
        }

        Long timestamp = context.timestamp();
        if (timestamp == null) {
            return UNKNOWN_BUCKET;
        }

        return Instant.ofEpochMilli(timestamp)
                .atZone(ZoneOffset.UTC)
                .toLocalDateTime()
                .format(formatter);
    }

    @Override
    public SimpleVersionedSerializer<String> getSerializer() {
        return SimpleVersionedStringSerializer.INSTANCE;
    }

    @Override
    public String toString() {
        return "UserBucketAssigner{" +
                "format=" + BUCKET_FORMAT +
                '}';
    }
}
