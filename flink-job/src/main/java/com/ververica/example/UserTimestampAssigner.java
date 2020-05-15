package com.ververica.example;

import com.ververica.avro.generated.User;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.watermark.Watermark;

import javax.annotation.Nullable;

/**
 * Assigns each record its event time based on information in the Kafka
 * {@link org.apache.kafka.clients.consumer.ConsumerRecord} and generates
 * watermarks based on 5 second expected latency.
 */
public class UserTimestampAssigner implements AssignerWithPeriodicWatermarks<User> {
    private static final long serialVersionUID = 1L;

    private static final long EXPECTED_LATENCY = 5 * 1000L;

    private long currentMaxTimestamp = Long.MIN_VALUE;
    private long lastEmittedWatermark = Long.MIN_VALUE;

    @Nullable
    @Override
    public Watermark getCurrentWatermark() {
        long potentialWM = this.currentMaxTimestamp - EXPECTED_LATENCY;
        if (potentialWM >= this.lastEmittedWatermark) {
            this.lastEmittedWatermark = potentialWM;
        }

        return new Watermark(this.lastEmittedWatermark);
    }

    @Override
    public long extractTimestamp(User user, long kafkaTimestamp) {
        if (kafkaTimestamp > currentMaxTimestamp) {
            currentMaxTimestamp = kafkaTimestamp;
        }

        return kafkaTimestamp;
    }
}
