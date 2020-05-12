package com.ververica.example.util;

import static org.apache.flink.util.Preconditions.checkArgument;

import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.SplittableRandom;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;

/** A simple random data generator with data rate throttling logic (max parallelism: 1). */
public abstract class SingleBaseGenerator<T> extends RichSourceFunction<T>
        implements CheckpointedFunction {

    private static final long serialVersionUID = 1L;
    protected final int maxRecordsPerSecond;
    private volatile boolean running = true;
    private long id = -1;
    private transient ListState<Long> idState;

    public SingleBaseGenerator(int maxRecordsPerSecond) {
        checkArgument(
                maxRecordsPerSecond == -1 || maxRecordsPerSecond > 0,
                "maxRecordsPerSecond must be positive or -1 (infinite)");
        this.maxRecordsPerSecond = maxRecordsPerSecond;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        if (id == -1) {
            id = getRuntimeContext().getIndexOfThisSubtask();
        }
    }

    @Override
    public final void run(SourceContext<T> ctx) throws Exception {
        LocalDateTime timestamp = LocalDateTime.of(2000, 1, 1, 1, 0);

        final int numberOfParallelSubtasks = getRuntimeContext().getNumberOfParallelSubtasks();
        final Throttler throttler = new Throttler(maxRecordsPerSecond, numberOfParallelSubtasks);
        final SplittableRandom rnd = new SplittableRandom();

        final Object lock = ctx.getCheckpointLock();

        while (running) {
            T event = randomEvent(rnd, id);

            synchronized (lock) {
                ctx.collectWithTimestamp(event, timestamp.atZone(ZoneId.of("UTC")).toInstant().toEpochMilli());
                id += numberOfParallelSubtasks;
            }

            timestamp = timestamp.plusMinutes(5);
            throttler.throttle();
        }
    }

    @Override
    public final void cancel() {
        running = false;
    }

    @Override
    public final void snapshotState(FunctionSnapshotContext context) throws Exception {
        idState.clear();
        idState.add(id);
    }

    @Override
    public void initializeState(FunctionInitializationContext context) throws Exception {
        idState =
                context
                        .getOperatorStateStore()
                        .getUnionListState(new ListStateDescriptor<>("ids", BasicTypeInfo.LONG_TYPE_INFO));

        if (context.isRestored()) {
            long max = Long.MIN_VALUE;
            for (Long value : idState.get()) {
                max = Math.max(max, value);
            }

            id = max + getRuntimeContext().getIndexOfThisSubtask();
        }
    }

    protected abstract T randomEvent(SplittableRandom rnd, long id);
}
