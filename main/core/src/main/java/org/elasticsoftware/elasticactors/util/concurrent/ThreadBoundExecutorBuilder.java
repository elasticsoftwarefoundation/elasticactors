package org.elasticsoftware.elasticactors.util.concurrent;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tags;
import org.elasticsoftware.elasticactors.util.concurrent.disruptor.DisruptorThreadBoundExecutor;
import org.elasticsoftware.elasticactors.util.concurrent.metrics.ThreadBoundExecutorMeterConfiguration;
import org.springframework.core.env.Environment;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import static java.lang.Boolean.FALSE;
import static java.lang.String.format;

public final class ThreadBoundExecutorBuilder {

    private ThreadBoundExecutorBuilder() {
    }

    public static ThreadBoundExecutor build(
        @Nonnull Environment env,
        @Nonnull String executorName,
        @Nonnull String baseThreadName,
        @Nullable MeterRegistry meterRegistry,
        @Nullable Tags customTags)
    {
        final int workers = env.getProperty(
            format("ea.%s.workerCount", executorName),
            Integer.class,
            Runtime.getRuntime().availableProcessors() * 3
        );
        final Boolean useDisruptor =
            env.getProperty(format("ea.%s.useDisruptor", executorName), Boolean.class, FALSE);
        if (useDisruptor) {
            return new DisruptorThreadBoundExecutor(
                new DaemonThreadFactory(baseThreadName),
                workers,
                ThreadBoundExecutorMeterConfiguration.build(
                    env,
                    meterRegistry,
                    executorName,
                    customTags
                )
            );
        } else {
            return new BlockingQueueThreadBoundExecutor(
                new DaemonThreadFactory(baseThreadName),
                workers,
                ThreadBoundExecutorMeterConfiguration.build(
                    env,
                    meterRegistry,
                    executorName,
                    customTags
                )
            );
        }
    }

    public static BlockingQueueThreadBoundExecutor buildBlockingQueueThreadBoundExecutor(
        @Nonnull Environment env,
        @Nonnull String executorName,
        @Nonnull String baseThreadName,
        @Nullable MeterRegistry meterRegistry,
        @Nullable Tags customTags)
    {
        final int workers = env.getProperty(
            format("ea.%s.workerCount", executorName),
            Integer.class,
            Runtime.getRuntime().availableProcessors() * 3
        );
        return new BlockingQueueThreadBoundExecutor(
            new DaemonThreadFactory(baseThreadName),
            workers,
            ThreadBoundExecutorMeterConfiguration.build(
                env,
                meterRegistry,
                executorName,
                customTags
            )
        );
    }

    public static ThreadBoundExecutor build(
        @Nonnull Environment env,
        @Nonnull ThreadBoundEventProcessor eventProcessor,
        @Nonnull String executorName,
        @Nonnull String baseThreadName,
        @Nullable MeterRegistry meterRegistry,
        @Nullable Tags customTags)
    {
        final int workers = env.getProperty(
            format("ea.%s.workerCount", executorName),
            Integer.class,
            Runtime.getRuntime().availableProcessors() * 3
        );
        final int batchSize = getBatchSize(env, executorName);
        final Boolean useDisruptor =
            env.getProperty(format("ea.%s.useDisruptor", executorName), Boolean.class, FALSE);
        if (useDisruptor) {
            return new DisruptorThreadBoundExecutor(
                eventProcessor,
                batchSize,
                new DaemonThreadFactory(baseThreadName),
                workers,
                ThreadBoundExecutorMeterConfiguration.build(
                    env,
                    meterRegistry,
                    executorName,
                    customTags
                )
            );
        } else {
            return new BlockingQueueThreadBoundExecutor(
                eventProcessor,
                batchSize,
                new DaemonThreadFactory(baseThreadName),
                workers,
                ThreadBoundExecutorMeterConfiguration.build(
                    env,
                    meterRegistry,
                    executorName,
                    customTags
                )
            );
        }
    }

    public static BlockingQueueThreadBoundExecutor buildBlockingQueueThreadBoundExecutor(
        @Nonnull Environment env,
        @Nonnull ThreadBoundEventProcessor eventProcessor,
        @Nonnull String executorName,
        @Nonnull String baseThreadName,
        @Nullable MeterRegistry meterRegistry,
        @Nullable Tags customTags)
    {
        final int workers = env.getProperty(
            format("ea.%s.workerCount", executorName),
            Integer.class,
            Runtime.getRuntime().availableProcessors() * 3
        );
        final int batchSize = getBatchSize(env, executorName);
        return buildBlockingQueueThreadBoundExecutor(
            env,
            eventProcessor,
            workers,
            batchSize,
            executorName,
            baseThreadName,
            meterRegistry,
            customTags
        );
    }

    public static BlockingQueueThreadBoundExecutor buildBlockingQueueThreadBoundExecutor(
        @Nonnull Environment env,
        @Nonnull ThreadBoundEventProcessor eventProcessor,
        int workers,
        int batchSize,
        @Nonnull String executorName,
        @Nonnull String baseThreadName,
        @Nullable MeterRegistry meterRegistry,
        @Nullable Tags customTags)
    {
        return new BlockingQueueThreadBoundExecutor(
            eventProcessor,
            batchSize,
            new DaemonThreadFactory(baseThreadName),
            workers,
            ThreadBoundExecutorMeterConfiguration.build(
                env,
                meterRegistry,
                executorName,
                customTags
            )
        );
    }

    private static int getBatchSize(Environment env, String executorName) {
        // Working around the fact some executors used 'maxBatchSize' instead of 'batchSize'
        Integer batchSize =
            env.getProperty(format("ea.%s.batchSize", executorName), Integer.class);
        Integer maxBatchSize =
            env.getProperty(format("ea.%s.maxBatchSize", executorName), Integer.class);
        if (batchSize != null) {
            return batchSize;
        } else if (maxBatchSize != null) {
            return maxBatchSize;
        } else {
            return 20;
        }
    }
}
