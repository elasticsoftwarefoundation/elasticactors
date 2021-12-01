package org.elasticsoftware.elasticactors.util.concurrent;

import io.micrometer.core.instrument.MeterRegistry;
import org.elasticsoftware.elasticactors.cluster.metrics.MicrometerTagCustomizer;
import org.elasticsoftware.elasticactors.util.concurrent.disruptor.DisruptorThreadBoundExecutor;
import org.elasticsoftware.elasticactors.util.concurrent.metrics.ThreadBoundExecutorMonitor;
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
        @Nullable MicrometerTagCustomizer tagCustomizer)
    {
        final int workers = env.getProperty(
            format("ea.%s.workerCount", executorName),
            Integer.class,
            getDefaultNumberOfWorkers()
        );
        final Boolean useDisruptor =
            env.getProperty(format("ea.%s.useDisruptor", executorName), Boolean.class, FALSE);
        if (useDisruptor) {
            return new DisruptorThreadBoundExecutor(
                new DaemonThreadFactory(baseThreadName),
                workers,
                ThreadBoundExecutorMonitor.build(env, meterRegistry, executorName, tagCustomizer)
            );
        } else {
            return new BlockingQueueThreadBoundExecutor(
                new DaemonThreadFactory(baseThreadName),
                workers,
                ThreadBoundExecutorMonitor.build(env, meterRegistry, executorName, tagCustomizer)
            );
        }
    }

    public static BlockingQueueThreadBoundExecutor buildBlockingQueueThreadBoundExecutor(
        @Nonnull Environment env,
        @Nonnull String executorName,
        @Nonnull String baseThreadName,
        @Nullable MeterRegistry meterRegistry,
        @Nullable MicrometerTagCustomizer tagCustomizer)
    {
        final int workers = env.getProperty(
            format("ea.%s.workerCount", executorName),
            Integer.class,
            getDefaultNumberOfWorkers()
        );
        return new BlockingQueueThreadBoundExecutor(
            new DaemonThreadFactory(baseThreadName),
            workers,
            ThreadBoundExecutorMonitor.build(env, meterRegistry, executorName, tagCustomizer)
        );
    }

    public static ThreadBoundExecutor build(
        @Nonnull Environment env,
        @Nonnull ThreadBoundEventProcessor eventProcessor,
        @Nonnull String executorName,
        @Nonnull String baseThreadName,
        @Nullable MeterRegistry meterRegistry,
        @Nullable MicrometerTagCustomizer tagCustomizer)
    {
        final int workers = env.getProperty(
            format("ea.%s.workerCount", executorName),
            Integer.class,
            getDefaultNumberOfWorkers()
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
                ThreadBoundExecutorMonitor.build(env, meterRegistry, executorName, tagCustomizer)
            );
        } else {
            return new BlockingQueueThreadBoundExecutor(
                eventProcessor,
                batchSize,
                new DaemonThreadFactory(baseThreadName),
                workers,
                ThreadBoundExecutorMonitor.build(env, meterRegistry, executorName, tagCustomizer)
            );
        }
    }

    public static BlockingQueueThreadBoundExecutor buildBlockingQueueThreadBoundExecutor(
        @Nonnull Environment env,
        @Nonnull ThreadBoundEventProcessor eventProcessor,
        @Nonnull String executorName,
        @Nonnull String baseThreadName,
        @Nullable MeterRegistry meterRegistry,
        @Nullable MicrometerTagCustomizer tagCustomizer)
    {
        final int workers = env.getProperty(
            format("ea.%s.workerCount", executorName),
            Integer.class,
            getDefaultNumberOfWorkers()
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
            tagCustomizer
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
        @Nullable MicrometerTagCustomizer tagCustomizer)
    {
        return new BlockingQueueThreadBoundExecutor(
            eventProcessor,
            batchSize,
            new DaemonThreadFactory(baseThreadName),
            workers,
            ThreadBoundExecutorMonitor.build(env, meterRegistry, executorName, tagCustomizer)
        );
    }

    private static int getDefaultNumberOfWorkers() {
        return Runtime.getRuntime().availableProcessors() * 3;
    }

    private static int getBatchSize(Environment env, String executorName) {
        Integer batchSize =
            env.getProperty(format("ea.%s.batchSize", executorName), Integer.class);
        if (batchSize != null) {
            return batchSize;
        }
        // Working around the fact some executors used 'maxBatchSize' instead of 'batchSize'
        Integer maxBatchSize =
            env.getProperty(format("ea.%s.maxBatchSize", executorName), Integer.class);
        if (maxBatchSize != null) {
            return maxBatchSize;
        }
        return 20;
    }
}
