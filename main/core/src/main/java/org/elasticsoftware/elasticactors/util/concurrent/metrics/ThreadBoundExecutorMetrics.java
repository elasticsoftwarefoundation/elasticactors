package org.elasticsoftware.elasticactors.util.concurrent.metrics;

import io.micrometer.core.instrument.FunctionCounter;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tag;
import io.micrometer.core.instrument.Tags;
import io.micrometer.core.instrument.binder.BaseUnits;
import io.micrometer.core.instrument.util.StringUtils;
import io.micrometer.core.lang.Nullable;
import org.elasticsoftware.elasticactors.util.concurrent.ThreadBoundExecutor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadPoolExecutor;

public final class ThreadBoundExecutorMetrics {

    private static final Logger logger = LoggerFactory.getLogger(ThreadBoundExecutorMetrics.class);
    private static final String DEFAULT_EXECUTOR_METRIC_PREFIX = "";
    @Nullable
    private final ExecutorService executorService;

    private final Iterable<Tag> tags;
    private final String metricPrefix;



    private ThreadBoundExecutorMetrics(@Nullable ExecutorService executorService, String executorServiceName, Iterable<Tag> tags) {
        this(executorService, executorServiceName, DEFAULT_EXECUTOR_METRIC_PREFIX, tags);
    }

    private ThreadBoundExecutorMetrics(@Nullable ExecutorService executorService, String executorServiceName,
        String metricPrefix, Iterable<Tag> tags) {
        this.executorService = executorService;
        this.tags = Tags.concat(tags, "name", executorServiceName);
        this.metricPrefix = sanitizePrefix(metricPrefix);
    }

    private static String sanitizePrefix(String metricPrefix) {
        if (StringUtils.isBlank(metricPrefix))
            return "";
        if (!metricPrefix.endsWith("."))
            return metricPrefix + ".";
        return metricPrefix;
    }

    private void monitor(MeterRegistry registry, @Nullable ThreadBoundExecutor tp) {
        if (tp == null) {
            return;
        }

        FunctionCounter.builder(metricPrefix + "threadbound.executor.completed", tp, ThreadPoolExecutor::getCompletedTaskCount)
            .tags(tags)
            .description("The approximate total number of tasks that have completed execution")
            .baseUnit(BaseUnits.TASKS)
            .register(registry);

        Gauge.builder(metricPrefix + "threadbound.executor.active", tp, ThreadPoolExecutor::getActiveCount)
            .tags(tags)
            .description("The approximate number of threads that are actively executing tasks")
            .baseUnit(BaseUnits.THREADS)
            .register(registry);

        Gauge.builder(metricPrefix + "threadbound.executor.queued", tp, tpRef -> tpRef.getQueue().size())
            .tags(tags)
            .description("The approximate number of tasks that are queued for execution")
            .baseUnit(BaseUnits.TASKS)
            .register(registry);

        Gauge.builder(metricPrefix + "threadbound.executor.queue.remaining", tp, tpRef -> tpRef.getQueue().remainingCapacity())
            .tags(tags)
            .description("The number of additional elements that this queue can ideally accept without blocking")
            .baseUnit(BaseUnits.TASKS)
            .register(registry);

        Gauge.builder(metricPrefix + "threadbound.executor.pool.size", tp, ThreadPoolExecutor::getPoolSize)
            .tags(tags)
            .description("The current number of threads in the pool")
            .baseUnit(BaseUnits.THREADS)
            .register(registry);

        Gauge.builder(metricPrefix + "threadbound.executor.pool.core", tp, ThreadPoolExecutor::getCorePoolSize)
            .tags(tags)
            .description("The core number of threads for the pool")
            .baseUnit(BaseUnits.THREADS)
            .register(registry);

        Gauge.builder(metricPrefix + "threadbound.executor.pool.max", tp, ThreadPoolExecutor::getMaximumPoolSize)
            .tags(tags)
            .description("The maximum allowed number of threads in the pool")
            .baseUnit(BaseUnits.THREADS)
            .register(registry);
    }

}
