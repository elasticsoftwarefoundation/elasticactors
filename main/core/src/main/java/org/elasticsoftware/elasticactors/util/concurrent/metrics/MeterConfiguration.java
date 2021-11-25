package org.elasticsoftware.elasticactors.util.concurrent.metrics;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tag;
import io.micrometer.core.instrument.Tags;
import io.micrometer.core.instrument.Timer;

import javax.annotation.Nonnull;
import java.util.Objects;

public class MeterConfiguration {

    private final MeterRegistry registry;
    private final Timer executionTimer;
    private final Timer idleTimer;

    public MeterConfiguration(
        @Nonnull MeterRegistry registry,
        @Nonnull String executorName,
        @Nonnull String metricPrefix,
        @Nonnull Iterable<Tag> tags)
    {
        this.registry = Objects.requireNonNull(registry);
        Tags finalTags = Tags.concat(tags, "name", executorName);
        this.executionTimer =
            registry.timer(metricPrefix + "threadbound.executor.execution", finalTags);
        this.idleTimer = registry.timer(metricPrefix + "threadbound.executor.idle", finalTags);
    }

    @Nonnull
    public MeterRegistry getRegistry() {
        return registry;
    }

    @Nonnull
    public Timer getExecutionTimer() {
        return executionTimer;
    }

    @Nonnull
    public Timer getIdleTimer() {
        return idleTimer;
    }
}
