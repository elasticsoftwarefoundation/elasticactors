package org.elasticsoftware.elasticactors.util.concurrent.metrics;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tags;
import io.micrometer.core.instrument.Timer;
import org.elasticsoftware.elasticactors.cluster.metrics.MicrometerConfiguration;
import org.elasticsoftware.elasticactors.cluster.metrics.MicrometerTagCustomizer;
import org.springframework.core.env.Environment;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

public final class ThreadBoundExecutorMonitor {

    private final MicrometerConfiguration configuration;
    private final Timer executionTimer;
    private final Timer idleTimer;
    private final Tags tags;

    @Nullable
    public static ThreadBoundExecutorMonitor build(
        @Nonnull Environment env,
        @Nullable MeterRegistry meterRegistry,
        @Nonnull String executorName,
        @Nullable MicrometerTagCustomizer tagCustomizer)
    {
        MicrometerConfiguration configuration =
            MicrometerConfiguration.build(env, meterRegistry, executorName, tagCustomizer);
        if (configuration != null) {
            return new ThreadBoundExecutorMonitor(configuration);
        } else {
            return null;
        }
    }

    public ThreadBoundExecutorMonitor(@Nonnull MicrometerConfiguration configuration) {
        this.configuration = configuration;
        this.tags = Tags.concat(configuration.getTags(), "name", configuration.getComponentName());
        MeterRegistry registry = configuration.getRegistry();
        this.executionTimer = registry.timer(createNameForSuffix("execution"), this.tags);
        this.idleTimer = registry.timer(createNameForSuffix("idle"), this.tags);
    }

    @Nonnull
    public MicrometerConfiguration getConfiguration() {
        return configuration;
    }

    @Nonnull
    public Tags getTags() {
        return tags;
    }

    @Nonnull
    public Timer getExecutionTimer() {
        return executionTimer;
    }

    @Nonnull
    public Timer getIdleTimer() {
        return idleTimer;
    }

    @Nonnull
    public String createNameForSuffix(String metricSuffix) {
        return configuration.getMetricPrefix() + "threadbound.executor." + metricSuffix;
    }

}
