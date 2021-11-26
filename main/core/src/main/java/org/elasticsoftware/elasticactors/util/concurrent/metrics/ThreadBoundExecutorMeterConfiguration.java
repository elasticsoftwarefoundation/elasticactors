package org.elasticsoftware.elasticactors.util.concurrent.metrics;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tag;
import io.micrometer.core.instrument.Tags;
import io.micrometer.core.instrument.Timer;
import io.micrometer.core.instrument.util.StringUtils;
import org.springframework.beans.factory.NoSuchBeanDefinitionException;
import org.springframework.core.env.Environment;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class ThreadBoundExecutorMeterConfiguration {

    private final MeterRegistry registry;
    private final String metricPrefix;
    private final Tags tags;
    private final Timer executionTimer;
    private final Timer idleTimer;

    @Nullable
    public static ThreadBoundExecutorMeterConfiguration build(
        @Nonnull Environment env,
        @Nullable MeterRegistry meterRegistry,
        @Nonnull String executorName,
        @Nullable Tags customTags)
    {
        boolean isMeterEnabled = env.getProperty(
            format("ea.%s.metrics.enable", executorName),
            Boolean.class,
            false
        );
        if (isMeterEnabled) {
            if (meterRegistry == null) {
                throw new NoSuchBeanDefinitionException(
                    MeterRegistry.class,
                    "expected a bean with name 'elasticActorsMeterRegistry'"
                );
            }
            String prefix = env.getProperty(format("ea.%s.metrics.prefix", executorName));
            return new ThreadBoundExecutorMeterConfiguration(
                meterRegistry,
                executorName,
                prefix,
                customTags
            );
        }
        return null;
    }

    public ThreadBoundExecutorMeterConfiguration(
        @Nonnull MeterRegistry registry,
        @Nonnull String executorName,
        @Nullable String metricPrefix,
        @Nullable Iterable<Tag> tags)
    {
        this.registry = requireNonNull(registry);
        this.metricPrefix = sanitizePrefix(metricPrefix);
        this.tags = Tags.concat(tags, "name", executorName);
        this.executionTimer =
            registry.timer(createNameForSuffix("execution"), this.tags);
        this.idleTimer = registry.timer(createNameForSuffix("idle"), this.tags);
    }

    private static String sanitizePrefix(String metricPrefix) {
        if (StringUtils.isBlank(metricPrefix)) {
            return "";
        }
        if (!metricPrefix.endsWith(".")) {
            return metricPrefix + ".";
        }
        return metricPrefix;
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

    @Nonnull
    public String createNameForSuffix(String metricSuffix) {
        return metricPrefix + "threadbound.executor." + metricSuffix;
    }

    @Nonnull
    public Tags getTags() {
        return tags;
    }
}
