package org.elasticsoftware.elasticactors.cluster.metrics;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tags;
import io.micrometer.core.instrument.util.StringUtils;
import org.springframework.beans.factory.NoSuchBeanDefinitionException;
import org.springframework.core.env.Environment;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public final class MeterConfiguration {

    private final MeterRegistry registry;
    private final String componentName;
    private final String metricPrefix;
    private final Tags tags;

    @Nullable
    public static MeterConfiguration build(
        @Nonnull Environment env,
        @Nullable MeterRegistry meterRegistry,
        @Nonnull String componentName,
        @Nullable MeterTagCustomizer tagCustomizer)
    {
        boolean isMeterEnabled = env.getProperty(
            format("ea.%s.metrics.enable", componentName),
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
            String prefix = env.getProperty(format("ea.%s.metrics.prefix", componentName));
            return new MeterConfiguration(
                meterRegistry,
                componentName,
                prefix,
                tagCustomizer != null ? tagCustomizer.get(componentName) : null
            );
        }
        return null;
    }

    public MeterConfiguration(
        @Nonnull MeterRegistry registry,
        @Nonnull String componentName,
        @Nullable String metricPrefix,
        @Nullable Tags tags)
    {
        this.registry = requireNonNull(registry);
        this.metricPrefix = sanitizePrefix(metricPrefix);
        this.componentName = requireNonNull(componentName);
        this.tags = tags != null ? tags : Tags.empty();
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
    public String getComponentName() {
        return componentName;
    }

    @Nonnull
    public String getMetricPrefix() {
        return metricPrefix;
    }

    @Nonnull
    public Tags getTags() {
        return tags;
    }
}
