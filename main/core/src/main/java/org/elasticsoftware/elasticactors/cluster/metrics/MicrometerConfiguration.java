package org.elasticsoftware.elasticactors.cluster.metrics;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tag;
import io.micrometer.core.instrument.Tags;
import io.micrometer.core.instrument.util.StringUtils;
import org.elasticsoftware.elasticactors.util.EnvironmentUtils;
import org.springframework.beans.factory.NoSuchBeanDefinitionException;
import org.springframework.core.env.Environment;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public final class MicrometerConfiguration {

    private final MeterRegistry registry;
    private final String componentName;
    private final String metricPrefix;
    private final Tags tags;

    @Nullable
    public static MicrometerConfiguration build(
        @Nonnull Environment env,
        @Nullable MeterRegistry meterRegistry,
        @Nonnull String componentName,
        @Nullable MicrometerTagCustomizer tagCustomizer)
    {
        boolean isMeterEnabled = env.getProperty(
            format("ea.metrics.micrometer.%s.enabled", componentName),
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
            String prefix = env.getProperty(format("ea.metrics.micrometer.%s.prefix", componentName));
            String nodeId = env.getRequiredProperty("ea.node.id");
            String clusterName = env.getRequiredProperty("ea.cluster");
            Tags tags = Tags.of(
                "elastic.actors.internal", "true",
                "elastic.actors.node.id", nodeId,
                "elastic.actors.cluster.name", clusterName
            );
            Map<String, String> configurationTagMap =
                EnvironmentUtils.getKeyValuePairsUnderPrefix(
                    env,
                    format("ea.metrics.micrometer.%s.tags", componentName),
                    Function.identity()
                );
            if (!configurationTagMap.isEmpty()) {
                List<Tag> configurationTags = new ArrayList<>(configurationTagMap.size());
                configurationTagMap.forEach((k, v) -> configurationTags.add(Tag.of(k, v)));
                tags = tags.and(configurationTags);
            }
            return new MicrometerConfiguration(
                meterRegistry,
                componentName,
                prefix,
                tagCustomizer != null ? tags.and(tagCustomizer.get(componentName)) : tags
            );
        }
        return null;
    }

    public MicrometerConfiguration(
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
