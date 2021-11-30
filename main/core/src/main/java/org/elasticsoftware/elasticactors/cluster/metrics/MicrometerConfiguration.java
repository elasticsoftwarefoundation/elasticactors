package org.elasticsoftware.elasticactors.cluster.metrics;

import com.google.common.collect.ImmutableSet;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tag;
import io.micrometer.core.instrument.Tags;
import io.micrometer.core.instrument.util.StringUtils;
import org.springframework.beans.factory.NoSuchBeanDefinitionException;
import org.springframework.core.env.Environment;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;

import static org.elasticsoftware.elasticactors.util.EnvironmentUtils.getKeyValuePairsUnderPrefix;

import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public final class MicrometerConfiguration {

    private final boolean measureDeliveryTimes;
    private final boolean tagMessageWrapperTypes;
    private final MeterRegistry registry;
    private final String componentName;
    private final String metricPrefix;
    private final Tags tags;
    private final ImmutableSet<String> allowedActorTypesForTagging;
    private final ImmutableSet<String> allowedMessageTypesForTagging;

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
            boolean measureMessageDeliveryTimes = env.getProperty(
                format("ea.metrics.micrometer.%s.measureDeliveryTimes", componentName),
                Boolean.class,
                false
            );
            boolean tagMessageWrapperTypes = env.getProperty(
                format("ea.metrics.micrometer.%s.tagMessageWrapperTypes", componentName),
                Boolean.class,
                false
            );
            String prefix =
                env.getProperty(format("ea.metrics.micrometer.%s.prefix", componentName));
            String nodeId = env.getRequiredProperty("ea.node.id");
            String clusterName = env.getRequiredProperty("ea.cluster");
            Tags tags = Tags.of(
                "elastic.actors.generated", "true",
                "elastic.actors.node.id", nodeId,
                "elastic.actors.cluster.name", clusterName
            );
            Map<String, String> configurationTagMap = getKeyValuePairsUnderPrefix(
                env,
                format("ea.metrics.micrometer.%s.tags", componentName),
                Function.identity()
            );
            if (!configurationTagMap.isEmpty()) {
                List<Tag> configurationTags = new ArrayList<>(configurationTagMap.size());
                configurationTagMap.forEach((k, v) -> configurationTags.add(Tag.of(k, v)));
                tags = tags.and(configurationTags);
            }
            Map<String, Boolean> allowedActorTypesForTaggingMap = getKeyValuePairsUnderPrefix(
                env,
                format("ea.metrics.micrometer.%s.detailed.actors", componentName),
                Boolean::parseBoolean
            );
            Map<String, Boolean> allowedMessageTypesForTaggingMap = getKeyValuePairsUnderPrefix(
                env,
                format("ea.metrics.micrometer.%s.detailed.messages", componentName),
                Boolean::parseBoolean
            );
            Set<String> allowedActorTypesForTagging = allowedActorTypesForTaggingMap.entrySet()
                .stream()
                .filter(Map.Entry::getValue)
                .map(Map.Entry::getKey)
                .collect(ImmutableSet.toImmutableSet());
            Set<String> allowedMessageTypesForTagging = allowedMessageTypesForTaggingMap.entrySet()
                .stream()
                .filter(Map.Entry::getValue)
                .map(Map.Entry::getKey)
                .collect(ImmutableSet.toImmutableSet());
            // Shortcut to always tag messages.
            // Users should not need to use this, but it can be useful for tricky scenarios.
            if (allowedMessageTypesForTagging.contains("all")) {
                allowedMessageTypesForTagging = ImmutableSet.of("all");
            }
            return new MicrometerConfiguration(
                measureMessageDeliveryTimes,
                tagMessageWrapperTypes,
                meterRegistry,
                componentName,
                prefix,
                tagCustomizer != null ? tags.and(tagCustomizer.get(componentName)) : tags,
                allowedActorTypesForTagging,
                allowedMessageTypesForTagging
            );
        }
        return null;
    }

    public MicrometerConfiguration(
        boolean measureDeliveryTimes,
        boolean tagMessageWrapperTypes,
        @Nonnull MeterRegistry registry,
        @Nonnull String componentName,
        @Nullable String metricPrefix,
        @Nullable Tags tags,
        @Nonnull Set<String> allowedActorTypesForTagging,
        @Nonnull Set<String> allowedMessageTypesForTagging)
    {
        this.measureDeliveryTimes = measureDeliveryTimes;
        this.tagMessageWrapperTypes = tagMessageWrapperTypes;
        this.registry = requireNonNull(registry);
        this.metricPrefix = sanitizePrefix(metricPrefix);
        this.componentName = requireNonNull(componentName);
        this.tags = tags != null ? tags : Tags.empty();
        this.allowedActorTypesForTagging =
            ImmutableSet.copyOf(requireNonNull(allowedActorTypesForTagging));
        this.allowedMessageTypesForTagging =
            ImmutableSet.copyOf(requireNonNull(allowedMessageTypesForTagging));
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

    public boolean isMeasureDeliveryTimes() {
        return measureDeliveryTimes;
    }

    public boolean isTagMessageWrapperTypes() {
        return tagMessageWrapperTypes;
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

    public ImmutableSet<String> getAllowedActorTypesForTagging() {
        return allowedActorTypesForTagging;
    }

    public ImmutableSet<String> getAllowedMessageTypesForTagging() {
        return allowedMessageTypesForTagging;
    }
}
