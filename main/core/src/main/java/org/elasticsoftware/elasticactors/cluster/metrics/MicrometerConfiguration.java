package org.elasticsoftware.elasticactors.cluster.metrics;

import com.google.common.collect.ImmutableMap;
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

import static org.elasticsoftware.elasticactors.util.EnvironmentUtils.getKeyValuePairsUnderPrefix;

import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public final class MicrometerConfiguration {

    private final boolean measureDeliveryTimes;
    private final boolean tagMessageWrapperTypes;
    private final boolean tagTaskTypes;
    private final MeterRegistry registry;
    private final String componentName;
    private final String metricPrefix;
    private final Tags tags;
    private final ImmutableMap<String, String> suffixesForActor;
    private final ImmutableMap<String, String> suffixesForMessages;

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
            boolean tagTaskTypes = env.getProperty(
                format("ea.metrics.micrometer.%s.tagTaskTypes", componentName),
                Boolean.class,
                false
            );
            String componentNamePrefix = env.getProperty("ea.metrics.micrometer.namePrefix", "");
            String prefix =
                env.getProperty(format("ea.metrics.micrometer.%s.prefix", componentName), "");
            String nodeId = env.getRequiredProperty("ea.node.id");
            String clusterName = env.getRequiredProperty("ea.cluster");
            Tags tags = Tags.of(
                "elastic.actors.generated", "true",
                "elastic.actors.component.name", componentName,
                "elastic.actors.node.id", nodeId,
                "elastic.actors.cluster.name", clusterName
            );
            Map<String, String> configurationTagMap = getKeyValuePairsUnderPrefix(
                env,
                format("ea.metrics.micrometer.%s.tags", componentName),
                String::trim
            );
            if (!configurationTagMap.isEmpty()) {
                List<Tag> configurationTags = new ArrayList<>(configurationTagMap.size());
                configurationTagMap.forEach((k, v) -> configurationTags.add(Tag.of(k, v)));
                tags = tags.and(configurationTags);
            }
            Map<String, String> allowedActorTypesForTagging = getKeyValuePairsUnderPrefix(
                env,
                format("ea.metrics.micrometer.%s.detailed.actors", componentName),
                MicrometerConfiguration::sanitizeSuffix
            );
            Map<String, String> allowedMessageTypesForTagging = getKeyValuePairsUnderPrefix(
                env,
                format("ea.metrics.micrometer.%s.detailed.messages", componentName),
                MicrometerConfiguration::sanitizeSuffix
            );
            return new MicrometerConfiguration(
                measureMessageDeliveryTimes,
                tagMessageWrapperTypes,
                tagTaskTypes,
                meterRegistry,
                sanitizePrefix(componentNamePrefix) + "elastic.actors." + componentName,
                prefix,
                tagCustomizer != null ? tags.and(tagCustomizer.get(componentName)) : tags,
                allowedActorTypesForTagging,
                allowedMessageTypesForTagging
            );
        }
        return null;
    }

    private static String sanitizeSuffix(String metricSuffix) {
        if (StringUtils.isBlank(metricSuffix)) {
            return "";
        }
        return metricSuffix;
    }

    public MicrometerConfiguration(
        boolean measureDeliveryTimes,
        boolean tagMessageWrapperTypes,
        boolean tagTaskTypes,
        @Nonnull MeterRegistry registry,
        @Nonnull String componentName,
        @Nullable String metricPrefix,
        @Nullable Tags tags,
        @Nonnull Map<String, String> suffixesForActor,
        @Nonnull Map<String, String> suffixesForMessages)
    {
        this.measureDeliveryTimes = measureDeliveryTimes;
        this.tagMessageWrapperTypes = tagMessageWrapperTypes;
        this.tagTaskTypes = tagTaskTypes;
        this.registry = requireNonNull(registry);
        this.metricPrefix = sanitizePrefix(metricPrefix);
        this.componentName = requireNonNull(componentName);
        this.tags = tags != null ? tags : Tags.empty();
        this.suffixesForActor = ImmutableMap.copyOf(requireNonNull(suffixesForActor));
        this.suffixesForMessages = ImmutableMap.copyOf(requireNonNull(suffixesForMessages));
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

    public boolean isTagTaskTypes() {
        return tagTaskTypes;
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

    public ImmutableMap<String, String> getSuffixesForActor() {
        return suffixesForActor;
    }

    public ImmutableMap<String, String> getSuffixesForMessages() {
        return suffixesForMessages;
    }
}
