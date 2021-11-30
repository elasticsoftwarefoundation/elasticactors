package org.elasticsoftware.elasticactors.cluster.metrics;

import org.elasticsoftware.elasticactors.messaging.InternalMessage;
import org.springframework.core.env.Environment;

import javax.annotation.Nonnull;

import static java.lang.String.format;

public final class MetricsSettings {

    public final static MetricsSettings DISABLED =
        new MetricsSettings(false, false, false, 0L, 0L, 0L);

    @Nonnull
    public static MetricsSettings build(
        @Nonnull Environment environment,
        @Nonnull String containerType)
    {
        boolean enabled = environment.getProperty(
            format("ea.metrics.%s.messaging.enabled", containerType),
            Boolean.class,
            false
        );
        boolean enabledForUndeliverable = environment.getProperty(
            format("ea.metrics.%s.messaging.undeliverable.enabled", containerType),
            Boolean.class,
            false
        );
        boolean enabledForReactive = environment.getProperty(
            format("ea.metrics.%s.messaging.reactive.enabled", containerType),
            Boolean.class,
            false
        );
        long messageDeliveryWarnThreshold = environment.getProperty(
            format("ea.metrics.%s.messaging.delivery.warn.threshold", containerType),
            Long.class,
            0L
        );
        long messageHandlingWarnThreshold = environment.getProperty(
            format("ea.metrics.%s.messaging.handling.warn.threshold", containerType),
            Long.class,
            0L
        );
        long serializationWarnThreshold = containerType.equals("node")
            ? 0L
            : environment.getProperty(
                format("ea.metrics.%s.serialization.warn.threshold", containerType),
                Long.class,
                0L
            );

        return new MetricsSettings(
            enabled,
            enabledForUndeliverable,
            enabledForReactive,
            messageDeliveryWarnThreshold,
            messageHandlingWarnThreshold,
            serializationWarnThreshold
        );
    }

    private final boolean enabled;
    private final boolean enabledForUndeliverable;
    private final boolean enabledForReactive;
    private final long messageDeliveryWarnThreshold;
    private final long messageHandlingWarnThreshold;
    private final long serializationWarnThreshold;

    public MetricsSettings(
        boolean enabled,
        boolean enabledForUndeliverable,
        boolean enabledForReactive,
        long messageDeliveryWarnThreshold,
        long messageHandlingWarnThreshold,
        long serializationWarnThreshold)
    {
        this.enabled = enabled;
        this.enabledForUndeliverable = enabledForUndeliverable;
        this.enabledForReactive = enabledForReactive;
        this.messageDeliveryWarnThreshold = messageDeliveryWarnThreshold;
        this.messageHandlingWarnThreshold = messageHandlingWarnThreshold;
        this.serializationWarnThreshold = serializationWarnThreshold;
    }

    public boolean isEnabled(InternalMessage message) {
        return enabled
            && (enabledForUndeliverable || !message.isUndeliverable())
            && (enabledForReactive || !message.isReactive());
    }

    public boolean isMessageDeliveryWarnThresholdEnabled(InternalMessage message) {
        return isEnabled(message) && messageDeliveryWarnThreshold > 0L;
    }

    public long getMessageDeliveryWarnThreshold() {
        return messageDeliveryWarnThreshold;
    }

    public boolean isMessageHandlingWarnThresholdEnabled(InternalMessage message) {
        return isEnabled(message) && messageHandlingWarnThreshold > 0L;
    }

    public long getMessageHandlingWarnThreshold() {
        return messageHandlingWarnThreshold;
    }

    public boolean isSerializationWarnThresholdEnabled(InternalMessage message) {
        return isEnabled(message) && serializationWarnThreshold > 0L;
    }

    public long getSerializationWarnThreshold() {
        return serializationWarnThreshold;
    }

    public boolean requiresMeasurement(InternalMessage message) {
        return isMessageHandlingWarnThresholdEnabled(message)
            || isSerializationWarnThresholdEnabled(message);
    }
}
