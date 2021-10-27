package org.elasticsoftware.elasticactors.cluster.metrics;

import com.google.common.collect.ImmutableMap;
import org.elasticsoftware.elasticactors.serialization.Message;

public final class MetricsSettings {

    private static final Message.LogFeature[] EMPTY = new Message.LogFeature[0];

    private final boolean loggingEnabled;
    private final boolean metricsEnabled;
    private final Long messageDeliveryWarnThreshold;
    private final Long messageHandlingWarnThreshold;
    private final Long serializationWarnThreshold;
    private final ImmutableMap<String, Message.LogFeature[]> overrides;

    public static MetricsSettings disabled() {
        return new MetricsSettings(false, false, null, null, null, ImmutableMap.of());
    }

    public MetricsSettings(
        boolean loggingEnabled,
        boolean metricsEnabled,
        Long messageDeliveryWarnThreshold,
        Long messageHandlingWarnThreshold,
        Long serializationWarnThreshold,
        ImmutableMap<String, Message.LogFeature[]> overrides)
    {
        this.loggingEnabled = loggingEnabled;
        this.metricsEnabled = metricsEnabled;
        this.messageDeliveryWarnThreshold = messageDeliveryWarnThreshold;
        this.messageHandlingWarnThreshold = messageHandlingWarnThreshold;
        this.serializationWarnThreshold = serializationWarnThreshold;
        this.overrides = overrides;
    }

    public boolean isMetricsEnabled() {
        return metricsEnabled;
    }

    public boolean isLoggingEnabled() {
        return loggingEnabled;
    }

    public boolean isMessageDeliveryWarnThresholdEnabled() {
        return metricsEnabled && messageDeliveryWarnThreshold != null;
    }

    public long getMessageDeliveryWarnThreshold() {
        return getLongValue(messageDeliveryWarnThreshold);
    }

    public boolean isMessageHandlingWarnThresholdEnabled() {
        return metricsEnabled && messageHandlingWarnThreshold != null;
    }

    public long getMessageHandlingWarnThreshold() {
        return getLongValue(messageHandlingWarnThreshold);
    }

    public boolean isSerializationWarnThresholdEnabled() {
        return metricsEnabled && serializationWarnThreshold != null;
    }

    public long getSerializationWarnThreshold() {
        return getLongValue(serializationWarnThreshold);
    }

    private static long getLongValue(Long number) {
        return number != null ? number : 0L;
    }

    public boolean requiresMeasurement() {
        return isMessageHandlingWarnThresholdEnabled() || isSerializationWarnThresholdEnabled();
    }

    public Message.LogFeature[] processOverrides(Class<?> messageClass) {
        Message.LogFeature[] overriden = overrides.get(messageClass.getName());
        if (overriden != null) {
            return overriden;
        }
        Message messageAnnotation = messageClass.getAnnotation(Message.class);
        if (messageAnnotation != null) {
            return messageAnnotation.logOnReceive();
        }
        return EMPTY;
    }
}
