package org.elasticsoftware.elasticactors.cluster.metrics;

public final class MetricsSettings {

    private final boolean enabled;
    private final Long messageDeliveryWarnThreshold;
    private final Long messageHandlingWarnThreshold;
    private final Long serializationWarnThreshold;

    public static MetricsSettings disabled() {
        return new MetricsSettings(false, null, null, null);
    }

    public MetricsSettings(
        boolean enabled,
        Long messageDeliveryWarnThreshold,
        Long messageHandlingWarnThreshold,
        Long serializationWarnThreshold)
    {
        this.enabled = enabled;
        this.messageDeliveryWarnThreshold = messageDeliveryWarnThreshold;
        this.messageHandlingWarnThreshold = messageHandlingWarnThreshold;
        this.serializationWarnThreshold = serializationWarnThreshold;
    }

    public boolean isEnabled() {
        return enabled;
    }

    public boolean isMessageDeliveryWarnThresholdEnabled() {
        return enabled && messageDeliveryWarnThreshold != null && messageDeliveryWarnThreshold > 0L;
    }

    public long getMessageDeliveryWarnThreshold() {
        return getLongValue(messageDeliveryWarnThreshold);
    }

    public boolean isMessageHandlingWarnThresholdEnabled() {
        return enabled && messageHandlingWarnThreshold != null && messageHandlingWarnThreshold > 0L;
    }

    public long getMessageHandlingWarnThreshold() {
        return getLongValue(messageHandlingWarnThreshold);
    }

    public boolean isSerializationWarnThresholdEnabled() {
        return enabled && serializationWarnThreshold != null && serializationWarnThreshold > 0L;
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
}
