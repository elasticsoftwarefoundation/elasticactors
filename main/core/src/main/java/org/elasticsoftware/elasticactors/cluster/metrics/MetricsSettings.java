package org.elasticsoftware.elasticactors.cluster.metrics;

public final class MetricsSettings {

    private final boolean enabled;
    private final long messageDeliveryWarnThreshold;
    private final long messageHandlingWarnThreshold;
    private final long serializationWarnThreshold;

    public static MetricsSettings disabled() {
        return new MetricsSettings(false, 0L, 0L, 0L);
    }

    public MetricsSettings(
        boolean enabled,
        long messageDeliveryWarnThreshold,
        long messageHandlingWarnThreshold,
        long serializationWarnThreshold)
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
        return enabled && messageDeliveryWarnThreshold > 0L;
    }

    public long getMessageDeliveryWarnThreshold() {
        return messageDeliveryWarnThreshold;
    }

    public boolean isMessageHandlingWarnThresholdEnabled() {
        return enabled && messageHandlingWarnThreshold > 0L;
    }

    public long getMessageHandlingWarnThreshold() {
        return messageHandlingWarnThreshold;
    }

    public boolean isSerializationWarnThresholdEnabled() {
        return enabled && serializationWarnThreshold > 0L;
    }

    public long getSerializationWarnThreshold() {
        return serializationWarnThreshold;
    }

    public boolean requiresMeasurement() {
        return isMessageHandlingWarnThresholdEnabled() || isSerializationWarnThresholdEnabled();
    }
}
