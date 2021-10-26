package org.elasticsoftware.elasticactors.cluster.metrics;

public final class MetricsSettings {

    private final boolean loggingEnabled;
    private final boolean metricsEnabled;
    private final Long messageDeliveryWarnThreshold;
    private final Long messageHandlingWarnThreshold;
    private final Long serializationWarnThreshold;

    public static MetricsSettings disabled() {
        return new MetricsSettings(false, false, null, null, null);
    }

    public MetricsSettings(
        boolean loggingEnabled,
        boolean metricsEnabled,
        Long messageDeliveryWarnThreshold,
        Long messageHandlingWarnThreshold,
        Long serializationWarnThreshold)
    {
        this.loggingEnabled = loggingEnabled;
        this.metricsEnabled = metricsEnabled;
        this.messageDeliveryWarnThreshold = messageDeliveryWarnThreshold;
        this.messageHandlingWarnThreshold = messageHandlingWarnThreshold;
        this.serializationWarnThreshold = serializationWarnThreshold;
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
}
