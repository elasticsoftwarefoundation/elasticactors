package org.elasticsoftware.elasticactors.cluster.logging;

import com.google.common.collect.ImmutableMap;
import org.elasticsoftware.elasticactors.serialization.Message;

public final class LoggingSettings {

    private static final Message.LogFeature[] EMPTY = new Message.LogFeature[0];

    private final boolean enabled;
    private final ImmutableMap<String, Message.LogFeature[]> overrides;

    public static LoggingSettings disabled() {
        return new LoggingSettings(false, ImmutableMap.of());
    }

    public LoggingSettings(
        boolean enabled,
        ImmutableMap<String, Message.LogFeature[]> overrides)
    {
        this.enabled = enabled;
        this.overrides = overrides;
    }

    public boolean isEnabled() {
        return enabled;
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
