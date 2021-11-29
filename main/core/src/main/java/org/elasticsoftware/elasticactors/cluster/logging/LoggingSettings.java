package org.elasticsoftware.elasticactors.cluster.logging;

import com.google.common.collect.ImmutableMap;
import org.elasticsoftware.elasticactors.serialization.Message;
import org.springframework.core.env.ConfigurableEnvironment;
import org.springframework.core.env.EnumerablePropertySource;
import org.springframework.core.env.Environment;
import org.springframework.core.env.PropertySource;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Arrays;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public final class LoggingSettings {

    public static final String EA_METRICS_OVERRIDES = "ea.metrics.messages.overrides.";
    public static final String EA_METRICS_DEFAULT = "ea.metrics.messages.default";
    private static final Message.LogFeature[] EMPTY = new Message.LogFeature[0];

    public static final LoggingSettings DISABLED =
        new LoggingSettings(false, EMPTY, ImmutableMap.of());

    private final boolean enabled;
    private final Message.LogFeature[] defaultFeatures;
    private final ImmutableMap<String, Message.LogFeature[]> overrides;
    private final ConcurrentMap<Class<?>, Message.LogFeature[]> cache;

    public LoggingSettings(boolean enabled, @Nonnull Environment environment) {
        this(
            enabled,
            toLogFeatures(environment.getProperty(EA_METRICS_DEFAULT)),
            buildOverridesMap(environment)
        );
    }

    public LoggingSettings(
        boolean enabled,
        @Nonnull Message.LogFeature[] defaultFeatures,
        @Nonnull Map<String, Message.LogFeature[]> overrides)
    {
        this.enabled = enabled;
        this.defaultFeatures = defaultFeatures;
        this.overrides = ImmutableMap.copyOf(overrides);
        this.cache = new ConcurrentHashMap<>();
    }

    public boolean isEnabled() {
        return enabled;
    }

    /**
     * Processes the log features set for this message class.
     * <br><br>
     * The order of precedence is:
     * <ol>
     *     <li>Configured overrides</li>
     *     <li>Features defined in the {@link Message} annotation</li>
     *     <li>Configured defaults (except for messages internal to Elastic Actors)</li>
     * </ol>
     *
     * @param messageClass the message class
     * @return the list of log features for this message class, according to the order of precedence
     */
    @Nonnull
    public Message.LogFeature[] processFeatures(@Nonnull Class<?> messageClass) {
        if (!enabled) {
            return EMPTY;
        }
        // Cache because processing some internal messages can be a bit expensive
        return cache.computeIfAbsent(messageClass, this::internalProcessFeatures);
    }

    @Nonnull
    private Message.LogFeature[] internalProcessFeatures(@Nonnull Class<?> messageClass) {
        Message.LogFeature[] overriden = overrides.get(messageClass.getName());
        if (overriden != null) {
            return overriden;
        }
        Message messageAnnotation = messageClass.getAnnotation(Message.class);
        if (messageAnnotation != null && messageAnnotation.logOnReceive().length > 0) {
            return messageAnnotation.logOnReceive();
        }
        if (messageClass.getName().startsWith("org.elasticsoftware.elasticactors.")) {
            return EMPTY;
        }
        return defaultFeatures;
    }

    @Nonnull
    private static Message.LogFeature[] toLogFeatures(@Nullable String value) {
        if (value == null) {
            return EMPTY;
        }
        return Arrays.stream(value.split(","))
            .map(String::trim)
            .filter(s -> !s.isEmpty())
            .map(String::toUpperCase)
            .map(Message.LogFeature::valueOf)
            .distinct()
            .toArray(Message.LogFeature[]::new);
    }

    @Nonnull
    private static ImmutableMap<String, Message.LogFeature[]> buildOverridesMap(@Nonnull Environment environment) {
        ImmutableMap.Builder<String, Message.LogFeature[]> mapBuilder = ImmutableMap.builder();
        if (environment instanceof ConfigurableEnvironment) {
            for (PropertySource<?> propertySource : ((ConfigurableEnvironment) environment).getPropertySources()) {
                if (propertySource instanceof EnumerablePropertySource) {
                    for (String key : ((EnumerablePropertySource<?>) propertySource).getPropertyNames()) {
                        if (key.length() > EA_METRICS_OVERRIDES.length() && key.startsWith(EA_METRICS_OVERRIDES)) {
                            Object property = propertySource.getProperty(key);
                            if (property != null) {
                                String value = property.toString();
                                Message.LogFeature[] features = LoggingSettings.toLogFeatures(value);
                                String className = key.substring(EA_METRICS_OVERRIDES.length());
                                mapBuilder.put(className, features);
                            }
                        }
                    }
                }
            }
        }
        return mapBuilder.build();
    }

}
