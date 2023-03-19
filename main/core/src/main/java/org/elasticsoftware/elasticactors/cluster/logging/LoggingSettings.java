/*
 * Copyright 2013 - 2023 The Original Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 *     you may not use this file except in compliance with the License.
 *     You may obtain a copy of the License at
 *
 *           http://www.apache.org/licenses/LICENSE-2.0
 *
 *     Unless required by applicable law or agreed to in writing, software
 *     distributed under the License is distributed on an "AS IS" BASIS,
 *     WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *     See the License for the specific language governing permissions and
 *     limitations under the License.
 *
 */

package org.elasticsoftware.elasticactors.cluster.logging;

import com.google.common.collect.ImmutableMap;
import org.elasticsoftware.elasticactors.messaging.InternalMessage;
import org.elasticsoftware.elasticactors.serialization.Message;
import org.springframework.core.env.Environment;

import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;
import java.util.Arrays;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import static org.elasticsoftware.elasticactors.util.EnvironmentUtils.getKeyValuePairsUnderPrefix;

import static java.lang.String.format;

public final class LoggingSettings {

    private static final Message.LogFeature[] EMPTY = new Message.LogFeature[0];

    public static final LoggingSettings DISABLED =
        new LoggingSettings(false, false, false, EMPTY, ImmutableMap.of());

    @Nonnull
    public static LoggingSettings build(
        @Nonnull Environment environment,
        @Nonnull String containerType)
    {
        boolean enabled = environment.getProperty(
            format("ea.logging.%s.messaging.enabled", containerType),
            Boolean.class,
            false
        );
        boolean enabledForUndeliverable = environment.getProperty(
            format("ea.logging.%s.messaging.undeliverable.enabled", containerType),
            Boolean.class,
            false
        );
        boolean enabledForReactive = environment.getProperty(
            format("ea.logging.%s.messaging.reactive.enabled", containerType),
            Boolean.class,
            false
        );

        return new LoggingSettings(
            enabled,
            enabledForUndeliverable,
            enabledForReactive,
            toLogFeatures(environment.getProperty("ea.logging.messages.default")),
            getKeyValuePairsUnderPrefix(
                environment,
                "ea.logging.messages.overrides",
                LoggingSettings::toLogFeatures
            )
        );
    }

    private final boolean enabled;
    private final boolean enabledForUndeliverable;
    private final boolean enabledForReactive;
    private final Message.LogFeature[] defaultFeatures;
    private final ImmutableMap<String, Message.LogFeature[]> overrides;

    private final transient ConcurrentMap<Class<?>, Message.LogFeature[]> cache;

    public LoggingSettings(
        boolean enabled,
        boolean enabledForUndeliverable,
        boolean enabledForReactive,
        @Nonnull Message.LogFeature[] defaultFeatures,
        @Nonnull Map<String, Message.LogFeature[]> overrides)
    {
        this.enabled = enabled;
        this.enabledForUndeliverable = enabledForUndeliverable;
        this.enabledForReactive = enabledForReactive;
        this.defaultFeatures = defaultFeatures;
        this.overrides = ImmutableMap.copyOf(overrides);
        this.cache = enabled ? new ConcurrentHashMap<>() : null;
    }

    public boolean isEnabled(InternalMessage message) {
        return enabled
            && (enabledForUndeliverable || !message.isUndeliverable())
            && (enabledForReactive || !message.isReactive());
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
        if (value == null || (value = value.trim()).isEmpty()) {
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

}
