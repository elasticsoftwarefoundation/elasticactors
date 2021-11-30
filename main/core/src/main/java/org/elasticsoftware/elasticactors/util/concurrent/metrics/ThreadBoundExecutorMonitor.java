package org.elasticsoftware.elasticactors.util.concurrent.metrics;

import com.google.common.collect.ImmutableSet;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tag;
import io.micrometer.core.instrument.Tags;
import io.micrometer.core.instrument.Timer;
import org.elasticsoftware.elasticactors.ElasticActor;
import org.elasticsoftware.elasticactors.cluster.metrics.MicrometerConfiguration;
import org.elasticsoftware.elasticactors.cluster.metrics.MicrometerTagCustomizer;
import org.elasticsoftware.elasticactors.messaging.InternalMessage;
import org.elasticsoftware.elasticactors.util.concurrent.MessageHandlingThreadBoundRunnable;
import org.elasticsoftware.elasticactors.util.concurrent.ThreadBoundRunnable;
import org.springframework.core.env.Environment;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.function.Function;

import static org.elasticsoftware.elasticactors.tracing.TracingUtils.shorten;

public final class ThreadBoundExecutorMonitor {

    // Key pool to prevent a lot of garbage being generated
    private final ThreadLocal<TimerCacheKey> keyPool = ThreadLocal.withInitial(TimerCacheKey::new);

    private final MicrometerConfiguration configuration;
    private final Timer executionTimer;
    private final Timer idleTimer;
    private final Timer deliveryTimer;
    private final Tags tags;
    private final String executionTimerName;
    private final String idleTimerName;
    private final String deliveryTimerName;
    private final ConcurrentMap<TimerCacheKey, Timer> idleTimers;
    private final ConcurrentMap<TimerCacheKey, Timer> executionTimers;
    private final ConcurrentMap<TimerCacheKey, Timer> deliveryTimers;

    @Nullable
    public static ThreadBoundExecutorMonitor build(
        @Nonnull Environment env,
        @Nullable MeterRegistry meterRegistry,
        @Nonnull String executorName,
        @Nullable MicrometerTagCustomizer tagCustomizer)
    {
        MicrometerConfiguration configuration =
            MicrometerConfiguration.build(env, meterRegistry, executorName, tagCustomizer);
        if (configuration != null) {
            return new ThreadBoundExecutorMonitor(configuration);
        } else {
            return null;
        }
    }

    public ThreadBoundExecutorMonitor(@Nonnull MicrometerConfiguration configuration) {
        this.configuration = configuration;
        this.tags = Tags.concat(configuration.getTags(), "name", configuration.getComponentName());
        MeterRegistry registry = configuration.getRegistry();

        this.idleTimers = new ConcurrentHashMap<>();
        this.executionTimers = new ConcurrentHashMap<>();
        this.deliveryTimers = new ConcurrentHashMap<>();
        this.executionTimerName = createNameForSuffix("execution");
        this.executionTimer = registry.timer(executionTimerName, this.tags);
        this.idleTimerName = createNameForSuffix("idle");
        this.idleTimer = registry.timer(idleTimerName, this.tags);
        if (configuration.isMeasureDeliveryTimes()) {
            this.deliveryTimerName = createNameForSuffix("delivery");
            this.deliveryTimer = registry.timer(deliveryTimerName, this.tags);
        } else {
            this.deliveryTimerName = null;
            this.deliveryTimer = null;
        }
    }

    @Nonnull
    public MicrometerConfiguration getConfiguration() {
        return configuration;
    }

    @Nonnull
    public Tags getTags() {
        return tags;
    }

    @Nonnull
    public Timer getExecutionTimer() {
        return executionTimer;
    }

    @Nonnull
    public Timer getIdleTimer() {
        return idleTimer;
    }

    @Nonnull
    public Timer getExecutionTimerFor(ThreadBoundRunnable<?> runnable) {
        return getTimerFor(
            runnable,
            executionTimer,
            executionTimers,
            this::createExecutionTimerForKey
        );
    }

    @Nonnull
    public Timer getIdleTimerFor(ThreadBoundRunnable<?> runnable) {
        return getTimerFor(
            runnable,
            idleTimer,
            idleTimers,
            this::createIdleTimerForKey
        );
    }

    @Nullable
    public Timer getDeliveryTimerFor(ThreadBoundRunnable<?> runnable) {
        if (deliveryTimer != null) {
            return getTimerFor(
                runnable,
                deliveryTimer,
                deliveryTimers,
                this::createDeliveryTimerForKey
            );
        } else {
            return null;
        }
    }

    private Timer createExecutionTimerForKey(TimerCacheKey key) {
        return createTimerForKey(key, executionTimerName);
    }

    private Timer createIdleTimerForKey(TimerCacheKey key) {
        return createTimerForKey(key, idleTimerName);
    }

    private Timer createDeliveryTimerForKey(TimerCacheKey key) {
        return createTimerForKey(key, deliveryTimerName);
    }

    private Timer getTimerFor(
        ThreadBoundRunnable<?> runnable,
        Timer defaultTimer,
        Map<TimerCacheKey, Timer> map,
        Function<TimerCacheKey, Timer> keyFunction)
    {
        if (runnable instanceof MessageHandlingThreadBoundRunnable) {
            MessageHandlingThreadBoundRunnable<?> mhtbRunnable =
                (MessageHandlingThreadBoundRunnable<?>) runnable;
            Class<? extends ElasticActor> actorClass =
                shouldAddTagsForActor(mhtbRunnable.getActorType())
                    ? mhtbRunnable.getActorType()
                    : null;
            Class<?> messageClass =
                actorClass != null && shouldAddTagsForMessage(mhtbRunnable.getMessageClass())
                    ? mhtbRunnable.getMessageClass()
                    : null;
            Class<? extends InternalMessage> internalMessageClass =
                configuration.isTagMessageWrapperTypes()
                    ? mhtbRunnable.getInternalMessage().getClass()
                    : null;
            Class<? extends ThreadBoundRunnable> runnableClass =
                configuration.isTagTaskTypes()
                    ? mhtbRunnable.getClass()
                    : null;
            if (actorClass != null || internalMessageClass != null || runnableClass != null) {
                TimerCacheKey pooledKey = keyPool.get().fill(
                    actorClass,
                    messageClass,
                    internalMessageClass,
                    runnableClass
                );
                // Reuse key here to avoid generating garbage, since this will be read a lot more
                // than written. Since this key is reused, we have to compute any actual results
                // in a separate step.
                Timer timer = map.get(pooledKey);
                if (timer == null) {
                    // Creating a new key here so the shared one is not put on the map
                    TimerCacheKey newKey = TimerCacheKey.of(
                        actorClass,
                        messageClass,
                        internalMessageClass,
                        runnableClass
                    );
                    return map.computeIfAbsent(newKey, keyFunction);
                } else {
                    return timer;
                }
            }
        }
        return defaultTimer;
    }

    private Timer createTimerForKey(TimerCacheKey key, String timerName) {
        Tags tags = getTags();
        if (key.actorClass != null) {
            String actorTypeName = shorten(key.actorClass);
            if (actorTypeName != null) {
                tags = tags.and("elastic.actors.actor.type", actorTypeName);
            }
        }
        if (key.messageClass != null) {
            String messageTypeName = shorten(key.messageClass);
            if (messageTypeName != null) {
                tags = tags.and(Tag.of("elastic.actors.message.type", messageTypeName));
            }
        }
        if (key.internalMessageClass != null) {
            String wrapperName = shorten(key.internalMessageClass);
            if (wrapperName != null) {
                tags = tags.and(Tag.of("elastic.actors.message.wrapper", wrapperName));
            }
        }
        if (key.runnableClass != null) {
            String taskName = shorten(key.runnableClass);
            if (taskName != null) {
                tags = tags.and(Tag.of("elastic.actors.message.task", taskName));
            }
        }
        return configuration.getRegistry().timer(timerName, tags);
    }

    @Nonnull
    public String createNameForSuffix(String metricSuffix) {
        return configuration.getMetricPrefix() + "threadbound.executor." + metricSuffix;
    }

    public boolean shouldAddTagsForActor(Class<? extends ElasticActor> elasticActor) {
        ImmutableSet<String> typesForTagging = configuration.getAllowedActorTypesForTagging();
        return elasticActor != null
            && !typesForTagging.isEmpty()
            && typesForTagging.contains(elasticActor.getName());
    }

    public boolean shouldAddTagsForMessage(Class<?> messageClass) {
        ImmutableSet<String> typesForTagging = configuration.getAllowedMessageTypesForTagging();
        return messageClass != null
            && !typesForTagging.isEmpty()
            && (typesForTagging.contains("all") || typesForTagging.contains(messageClass.getName()));
    }

    private static final class TimerCacheKey {
        private Class<? extends ElasticActor> actorClass;
        private Class<?> messageClass;
        private Class<? extends InternalMessage> internalMessageClass;
        private Class<? extends ThreadBoundRunnable> runnableClass;

        // Fill this key, in case it's coming from the pool
        public TimerCacheKey fill(
            Class<? extends ElasticActor> actorClass,
            Class<?> messageClass,
            Class<? extends InternalMessage> internalMessageClass,
            Class<? extends ThreadBoundRunnable> runnableClass)
        {
            this.actorClass = actorClass;
            this.messageClass = messageClass;
            this.internalMessageClass = internalMessageClass;
            this.runnableClass = runnableClass;
            return this;
        }

        public static TimerCacheKey of(
            Class<? extends ElasticActor> actorClass,
            Class<?> messageClass,
            Class<? extends InternalMessage> internalMessageClass,
            Class<? extends ThreadBoundRunnable> runnableClass)
        {
            return new TimerCacheKey().fill(
                actorClass,
                messageClass,
                internalMessageClass,
                runnableClass
            );
        }
    }

}
