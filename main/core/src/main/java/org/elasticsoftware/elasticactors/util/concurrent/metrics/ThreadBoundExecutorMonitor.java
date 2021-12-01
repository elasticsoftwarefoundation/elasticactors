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
import org.elasticsoftware.elasticactors.util.concurrent.WrapperThreadBoundRunnable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.core.env.Environment;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Collections;
import java.util.EnumMap;
import java.util.Map;
import java.util.Objects;
import java.util.StringJoiner;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import static org.elasticsoftware.elasticactors.tracing.TracingUtils.shorten;
import static org.elasticsoftware.elasticactors.util.concurrent.metrics.ThreadBoundExecutorMonitor.TimerType.DELIVERY;
import static org.elasticsoftware.elasticactors.util.concurrent.metrics.ThreadBoundExecutorMonitor.TimerType.EXECUTION;
import static org.elasticsoftware.elasticactors.util.concurrent.metrics.ThreadBoundExecutorMonitor.TimerType.IDLE;

public final class ThreadBoundExecutorMonitor {

    private final static Logger logger = LoggerFactory.getLogger(ThreadBoundExecutorMonitor.class);

    public enum TimerType {
        EXECUTION,
        IDLE,
        DELIVERY
    }

    // Key pool to prevent a lot of garbage being generated
    private final static ThreadLocal<TimerCacheKey> keyPool = ThreadLocal.withInitial(TimerCacheKey::new);

    private final MicrometerConfiguration configuration;
    private final Map<TimerType, Timer> timers;
    private final Tags tags;
    private final String executionTimerName;
    private final String idleTimerName;
    private final String deliveryTimerName;
    private final ConcurrentMap<TimerCacheKey, Map<TimerType, Timer>> timersMap;

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

        this.timersMap = new ConcurrentHashMap<>();
        this.executionTimerName = createNameForSuffix("execution");
        this.idleTimerName = createNameForSuffix("idle");
        this.deliveryTimerName = createNameForSuffix("delivery");

        EnumMap<TimerType, Timer> timers = new EnumMap<>(TimerType.class);
        timers.put(EXECUTION, registry.timer(executionTimerName, this.tags));
        timers.put(IDLE, registry.timer(idleTimerName, this.tags));
        if (configuration.isMeasureDeliveryTimes()) {
            timers.put(DELIVERY, registry.timer(deliveryTimerName, this.tags));
        }
        this.timers = Collections.unmodifiableMap(timers);
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
    public Map<TimerType, Timer> getTimers() {
        return timers;
    }

    @Nonnull
    public Map<TimerType, Timer> getTimersFor(ThreadBoundRunnable<?> runnable) {
        if (runnable instanceof WrapperThreadBoundRunnable) {
            runnable = ((WrapperThreadBoundRunnable<?>) runnable).unwrap();
        }
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
                Map<TimerType, Timer> timer = timersMap.get(pooledKey);
                if (timer != null) {
                    return timer;
                } else {
                    // Creating a new key here so the shared one is not put on the map
                    TimerCacheKey newKey = pooledKey.copy();
                    return timersMap.computeIfAbsent(newKey, this::createTimersForKey);
                }
            }
        }
        return timers;
    }

    private Map<TimerType, Timer> createTimersForKey(TimerCacheKey key) {
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
        if (tags == getTags()) {
            logger.error("Something went wrong with determining making a new tag for {}", key);
            return timers;
        }
        EnumMap<TimerType, Timer> newTimers = new EnumMap<>(TimerType.class);
        newTimers.put(EXECUTION, configuration.getRegistry().timer(executionTimerName, tags));
        newTimers.put(IDLE, configuration.getRegistry().timer(idleTimerName, tags));
        if (configuration.isMeasureDeliveryTimes()) {
            newTimers.put(DELIVERY, configuration.getRegistry().timer(deliveryTimerName, tags));
        }
        return Collections.unmodifiableMap(newTimers);
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
        private int hashCode;

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
            this.hashCode = internalHashCode();
            return this;
        }

        public TimerCacheKey copy() {
            TimerCacheKey copy = new TimerCacheKey();
            copy.actorClass = this.actorClass;
            copy.messageClass = this.messageClass;
            copy.internalMessageClass = this.internalMessageClass;
            copy.runnableClass = this.runnableClass;
            copy.hashCode = this.hashCode;
            return copy;
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

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (!(o instanceof TimerCacheKey)) {
                return false;
            }

            TimerCacheKey that = (TimerCacheKey) o;

            if (!Objects.equals(actorClass, that.actorClass)) {
                return false;
            }
            if (!Objects.equals(messageClass, that.messageClass)) {
                return false;
            }
            if (!Objects.equals(internalMessageClass, that.internalMessageClass)) {
                return false;
            }
            return Objects.equals(runnableClass, that.runnableClass);
        }

        @Override
        public int hashCode() {
            return hashCode;
        }

        private int internalHashCode() {
            int result = actorClass != null ? actorClass.hashCode() : 0;
            result = 31 * result + (messageClass != null ? messageClass.hashCode() : 0);
            result =
                31 * result + (internalMessageClass != null ? internalMessageClass.hashCode() : 0);
            result = 31 * result + (runnableClass != null ? runnableClass.hashCode() : 0);
            return result;
        }

        @Override
        public String toString() {
            return new StringJoiner(", ", TimerCacheKey.class.getSimpleName() + "{", "}")
                .add("actorClass=" + actorClass.getName())
                .add("messageClass=" + messageClass.getName())
                .add("internalMessageClass=" + internalMessageClass.getName())
                .add("runnableClass=" + runnableClass.getName())
                .toString();
        }
    }

}
