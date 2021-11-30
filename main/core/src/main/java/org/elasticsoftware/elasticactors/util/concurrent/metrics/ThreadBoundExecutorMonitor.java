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
import java.util.Objects;
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
    private final Tags tags;
    private final String executionTimerName;
    private final String idleTimerName;
    private final ConcurrentMap<TimerCacheKey, Timer> idleTimers;
    private final ConcurrentMap<TimerCacheKey, Timer> executionTimers;
    private final ConcurrentMap<Class<? extends InternalMessage>, Timer> wrapperIdleTimers;
    private final ConcurrentMap<Class<? extends InternalMessage>, Timer> wrapperExecutionTimers;

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
        this.wrapperIdleTimers = new ConcurrentHashMap<>();
        this.wrapperExecutionTimers = new ConcurrentHashMap<>();
        this.executionTimerName = createNameForSuffix("execution");
        this.executionTimer = registry.timer(executionTimerName, this.tags);
        this.idleTimerName = createNameForSuffix("idle");
        this.idleTimer = registry.timer(idleTimerName, this.tags);
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
            wrapperExecutionTimers,
            this::createExecutionTimerForKey,
            this::createExecutionTimerForWrapper
        );
    }

    @Nonnull
    public Timer getIdleTimerFor(ThreadBoundRunnable<?> runnable) {
        return getTimerFor(
            runnable,
            idleTimer,
            idleTimers,
            wrapperIdleTimers,
            this::createIdleTimerForKey,
            this::createIdleTimerForWrapper
        );
    }

    private Timer createExecutionTimerForKey(TimerCacheKey key) {
        return createTimerForKey(key, executionTimerName);
    }

    private Timer createIdleTimerForKey(TimerCacheKey key) {
        return createTimerForKey(key, idleTimerName);
    }

    private Timer createExecutionTimerForWrapper(Class<? extends InternalMessage> wrapperClass) {
        return createTimerForWrapper(wrapperClass, executionTimerName);
    }

    private Timer createIdleTimerForWrapper(Class<? extends InternalMessage> wrapperClass) {
        return createTimerForWrapper(wrapperClass, idleTimerName);
    }

    private Timer getTimerFor(
        ThreadBoundRunnable<?> runnable,
        Timer defaultTimer,
        Map<TimerCacheKey, Timer> map,
        Map<Class<? extends InternalMessage>, Timer> wrapperMap,
        Function<TimerCacheKey, Timer> keyFunction,
        Function<Class<? extends InternalMessage>, Timer> wrapperFunction)
    {
        if (runnable instanceof MessageHandlingThreadBoundRunnable) {
            MessageHandlingThreadBoundRunnable<?> mhtbRunnable =
                (MessageHandlingThreadBoundRunnable<?>) runnable;
            if (shouldAddTagsForActor(mhtbRunnable.getActorType())) {
                TimerCacheKey pooledKey =
                    keyPool.get().fill(mhtbRunnable, configuration.isTagMessageWrapperTypes());
                // Reuse key here to avoid generating garbage, since this will be read a lot more
                // than written. Since this key is reused, we have to compute any actual results
                // in a separate step.
                Timer timer = map.get(pooledKey);
                if (timer == null) {
                    // Creating a new key here so the shared one is not put on the map
                    TimerCacheKey newKey =
                        new TimerCacheKey(mhtbRunnable, configuration.isTagMessageWrapperTypes());
                    return map.computeIfAbsent(newKey, keyFunction);
                } else {
                    return timer;
                }
            } else if (configuration.isTagMessageWrapperTypes()) {
                Class<? extends InternalMessage> wrapperClass =
                    mhtbRunnable.getInternalMessage().getClass();
                return wrapperMap.computeIfAbsent(wrapperClass, wrapperFunction);
            }
        }
        return defaultTimer;
    }

    private Timer createTimerForKey(TimerCacheKey key, String timerName) {
        Tags tags = getTags();
        String actorTypeName = shorten(key.actorClass);
        if (actorTypeName != null) {
            tags = tags.and("elastic.actors.actor.type", actorTypeName);
        }
        if (shouldAddTagsForMessage(key.messageClass)) {
            String messageTypeName = shorten(key.messageClass);
            if (messageTypeName != null) {
                tags = tags.and(Tag.of("elastic.actors.message.type", messageTypeName));
            }
        }
        if (configuration.isTagMessageWrapperTypes()) {
            tags = getTagsForWrapper(key.getInternalMessageClass(), tags);
        }
        return configuration.getRegistry().timer(timerName, tags);
    }

    private Timer createTimerForWrapper(Class<? extends InternalMessage> wrapperClass, String timerName) {
        Tags tags = getTagsForWrapper(wrapperClass, getTags());
        return configuration.getRegistry().timer(timerName, tags);
    }

    private Tags getTagsForWrapper(Class<? extends InternalMessage> wrapperClass, Tags tags) {
        if (wrapperClass != null) {
            String wrapperName = shorten(wrapperClass);
            if (wrapperName != null) {
                tags = tags.and(Tag.of("elastic.actors.message.wrapper", wrapperName));
            }
        }
        return tags;
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

        // Fill this key, in case it's coming from the pool
        public TimerCacheKey fill(MessageHandlingThreadBoundRunnable<?> runnable, boolean addWrapper) {
            this.actorClass = runnable.getActorType();
            this.messageClass = runnable.getMessageClass();
            this.internalMessageClass = addWrapper ? runnable.getInternalMessage().getClass() : null;
            return this;
        }

        public TimerCacheKey(){

        }

        public TimerCacheKey(MessageHandlingThreadBoundRunnable<?> runnable, boolean addWrapper) {
            this.actorClass = runnable.getActorType();
            this.messageClass = runnable.getMessageClass();
            this.internalMessageClass = addWrapper ? runnable.getInternalMessage().getClass() : null;
        }

        public Class<? extends ElasticActor> getActorClass() {
            return actorClass;
        }

        public Class<?> getMessageClass() {
            return messageClass;
        }

        @Nullable
        public Class<? extends InternalMessage> getInternalMessageClass() {
            return internalMessageClass;
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

            if (!actorClass.equals(that.actorClass)) {
                return false;
            }
            if (!messageClass.equals(that.messageClass)) {
                return false;
            }
            return Objects.equals(internalMessageClass, that.internalMessageClass);
        }

        @Override
        public int hashCode() {
            int result = actorClass.hashCode();
            result = 31 * result + messageClass.hashCode();
            result =
                31 * result + (internalMessageClass != null ? internalMessageClass.hashCode() : 0);
            return result;
        }
    }

}
