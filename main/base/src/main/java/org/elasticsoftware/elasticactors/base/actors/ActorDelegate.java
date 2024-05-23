/*
 * Copyright 2013 - 2024 The Original Authors
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

package org.elasticsoftware.elasticactors.base.actors;

import com.google.common.collect.ImmutableMap;
import org.elasticsoftware.elasticactors.ActorContextHolder;
import org.elasticsoftware.elasticactors.ActorNotFoundException;
import org.elasticsoftware.elasticactors.ActorRef;
import org.elasticsoftware.elasticactors.ActorState;
import org.elasticsoftware.elasticactors.TypedActor;
import org.elasticsoftware.elasticactors.UnexpectedResponseTypeException;
import org.elasticsoftware.elasticactors.concurrent.Expirable;
import org.elasticsoftware.elasticactors.serialization.NoopSerializationFramework;
import org.elasticsoftware.elasticactors.serialization.SerializationFramework;
import org.elasticsoftware.elasticactors.tracing.CreationContext;
import org.elasticsoftware.elasticactors.tracing.MessagingContextManager;
import org.elasticsoftware.elasticactors.tracing.MessagingContextManager.MessagingScope;
import org.elasticsoftware.elasticactors.tracing.TraceContext;
import org.elasticsoftware.elasticactors.tracing.Traceable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.TimeUnit;

import static java.lang.String.format;

/**
 * @author Joost van de Wijgerd
 */
public abstract class ActorDelegate<T>
    extends TypedActor<T>
    implements ActorState<ActorDelegate<T>>, Traceable, Expirable {

    private final static Logger staticLogger = LoggerFactory.getLogger(ActorDelegate.class);

    private final TraceContext traceContext;
    private final CreationContext creationContext;

    /**
     * Default implementation that uses the static logger for {@link ActorDelegate}.
     * Although the user can override it, {@link ActorDelegate} is often used for anonymous
     * subclassing, so this is a valuable optimization.
     */
    @Override
    protected Logger initLogger() {
        return staticLogger;
    }

    private final boolean deleteAfterReceive;

    private final long expirationTime;

    /**
     * Creates an ActorDelegate that will be stopped after it receives a message and will expire
     * at the default time after {@link Expirable#TEMP_ACTOR_TIMEOUT_DEFAULT} milliseconds.
     */
    protected ActorDelegate() {
        this(true);
    }

    /**
     * Creates an ActorDelegate that will expire at the default time after
     * {@link Expirable#TEMP_ACTOR_TIMEOUT_DEFAULT} milliseconds
     *
     * @param deleteAfterReceive true if the actor is meant to be stopped after it receives any
     * message. If false, the actor must be manually stopped.
     */
    protected ActorDelegate(boolean deleteAfterReceive) {
        this(deleteAfterReceive, TEMP_ACTOR_TIMEOUT_DEFAULT);
    }

    /**
     * Creates an ActorDelegate.
     *
     * @param deleteAfterReceive true if the actor is meant to be stopped after it receives any
     * message. If false, the actor must be manually stopped.
     * @param timeoutMillis timeout is applied to this actor (in milliseconds). After this amount
     * of time has passed, the actor will be destroyed. The value will be clamped so that it sits
     * between {@link Expirable#TEMP_ACTOR_TIMEOUT_MIN} and {@link Expirable#TEMP_ACTOR_TIMEOUT_MAX}.
     */
    protected ActorDelegate(boolean deleteAfterReceive, long timeoutMillis) {
        this.deleteAfterReceive = deleteAfterReceive;
        this.expirationTime = System.currentTimeMillis() + Expirable.clamp(
            timeoutMillis,
            TEMP_ACTOR_TIMEOUT_MIN,
            TEMP_ACTOR_TIMEOUT_MAX
        );
        MessagingScope currentScope = MessagingContextManager.getManager().currentScope();
        if (currentScope != null) {
            traceContext = currentScope.getTraceContext();
            creationContext = currentScope.getCreationContext();
        } else {
            traceContext = null;
            creationContext = null;
        }
    }

    /**
     * Creates an ActorDelegate.
     *
     * @param deleteAfterReceive true if the actor is meant to be stopped after it receives any
     * message. If false, the actor must be manually stopped.
     * @param timeout timeout is applied to this actor (in the specified time unit).
     * After this amount of time has passed, the actor will be destroyed. The converted value will
     * be clamped so that it sits between {@link Expirable#TEMP_ACTOR_TIMEOUT_MIN} and
     * {@link Expirable#TEMP_ACTOR_TIMEOUT_MAX}.
     * @param timeUnit the unit of time in which timeout is represented.
     */
    protected ActorDelegate(boolean deleteAfterReceive, long timeout, @Nonnull TimeUnit timeUnit) {
        this(deleteAfterReceive, Objects.requireNonNull(timeUnit).toMillis(timeout));
    }

    public boolean isDeleteAfterReceive() {
        return deleteAfterReceive;
    }

    @Override
    public long getExpirationTime() {
        return expirationTime;
    }

    @Override
    @Nullable
    public TraceContext getTraceContext() {
        return traceContext;
    }

    @Override
    @Nullable
    public CreationContext getCreationContext() {
        return creationContext;
    }

    @Override
    public ActorDelegate<T> getBody() {
        return this;
    }

    @Override
    public Class<? extends SerializationFramework> getSerializationFramework() {
        return NoopSerializationFramework.class;
    }

    public static <D> PreparatoryStep<D> builder() {
        return new Builder<>();
    }

    private static final class FunctionalActorDelegate<D> extends ActorDelegate<D> {

        private final static Logger staticLogger = LoggerFactory.getLogger(FunctionalActorDelegate.class);

        @Override
        protected Logger initLogger() {
            return staticLogger;
        }

        private final Map<Class<?>, MessageConsumer<?>> onReceiveConsumers;
        private final MessageConsumer<D> orElseConsumer;
        private final MessageConsumer<Object> onUndeliverableConsumer;
        private final MessageConsumer<D> preReceiveConsumer;
        private final MessageConsumer<D> postReceiveConsumer;

        private FunctionalActorDelegate(
            Map<Class<?>, MessageConsumer<?>> onReceiveConsumers,
            MessageConsumer<D> orElseConsumer,
            MessageConsumer<Object> onUndeliverableConsumer,
            MessageConsumer<D> preReceiveConsumer,
            MessageConsumer<D> postReceiveConsumer,
            boolean deleteAfterReceive,
            long timeout)
        {
            super(deleteAfterReceive, timeout);
            this.onReceiveConsumers = ImmutableMap.copyOf(onReceiveConsumers);
            this.orElseConsumer = orElseConsumer;
            this.onUndeliverableConsumer = onUndeliverableConsumer;
            this.preReceiveConsumer = preReceiveConsumer;
            this.postReceiveConsumer = postReceiveConsumer;
        }

        @Override
        public void onReceive(ActorRef sender, D message) throws Exception {
            Class<?> messageClass = message.getClass();
            MessageConsumer consumer = onReceiveConsumers.get(messageClass);
            if (consumer == null) {
                for (Class<?> key : onReceiveConsumers.keySet()) {
                    if (key.isAssignableFrom(messageClass)) {
                        consumer = onReceiveConsumers.get(key);
                        break;
                    }
                }
            }
            if (consumer != null) {
                runIfPresent(sender, message, preReceiveConsumer);
                consumer.accept(sender, message);
                runIfPresent(sender, message, postReceiveConsumer);
            } else if (orElseConsumer != null) {
                runIfPresent(sender, message, preReceiveConsumer);
                orElseConsumer.accept(sender, message);
                runIfPresent(sender, message, postReceiveConsumer);
            } else {
                throw new UnexpectedResponseTypeException(
                    String.format(
                        "Receiver unexpectedly responded with a message of type [%s]",
                        messageClass.getTypeName()
                    ));
            }
        }

        private void runIfPresent(ActorRef sender, D message, MessageConsumer<D> consumer) throws Exception {
            if (consumer != null) {
                consumer.accept(sender, message);
            }
        }

        @Override
        public void onUndeliverable(ActorRef receiver, Object message) throws Exception {
            if (onUndeliverableConsumer != null) {
                onUndeliverableConsumer.accept(receiver, message);
            } else {
                throw new ActorNotFoundException(format("Actor with id %s does not exist", receiver.getActorId()), receiver);
            }
        }
    }

    public interface BuildStep<D> {

        /**
         * Builds the ActorDelegate actor
         *
         * @return A new ActorDelegate based on this builder
         */
        @Nonnull
        ActorDelegate<D> build();
    }

    public interface PreparatoryStep<D> extends TimeoutStep<D> {

        /**
         * Sets whether the actor should be stopped after receiving a message. Defaults to
         * {@code true}.
         *
         * @param deleteAfterReceive If true, this actor will be stopped as soon as it receives a
         * message
         * @return A {@link TimeoutStep} version of this builder
         */
        @Nonnull
        TimeoutStep<D> deleteAfterReceive(boolean deleteAfterReceive);
    }

    public interface TimeoutStep<D> extends PreReceiveStep<D> {

        /**
         * Sets the timeout for this actor.
         * The value will be clamped so that it sits between
         * {@link Expirable#TEMP_ACTOR_TIMEOUT_MIN} and
         * {@link Expirable#TEMP_ACTOR_TIMEOUT_MAX}.
         * Defaults to {@link Expirable#TEMP_ACTOR_TIMEOUT_DEFAULT}.
         *
         * @param timeoutMillis The timeout for this actor (in milliseconds).
         * @return A {@link PreReceiveStep} version of this builder
         */
        @Nonnull
        PreReceiveStep<D> timeout(long timeoutMillis);

        /**
         * Sets the timeout for this actor.
         * The converted value will be clamped so that it sits between
         * {@link Expirable#TEMP_ACTOR_TIMEOUT_MIN} and
         * {@link Expirable#TEMP_ACTOR_TIMEOUT_MAX}.
         * Defaults to {@link Expirable#TEMP_ACTOR_TIMEOUT_DEFAULT}.
         *
         * @param timeout The timeout for this actor (in the unit specified).
         * @param timeUnit The unit of time to use for the timeout.
         * @return A {@link PreReceiveStep} version of this builder
         */
        @Nonnull
        PreReceiveStep<D> timeout(long timeout, @Nonnull TimeUnit timeUnit);
    }

    public interface PreReceiveStep<D> extends MessageHandlingStep<D> {

        /**
         * Sets a consumer that must be executed before each message is received.
         *
         * @param consumer The consumer
         * @return A {@link MessageHandlingStep} version of this builder
         */
        @Nonnull
        MessageHandlingStep<D> preReceive(@Nonnull MessageConsumer<D> consumer);

        /**
         * A convenience alias to {@link PreReceiveStep#preReceive(MessageConsumer)}
         */
        @Nonnull
        MessageHandlingStep<D> preReceive(@Nonnull ThrowingConsumer<D> consumer);

        /**
         * A convenience alias to {@link PreReceiveStep#preReceive(MessageConsumer)}
         */
        @Nonnull
        MessageHandlingStep<D> preReceive(@Nonnull ThrowingRunnable runnable);
    }

    public interface MessageHandlingStep<D> extends PostReceiveStep<D> {

        /**
         * Adds a message consumer for a given message type, replacing any consumer previously
         * assigned to that type, if any.
         *
         * @param tClass The class of the messages
         * @param consumer The consumer
         * @param <M> The type of the messages
         * @return This builder
         */
        @Nonnull
        <M extends D> MessageHandlingStep<D> onReceive(@Nonnull Class<M> tClass, @Nonnull MessageConsumer<M> consumer);

        /**
         * A convenience alias to {@link MessageHandlingStep#onReceive(Class, MessageConsumer)}
         */
        @Nonnull
        <M extends D> MessageHandlingStep<D> onReceive(@Nonnull Class<M> tClass, @Nonnull ThrowingConsumer<M> consumer);

        /**
         * A convenience alias to {@link MessageHandlingStep#onReceive(Class, MessageConsumer)}
         */
        @Nonnull
        <M extends D> MessageHandlingStep<D> onReceive(@Nonnull Class<M> tClass, @Nonnull ThrowingRunnable runnable);

        /**
         * Adds a message consumer for any message types not covered by the current consumers.
         * <br><br>
         *
         * Note that, if no consumer for unexpected types is provided, the actor will throw an
         * {@link UnexpectedResponseTypeException} if a message of an unknown type is received.
         * <br><br>
         *
         * A convenience constant for when such behavior is not desired is provided with
         * {@link MessageConsumer#noop()}
         *
         * @param consumer The consumer
         * @return A {@link PostReceiveStep} version of this builder
         */
        @Nonnull
        PostReceiveStep<D> orElse(@Nonnull MessageConsumer<D> consumer);

        /**
         * A convenience alias to {@link MessageHandlingStep#orElse(MessageConsumer)}
         */
        @Nonnull
        PostReceiveStep<D> orElse(@Nonnull ThrowingConsumer<D> consumer);

        /**
         * A convenience alias to {@link MessageHandlingStep#orElse(MessageConsumer)}
         */
        @Nonnull
        PostReceiveStep<D> orElse(@Nonnull ThrowingRunnable runnable);
    }

    public interface PostReceiveStep<D> extends OnUndeliverableStep<D> {

        /**
         * Sets a consumer that must be executed after each message is received
         *
         * @param consumer The consumer
         * @return A {@link OnUndeliverableStep} version of this builder
         */
        @Nonnull
        OnUndeliverableStep<D> postReceive(@Nonnull MessageConsumer<D> consumer);

        /**
         * A convenience alias to {@link PostReceiveStep#postReceive(MessageConsumer)}
         */
        @Nonnull
        OnUndeliverableStep<D> postReceive(@Nonnull ThrowingConsumer<D> consumer);

        /**
         * A convenience alias to {@link PostReceiveStep#postReceive(MessageConsumer)}
         */
        @Nonnull
        OnUndeliverableStep<D> postReceive(@Nonnull ThrowingRunnable runnable);
    }

    public interface OnUndeliverableStep<D> extends BuildStep<D> {

        /**
         * Adds a message consumer for undeliverable messages. <br>
         *
         * Note that, if no consumer for undeliverable messages is set, the actor will throw an
         * {@link ActorNotFoundException} if the message could not be delivered.
         *
         * @return A {@link BuildStep} version of this builder
         */
        @Nonnull
        BuildStep<D> onUndeliverable(@Nonnull MessageConsumer<Object> consumer);

        /**
         * A convenience alias to {@link OnUndeliverableStep#onUndeliverable(MessageConsumer)}
         */
        @Nonnull
        BuildStep<D> onUndeliverable(@Nonnull ThrowingConsumer<Object> consumer);

        /**
         * A convenience alias to {@link OnUndeliverableStep#onUndeliverable(MessageConsumer)}
         */
        @Nonnull
        BuildStep<D> onUndeliverable(@Nonnull ThrowingRunnable runnable);
    }

    public final static class Builder<D> implements MessageHandlingStep<D>, PreparatoryStep<D> {

        /**
         * Convenience function for stopping the actor.
         * <br><br>
         * <strong>This only works inside actor context.</strong><br>
         * The point of this method is to make stopping an ActorDelegate cleaner.
         * <br><br>
         * Example:
         * <pre>{@code builder.onReceive(MessageClass.class, m -> {
         *     doSomething(m);
         *     stopActor();
         * });}
         * </pre>
         */
        public static void stopActor() throws Exception {
            ActorContextHolder.getSystem().stop(ActorContextHolder.getSelf());
        }

        private final Map<Class<?>, MessageConsumer<?>> onReceiveConsumers = new LinkedHashMap<>();
        private MessageConsumer<D> orElseConsumer;
        private MessageConsumer<Object> onUndeliverableConsumer;
        private MessageConsumer<D> preReceiveConsumer;
        private MessageConsumer<D> postReceiveConsumer;
        private boolean deleteAfterReceive = true;
        private long timeout = TEMP_ACTOR_TIMEOUT_DEFAULT;

        @Nonnull
        @Override
        public ActorDelegate<D> build() {
            return new FunctionalActorDelegate<>(
                onReceiveConsumers,
                orElseConsumer,
                onUndeliverableConsumer,
                preReceiveConsumer,
                postReceiveConsumer,
                deleteAfterReceive,
                timeout
            );
        }

        @Nonnull
        @Override
        public TimeoutStep<D> deleteAfterReceive(boolean deleteAfterReceive) {
            this.deleteAfterReceive = deleteAfterReceive;
            return this;
        }

        @Nonnull
        @Override
        public PreReceiveStep<D> timeout(long timeoutMillis) {
            this.timeout = timeoutMillis;
            return this;
        }

        @Nonnull
        @Override
        public PreReceiveStep<D> timeout(long timeout, @Nonnull TimeUnit timeUnit) {
            this.timeout = Objects.requireNonNull(timeUnit).toMillis(timeout);
            return this;
        }

        @Nonnull
        @Override
        public MessageHandlingStep<D> preReceive(@Nonnull MessageConsumer<D> consumer) {
            Objects.requireNonNull(consumer);
            this.preReceiveConsumer = consumer;
            return this;
        }

        @Nonnull
        @Override
        public MessageHandlingStep<D> preReceive(@Nonnull ThrowingConsumer<D> consumer) {
            Objects.requireNonNull(consumer);
            return preReceive((a, m) -> consumer.accept(m));
        }

        @Nonnull
        @Override
        public MessageHandlingStep<D> preReceive(@Nonnull ThrowingRunnable runnable) {
            Objects.requireNonNull(runnable);
            return preReceive((a, m) -> runnable.run());
        }

        @Nonnull
        @Override
        public <M extends D> MessageHandlingStep<D> onReceive(@Nonnull Class<M> tClass, @Nonnull MessageConsumer<M> consumer) {
            Objects.requireNonNull(tClass);
            Objects.requireNonNull(consumer);
            onReceiveConsumers.put(tClass, consumer);
            return this;
        }

        @Nonnull
        @Override
        public <M extends D> MessageHandlingStep<D> onReceive(@Nonnull Class<M> tClass, @Nonnull ThrowingConsumer<M> consumer) {
            Objects.requireNonNull(tClass);
            Objects.requireNonNull(consumer);
            return onReceive(tClass, (a, m) -> consumer.accept(m));
        }

        @Nonnull
        @Override
        public <M extends D> MessageHandlingStep<D> onReceive(@Nonnull Class<M> tClass, @Nonnull ThrowingRunnable runnable) {
            Objects.requireNonNull(tClass);
            Objects.requireNonNull(runnable);
            return onReceive(tClass, (a, m) -> runnable.run());
        }

        @Nonnull
        @Override
        public PostReceiveStep<D> orElse(@Nonnull MessageConsumer<D> consumer) {
            Objects.requireNonNull(consumer);
            this.orElseConsumer = consumer;
            return this;
        }

        @Nonnull
        @Override
        public PostReceiveStep<D> orElse(@Nonnull ThrowingConsumer<D> consumer) {
            Objects.requireNonNull(consumer);
            return orElse((a, m) -> consumer.accept(m));
        }

        @Nonnull
        @Override
        public PostReceiveStep<D> orElse(@Nonnull ThrowingRunnable runnable) {
            Objects.requireNonNull(runnable);
            return orElse((a, m) -> runnable.run());
        }

        @Nonnull
        @Override
        public OnUndeliverableStep<D> postReceive(@Nonnull MessageConsumer<D> consumer) {
            Objects.requireNonNull(consumer);
            this.postReceiveConsumer = consumer;
            return this;
        }

        @Nonnull
        @Override
        public OnUndeliverableStep<D> postReceive(@Nonnull ThrowingConsumer<D> consumer) {
            Objects.requireNonNull(consumer);
            return postReceive((a, m) -> consumer.accept(m));
        }

        @Nonnull
        @Override
        public OnUndeliverableStep<D> postReceive(@Nonnull ThrowingRunnable runnable) {
            Objects.requireNonNull(runnable);
            return postReceive((a, m) -> runnable.run());
        }

        @Nonnull
        @Override
        public BuildStep<D> onUndeliverable(@Nonnull MessageConsumer<Object> consumer) {
            Objects.requireNonNull(consumer);
            this.onUndeliverableConsumer = consumer;
            return this;
        }

        @Nonnull
        @Override
        public BuildStep<D> onUndeliverable(@Nonnull ThrowingConsumer<Object> consumer) {
            Objects.requireNonNull(consumer);
            return onUndeliverable((a, m) -> consumer.accept(m));
        }

        @Nonnull
        @Override
        public BuildStep<D> onUndeliverable(@Nonnull ThrowingRunnable runnable) {
            Objects.requireNonNull(runnable);
            return onUndeliverable((a, m) -> runnable.run());
        }
    }

    @FunctionalInterface
    public interface MessageConsumer<M> {

        /**
         * A message consumer that does nothing
         */
        static <T> MessageConsumer<T> noop() {
            return (a, m) -> { };
        }

        void accept(ActorRef actorRef, M message) throws Exception;

        default MessageConsumer<M> andThen(@Nonnull MessageConsumer<M> after) {
            Objects.requireNonNull(after);
            return (a, m) -> {
                accept(a, m);
                after.accept(a, m);
            };
        }
    }

    @FunctionalInterface
    public interface ThrowingConsumer<M> {

        void accept(M message) throws Exception;

        @Nonnull
        default ThrowingConsumer<M> andThen(@Nonnull ThrowingConsumer<M> after) {
            Objects.requireNonNull(after);
            return m -> {
                accept(m);
                after.accept(m);
            };
        }
    }

    @FunctionalInterface
    public interface ThrowingRunnable {

        void run() throws Exception;

        @Nonnull
        default ThrowingRunnable andThen(@Nonnull ThrowingRunnable after) {
            Objects.requireNonNull(after);
            return () -> {
                run();
                after.run();
            };
        }
    }

}
