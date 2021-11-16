/*
 *   Copyright 2013 - 2019 The Original Authors
 *
 *   Licensed under the Apache License, Version 2.0 (the "License");
 *   you may not use this file except in compliance with the License.
 *   You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 */

package org.elasticsoftware.elasticactors.base.actors;

import com.google.common.collect.ImmutableMap;
import org.elasticsoftware.elasticactors.ActorContextHolder;
import org.elasticsoftware.elasticactors.ActorNotFoundException;
import org.elasticsoftware.elasticactors.ActorRef;
import org.elasticsoftware.elasticactors.ActorState;
import org.elasticsoftware.elasticactors.TypedActor;
import org.elasticsoftware.elasticactors.UnexpectedResponseTypeException;
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

import static java.lang.String.format;

/**
 * @author Joost van de Wijgerd
 */
public abstract class ActorDelegate<T>
    extends TypedActor<T>
    implements ActorState<ActorDelegate<T>>, Traceable {

    private final static Logger staticLogger = LoggerFactory.getLogger(ActorDelegate.class);

    private final TraceContext traceContext;
    private final CreationContext creationContext;

    /**
     * Default implementation that uses the static logger for {@link ActorDelegate}.
     * Although the user can override it, {@link ActorDelegate}  is often used for anonymous
     * subclassing, so this is a valuable optimization.
     */
    @Override
    protected Logger initLogger() {
        return staticLogger;
    }

    private final boolean deleteAfterReceive;

    protected ActorDelegate() {
        this(true);
    }

    protected ActorDelegate(boolean deleteAfterReceive) {
        this.deleteAfterReceive = deleteAfterReceive;
        MessagingScope currentScope = MessagingContextManager.getManager().currentScope();
        if (currentScope != null) {
            traceContext = currentScope.getTraceContext();
            creationContext = currentScope.getCreationContext();
        } else {
            traceContext = null;
            creationContext = null;
        }
    }

    public boolean isDeleteAfterReceive() {
        return deleteAfterReceive;
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
                boolean deleteAfterReceive) {
            super(deleteAfterReceive);
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

    public interface PreparatoryStep<D> extends PreReceiveStep<D> {

        /**
         * Sets whether or not the actor should be stopped after receiving a message.
         * Defaults to {@code true}.
         *
         * @param deleteAfterReceive If true, this actor will be stopped as soon as it receives a message
         * @return A {@link PreReceiveStep} version of this builder
         */
        @Nonnull
        PreReceiveStep<D> deleteAfterReceive(boolean deleteAfterReceive);
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

        @Nonnull
        @Override
        public ActorDelegate<D> build() {
            return new FunctionalActorDelegate<>(
                    onReceiveConsumers,
                    orElseConsumer,
                    onUndeliverableConsumer,
                    preReceiveConsumer,
                    postReceiveConsumer,
                    deleteAfterReceive);
        }

        @Nonnull
        @Override
        public PreReceiveStep<D> deleteAfterReceive(boolean deleteAfterReceive) {
            this.deleteAfterReceive = deleteAfterReceive;
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
