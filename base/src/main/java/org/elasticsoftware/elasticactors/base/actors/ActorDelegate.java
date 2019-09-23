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
import org.elasticsoftware.elasticactors.ActorNotFoundException;
import org.elasticsoftware.elasticactors.ActorRef;
import org.elasticsoftware.elasticactors.ActorState;
import org.elasticsoftware.elasticactors.TypedActor;
import org.elasticsoftware.elasticactors.UnexpectedResponseTypeException;
import org.elasticsoftware.elasticactors.serialization.NoopSerializationFramework;
import org.elasticsoftware.elasticactors.serialization.SerializationFramework;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

import static java.lang.String.format;

/**
 * @author Joost van de Wijgerd
 */
public abstract class ActorDelegate<T> extends TypedActor<T> implements ActorState<ActorDelegate<T>> {
    private final boolean deleteAfterReceive;

    protected ActorDelegate() {
        this(true);
    }

    protected ActorDelegate(boolean deleteAfterReceive) {
        this.deleteAfterReceive = deleteAfterReceive;
    }

    public boolean isDeleteAfterReceive() {
        return deleteAfterReceive;
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

    private static class FunctionalActorDelegate<D> extends ActorDelegate<D> {

        private final Map<Class<?>, MessageConsumer<?>> onReceiveConsumers;
        private final MessageConsumer<Object> orElseConsumer;
        private final MessageConsumer<Object> onUndeliverableConsumer;
        private final MessageConsumer<Object> preReceiveConsumer;
        private final MessageConsumer<Object> postReceiveConsumer;

        private FunctionalActorDelegate(
                Map<Class<?>, MessageConsumer<?>> onReceiveConsumers,
                MessageConsumer<Object> orElseConsumer,
                MessageConsumer<Object> onUndeliverableConsumer,
                MessageConsumer<Object> preReceiveConsumer,
                MessageConsumer<Object> postReceiveConsumer,
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
            MessageConsumer<? super D> consumer = (MessageConsumer<? super D>) onReceiveConsumers.get(message.getClass());
            if (consumer != null) {
                runIfPresent(sender, message, preReceiveConsumer);
                consumer.accept(sender, message);
                runIfPresent(sender, message, postReceiveConsumer);
            } else if (orElseConsumer != null) {
                runIfPresent(sender, message, preReceiveConsumer);
                orElseConsumer.accept(sender, message);
                runIfPresent(sender, message, postReceiveConsumer);
            } else {
                throw new UnexpectedResponseTypeException("Receiver unexpectedly responded with a message of type " + message.getClass().getTypeName());
            }
        }

        private void runIfPresent(ActorRef sender, D message, MessageConsumer<Object> consumer) throws Exception {
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
        ActorDelegate<D> build();
    }

    public interface PreparatoryStep<D> extends PreReceiveStep<D> {

        /**
         * Sets whether or not the actor should be stopped after receiving a message. Defaults to {@code true}.
         *
         * @param deleteAfterReceive If true, this actor will be stopped as soon as it receives a message
         * @return A {@link PreReceiveStep} version of this builder
         */
        PreReceiveStep<D> deleteAfterReceive(boolean deleteAfterReceive);
    }

    public interface PreReceiveStep<D> extends MessageHandlingStep<D> {

        /**
         * Sets a consumer that must be executed before each message is received.
         *
         * @param consumer The consumer
         * @return A {@link MessageHandlingStep} version of this builder
         */
        MessageHandlingStep<D> preReceive(MessageConsumer<Object> consumer);

        /**
         * A convenience alias to {@link PreReceiveStep#preReceive(MessageConsumer)}
         */
        default MessageHandlingStep<D> preReceive(ThrowingConsumer<Object> consumer) {
            Objects.requireNonNull(consumer);
            return preReceive((a, m) -> consumer.accept(m));
        }

        /**
         * A convenience alias to {@link PreReceiveStep#preReceive(MessageConsumer)}
         */
        default MessageHandlingStep<D> preReceive(ThrowingRunnable runnable) {
            Objects.requireNonNull(runnable);
            return preReceive((a, m) -> runnable.run());
        }
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
        <M> MessageHandlingStep<D> onReceive(Class<M> tClass, MessageConsumer<? super M> consumer);

        /**
         * A convenience alias to {@link MessageHandlingStep#onReceive(Class, MessageConsumer)}
         */
        default <M> MessageHandlingStep<D> onReceive(Class<M> tClass, ThrowingConsumer<? super M> consumer) {
            Objects.requireNonNull(tClass);
            Objects.requireNonNull(consumer);
            return onReceive(tClass, (a, m) -> consumer.accept(m));
        }

        /**
         * A convenience alias to {@link MessageHandlingStep#onReceive(Class, MessageConsumer)}
         */
        default <M> MessageHandlingStep<D> onReceive(Class<M> tClass, ThrowingRunnable runnable) {
            Objects.requireNonNull(tClass);
            Objects.requireNonNull(runnable);
            return onReceive(tClass, (a, m) -> runnable.run());
        }

        /**
         * Adds a message consumer for any message types not covered by the current consumers.
         * <br/><br/>
         *
         * Note that, if no consumer for unexpected types is provided, the actor will throw an
         * {@link UnexpectedResponseTypeException} if a message of an unknown type is received.
         * <br/><br/>
         *
         * A convenience constant for when such behavior is not desired is provided with
         * {@link MessageConsumer#NOOP}
         *
         * @param consumer The consumer
         * @return A {@link PostReceiveStep} version of this builder
         */
        PostReceiveStep<D> orElse(MessageConsumer<Object> consumer);

        /**
         * A convenience alias to {@link MessageHandlingStep#orElse(MessageConsumer)}
         */
        default PostReceiveStep<D> orElse(ThrowingConsumer<Object> consumer) {
            Objects.requireNonNull(consumer);
            return orElse((a, m) -> consumer.accept(m));
        }

        /**
         * A convenience alias to {@link MessageHandlingStep#orElse(MessageConsumer)}
         */
        default PostReceiveStep<D> orElse(ThrowingRunnable runnable) {
            Objects.requireNonNull(runnable);
            return orElse((a, m) -> runnable.run());
        }
    }

    public interface PostReceiveStep<D> extends OnUndeliverableStep<D> {

        /**
         * Sets a consumer that must be executed after each message is received
         * @param consumer The consumer
         * @return A {@link OnUndeliverableStep} version of this builder
         */
        OnUndeliverableStep<D> postReceive(MessageConsumer<Object> consumer);

        /**
         * A convenience alias to {@link PostReceiveStep#postReceive(MessageConsumer)}
         */
        default OnUndeliverableStep<D> postReceive(ThrowingConsumer<Object> consumer) {
            Objects.requireNonNull(consumer);
            return postReceive((a, m) -> consumer.accept(m));
        }

        /**
         * A convenience alias to {@link PostReceiveStep#postReceive(MessageConsumer)}
         */
        default OnUndeliverableStep<D> postReceive(ThrowingRunnable runnable) {
            Objects.requireNonNull(runnable);
            return postReceive((a, m) -> runnable.run());
        }
    }

    public interface OnUndeliverableStep<D> extends BuildStep<D> {

        /**
         * Adds a message consumer for undeliverable messages. <br/>
         *
         * Note that, if no consumer for undeliverable messages is set, the actor will throw an
         * {@link ActorNotFoundException} if the message could not be delivered.
         */
        BuildStep<D> onUndeliverable(MessageConsumer<Object> consumer);

        /**
         * A convenience alias to {@link OnUndeliverableStep#onUndeliverable(MessageConsumer)}
         */
        default BuildStep<D> onUndeliverable(ThrowingConsumer<Object> consumer) {
            Objects.requireNonNull(consumer);
            return onUndeliverable((a, m) -> consumer.accept(m));
        }

        /**
         * A convenience alias to {@link OnUndeliverableStep#onUndeliverable(MessageConsumer)}
         */
        default BuildStep<D> onUndeliverable(ThrowingRunnable runnable) {
            Objects.requireNonNull(runnable);
            return onUndeliverable((a, m) -> runnable.run());
        }
    }

    public final static class Builder<D> implements MessageHandlingStep<D>, PreparatoryStep<D> {

        private Map<Class<?>, MessageConsumer<?>> onReceiveConsumers = new HashMap<>();
        private MessageConsumer<Object> orElseConsumer;
        private MessageConsumer<Object> onUndeliverableConsumer;
        private MessageConsumer<Object> preReceiveConsumer;
        private MessageConsumer<Object> postReceiveConsumer;
        private boolean deleteAfterReceive = true;

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

        @Override
        public PreparatoryStep<D> deleteAfterReceive(boolean deleteAfterReceive) {
            this.deleteAfterReceive = deleteAfterReceive;
            return this;
        }


        @Override
        public MessageHandlingStep<D> preReceive(MessageConsumer<Object> consumer) {
            Objects.requireNonNull(consumer);
            this.preReceiveConsumer = consumer;
            return this;
        }

        @Override
        public <M> MessageHandlingStep<D> onReceive(Class<M> tClass, MessageConsumer<? super M> consumer) {
            Objects.requireNonNull(tClass);
            Objects.requireNonNull(consumer);
            onReceiveConsumers.put(tClass, consumer);
            return this;
        }

        @Override
        public PostReceiveStep<D> orElse(MessageConsumer<Object> consumer) {
            Objects.requireNonNull(consumer);
            this.orElseConsumer = consumer;
            return this;
        }

        @Override
        public OnUndeliverableStep<D> postReceive(MessageConsumer<Object> consumer) {
            Objects.requireNonNull(consumer);
            this.postReceiveConsumer = consumer;
            return this;
        }

        @Override
        public BuildStep<D> onUndeliverable(MessageConsumer<Object> consumer) {
            Objects.requireNonNull(consumer);
            this.onUndeliverableConsumer = consumer;
            return this;
        }
    }

    @FunctionalInterface
    public interface MessageConsumer<M> {

        /**
         * A message consumer that does nothing
         */
        MessageConsumer<Object> NOOP = (a, m) -> {};

        void accept(ActorRef actorRef, M message) throws Exception;

        default MessageConsumer<M> andThen(MessageConsumer<? super M> after) {
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

        default ThrowingConsumer<M> andThen(ThrowingConsumer<? super M> after) {
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

        default ThrowingRunnable andThen(ThrowingRunnable after) {
            Objects.requireNonNull(after);
            return () -> {
                run();
                after.run();
            };
        }
    }

}
