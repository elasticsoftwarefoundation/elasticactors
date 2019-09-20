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

    public static <D> Builder<D> builder() {
        return new Builder<>();
    }

    private static class FunctionalActorDelegate<D> extends ActorDelegate<D> {

        private final Map<Class<?>, MessageConsumer<?>> onReplyConsumers;
        private final MessageConsumer<Object> orElseConsumer;
        private final MessageConsumer<Object> onUndeliverableConsumer;

        private FunctionalActorDelegate(
                Map<Class<?>, MessageConsumer<?>> onReplyConsumers,
                MessageConsumer<Object> orElseConsumer,
                MessageConsumer<Object> onUndeliverableConsumer) {
            this.onReplyConsumers = ImmutableMap.copyOf(onReplyConsumers);
            this.orElseConsumer = orElseConsumer;
            this.onUndeliverableConsumer = onUndeliverableConsumer;
        }

        @Override
        public void onReceive(ActorRef sender, D message) throws Exception {
            MessageConsumer<D> consumer = (MessageConsumer<D>) onReplyConsumers.get(message.getClass());
            if (consumer != null) {
                consumer.accept(sender, message);
            } else if (orElseConsumer != null) {
                orElseConsumer.accept(sender, message);
            } else {
                throw new UnexpectedResponseTypeException("Receiver unexpectedly responded with a message of type " + message.getClass().getTypeName());
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

        ActorDelegate<D> build();
    }

    public interface MessageHandlingStep<D> extends OnUndeliverableStep<D> {

        /**
         * Adds a reply message consumer for a given reply type, replacing any consumer previously
         * assigned to that type, if any.
         *
         * @param tClass The class of the messages
         * @param consumer The consumer
         * @param <M> The type of the messages
         * @return This builder
         */
        <M> MessageHandlingStep<D> onReply(Class<M> tClass, MessageConsumer<M> consumer);

        /**
         * Adds a message consumer for any reply types not covered by the current reply types
         * consumers. <br/>
         *
         * Note that, if no consumer for unexpected types is provided, the actor will throw an
         * {@link UnexpectedResponseTypeException} if a message of an unknown type is received.
         *
         * @param consumer The consumer
         * @return A {@link OnUndeliverableStep} version of this builder
         */
        OnUndeliverableStep<D> orElse(MessageConsumer<Object> consumer);
    }

    public interface OnUndeliverableStep<D> extends BuildStep<D> {

        /**
         * Adds a message consumer for undeliverable messages. <br/>
         *
         * Note that, if no consumer for undeliverable messages is set, the actor will throw an
         * {@link ActorNotFoundException} if the message could not be delivered.
         */
        BuildStep<D> onUndeliverable(MessageConsumer<Object> consumer);
    }

    public final static class Builder<D> implements MessageHandlingStep<D> {

        private Map<Class<?>, MessageConsumer<?>> onReplyConsumers = new HashMap<>();
        private MessageConsumer<Object> orElseConsumer;
        private MessageConsumer<Object> onUndeliverableConsumer;

        @Override
        public ActorDelegate<D> build() {
            return new FunctionalActorDelegate<>(
                    onReplyConsumers,
                    orElseConsumer,
                    onUndeliverableConsumer);
        }

        @Override
        public <M> MessageHandlingStep<D> onReply(Class<M> tClass, MessageConsumer<M> consumer) {
            Objects.requireNonNull(tClass);
            Objects.requireNonNull(consumer);
            onReplyConsumers.put(tClass, consumer);
            return this;
        }

        @Override
        public OnUndeliverableStep<D> orElse(MessageConsumer<Object> consumer) {
            Objects.requireNonNull(consumer);
            this.orElseConsumer = consumer;
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

        void accept(ActorRef actorRef, M message) throws Exception;

        default MessageConsumer<M> andThen(MessageConsumer<? super M> after) {
            Objects.requireNonNull(after);
            return (a, m) -> {
                accept(a, m);
                after.accept(a, m);
            };
        }
    }

}
