/*
 *   Copyright 2013 - 2022 The Original Authors
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

package org.elasticsoftware.elasticactors;

import org.elasticsoftware.elasticactors.concurrent.ActorCompletableFuture;
import org.reactivestreams.Publisher;

/**
 * This is the main entry point for the ElasticActors API. When a {@link org.elasticsoftware.elasticactors.serialization.Message}
 * needs to be send to an {@link ElasticActor} first the {@link ActorRef} needs to be obtained. An Actor Reference
 * can be obtained by using the sender field from {@link ElasticActor#onReceive(ActorRef, Object)} or by getting an
 * {@link ActorRef} from the various methods on {@link ActorSystem}.
 *
 * An {@link ActorRef} in serialized form looks as follows:<br>
 *
 * (Normal) Actors: {@code actor://<cluster>/<actorSystem>/shards/<shardId>/<actorId>}<br>
 * Service Actors: {@code actor://<cluster>/<actorSystem>/nodes/<nodeId>/<serviceId>}<br>
 * Temporary Actors: {@code actor://<cluster>/<actorSystem>/nodes/<nodeId>/<actorId>}<br>
 *
 *
 * @see     ActorSystem#actorFor(String)
 * @see     ActorSystem#actorOf(String, Class, ActorState)
 * @see     ActorSystem#tempActorOf(Class, ActorState)
 * @see     ActorSystem#serviceActorFor(String)
 *
 * @author Joost van de Wijgerd
 */
public interface ActorRef {
    String getActorCluster();
    /**
     * The path of an actor is the Shard (or Node) where the actor is located. It looks like:
     * {@code <actorSystem>/shards/<shardId>} or {@code <actorSystem>/nodes/<nodeId>}
     *
     * @return  the actor path as a string
     */
    String getActorPath();

    /**
     * The actor id is what the Developer names the Actor, or in case of Temporary Actors it's a
     * {@link java.util.UUID} that's assigned by the Runtime
     *
     * @see         ActorSystem#actorFor(String)
     * @see         ActorSystem#tempActorOf(Class, ActorState)
     * @see         ActorSystem#serviceActorFor(String)
     * @return      the actor id for this {@link ActorRef}
     */
    String getActorId();

    /**
     * This is the main method to send a message to an {@link ElasticActor}.
     *
     * @param message       the message to send (needs to be annotated with {@link org.elasticsoftware.elasticactors.serialization.Message}
     * @param sender        the sender, this can be self, but it can also be another {@link ActorRef}
     * @throws              MessageDeliveryException when something is wrong with the Messaging Subsystem
     */
    void tell(Object message, ActorRef sender) throws MessageDeliveryException;

    /**
     * Equivalent to calling ActorRef.tell(message,getSelf())
     *
     * @param message       the message to send (needs to be annotated with {@link org.elasticsoftware.elasticactors.serialization.Message}
     * @throws              IllegalStateException if the method is not called withing a {@link ElasticActor} lifecycle or on(Message) method
     * @throws              MessageDeliveryException when somthing is wrong with the Messaging Subsystem
     */
    void tell(Object message) throws IllegalStateException, MessageDeliveryException;

    /**
     * Send a message to an {@link ElasticActor} and request a response.
     *
     * If you're calling this from a Persistent Actor and want to persist changes made to the state
     * inside the chain returned by this method, run {@link ActorRef#ask(Object, Class, Boolean)} instead.
     *
     * @param message       the message to send (needs to be annotated with {@link org.elasticsoftware.elasticactors.serialization.Message}
     * @param responseType  the expected message type of the response
     * @param <T>           the expected message type of the response
     * @return              an {@link ActorCompletableFuture} that completes with the response message
     */
    <T> ActorCompletableFuture<T> ask(Object message, Class<T> responseType);

    /**
     * Send a message to an {@link ElasticActor} and request a response. When {@code persistOnResponse} is {@link Boolean#TRUE}
     * the calling actor's state (if it's a persistent actor) will be saved.
     *
     * @param message the message to send (needs to be annotated with {@link org.elasticsoftware.elasticactors.serialization.Message}
     * @param responseType the expected message type of the response
     * @param persistOnResponse if true, the calling actor's state (if it's a persistent actor) will be saved
     * @param <T> the expected message type of the response
     * @return an {@link ActorCompletableFuture} that completes with the response message
     */
    <T> ActorCompletableFuture<T> ask(Object message, Class<T> responseType, Boolean persistOnResponse);

    /**
     * Return whether the Actor is co-located on the same JVM as the caller. There can be significant performance
     * improvements (especially immutable and non-durable {@link org.elasticsoftware.elasticactors.serialization.Message}s
     * are highly optimized in this scenario)
     *
     * @return              true if the referenced Actor is running on the same JVM as the caller, false otherwise
     */
    boolean isLocal();

    /**
     * Returns a view of the referenced Actor as a {@link Publisher} of messages of type {@code T}. When this method is called
     * from within a {@link ActorContext} (i.e. when executing {@link ElasticActor#onReceive(ActorRef, Object)} or any
     * of the other {@link ElasticActor} lifecycle methods) the supplied {@link org.reactivestreams.Subscriber} should
     * have been obtained by calling {@link ElasticActor#asSubscriber(Class)} on the calling actor.<br><br>
     *
     * Because the {@link org.reactivestreams.Subscription} is persistent, the framework will call
     * {@link ElasticActor#asSubscriber(Class)} when deserializing the state of the calling actor.<br><br>
     *
     * It is also required for the supplied {@link org.reactivestreams.Subscriber} to extend {@link TypedSubscriber} when
     * called within a {@link ActorContext} to ensure that the implementation has access to the correct
     * {@link SubscriberContext}
     *
     * @param messageClass the message type the actor is expected to publish
     * @param <T> the message type the actor is expected to publish
     * @return a view of the referenced Actor as a {@link Publisher} of messages of type {@code T}
     */
    <T> Publisher<T> publisherOf(Class<T> messageClass);
}
