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

package org.elasticsoftware.elasticactors;

import org.elasticsoftware.elasticactors.serialization.SerializationFramework;
import org.reactivestreams.Subscriber;

import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;

/**
 * Main interface to implement to create an Actor in the ElasticActors Framework. Defines Actor lifecycle methods
 * and Receive and Undeliverable message methods. The interface has a couple of convenient base implementation and
 * should normally not be implemented directly.
 *
 * @author Joost van de Wijgerd
 * @see     TypedActor
 * @see     UntypedActor
 * @see     MethodActor
 */
public interface ElasticActor<T> {
    /**
     * Called after the instance of the {@link ElasticActor} was created. Will be called exactly once in the
     * lifecycle of an Actor. Access to the {@link ActorContext} can be obtained by using the {@link ActorContextHolder} methods
     *
     * @param creator       {@link ActorRef} to the Actor that created this instance or null if {@link ActorSystem#actorOf(String, Class, ActorState)} was called from other code
     * @throws Exception    when something unexpected happens
     * @see                 org.elasticsoftware.elasticactors.ActorContextHolder#getSelf()
     * @see                 ActorContextHolder#getState(Class)
     * @see                 org.elasticsoftware.elasticactors.ActorContextHolder#getSystem()
     */
    void postCreate(@Nullable ActorRef creator) throws Exception;

    /**
     * Called before an instance of the {@link ElasticActor} is loaded in to memory. This hook gives developers a chance to make changes to the
     * {@link ActorState} structure. The raw bytes are passed in as well as an instance of the {@link SerializationFramework}.
     * This framework is configured via {@link org.elasticsoftware.elasticactors.Actor#serializationFramework()}.<br><br>
     *
     * The versions are strings that passed in come from the {@link java.util.jar.Manifest} file, Implementation-Version attribute.
     * The version format is determined by the application and is set to UNKNOWN if it cannot be determined from the Manifest
     *
     * @param previousVersion           the previous version of the {@link ActorState}
     * @param currentVersion            the current version of the application
     * @param serializedForm            the serialized form of the actor's state object
     * @param serializationFramework    the serialization framework used to deserialize this actor's state
     * @return                          the {@link ActorState} object after being processed
     * @throws Exception                when something unexpected happens
     */
    @SuppressWarnings("rawtypes")
    @Nonnull
    ActorState preActivate(String previousVersion,String currentVersion,byte[] serializedForm,SerializationFramework serializationFramework) throws Exception;

    /**
     * Called after an instance of the {@link ElasticActor} was loaded into memory. Will be called every time the
     * actor was deserialized from the Persistent Store. Depending on the memory pressure this can happen multiple times
     * within the lifetime of the JVM but will happen at least once (unless the {@link ElasticActor} gets no messages).
     * Access to the {@link ActorContext} can be obtained by using the {@link ActorContextHolder} methods
     *
     * @param previousVersion   the previous version of this {@link ElasticActor} instance. Can be used to determine whether the state needs to be transformed between versions
     * @throws Exception    when something unexpected happens
     * @see                 org.elasticsoftware.elasticactors.ActorContextHolder#getSelf()
     * @see                 ActorContextHolder#getState(Class)
     * @see                 org.elasticsoftware.elasticactors.ActorContextHolder#getSystem()
     */
    void postActivate(@Nullable String previousVersion) throws Exception;

    /**
     * Receive a Message. The message object will be annotated with {@link org.elasticsoftware.elasticactors.serialization.Message}.
     * The sender of the message will also be passed in. This is the {@link ActorRef} parameter set in {@link ActorRef#tell(Object, ActorRef)}
     * Access to the {@link ActorContext} can be obtained by using the {@link ActorContextHolder} methods
     *
     * @param sender        the sender of the message (as passed in {@link ActorRef#tell(Object, ActorRef)})
     * @param message       the message object
     * @throws Exception    when something unexpected happens
     * @see                 org.elasticsoftware.elasticactors.ActorContextHolder#getSelf()
     * @see                 ActorContextHolder#getState(Class)
     * @see                 org.elasticsoftware.elasticactors.ActorContextHolder#getSystem()
     */
    void onReceive(ActorRef sender, T message) throws Exception;

    /**
     * Called when a message that was originally send by this Actor could not be delivered to the intended
     * recipient. Mind you that it should not have necessarily need this Actor instance that send that message as
     * it could have also been another actor (or code) that set this Actor's {@link ActorRef} in {@link ActorRef#tell(Object, ActorRef)})
     * Access to the {@link ActorContext} can be obtained by using the {@link ActorContextHolder} methods
     *
     * @param receiver      the {@link ActorRef} the message was originally sent to
     * @param message       the original message
     * @throws Exception    when something unexpected happens
     * @see                 org.elasticsoftware.elasticactors.ActorContextHolder#getSelf()
     * @see                 ActorContextHolder#getState(Class)
     * @see                 org.elasticsoftware.elasticactors.ActorContextHolder#getSystem()
     */
    void onUndeliverable(ActorRef receiver, Object message) throws Exception;

    /**
     * Called after an instance of the {@link ElasticActor} was evicted from memory.Depending on the memory pressure this can happen multiple times
     * within the lifetime of the JVM but will happen at least once (unless the {@link ElasticActor} gets no messages).
     * Access to the {@link ActorContext} can be obtained by using the {@link ActorContextHolder} methods
     *
     * @throws Exception    when something unexpected happens
     * @see                 org.elasticsoftware.elasticactors.ActorContextHolder#getSelf()
     * @see                 ActorContextHolder#getState(Class)
     * @see                 org.elasticsoftware.elasticactors.ActorContextHolder#getSystem()
     */
    void prePassivate() throws Exception;

    /**
     * Called before the instance of the {@link ElasticActor} will be destroyed (by calling {@link ActorSystem#stop(ActorRef)}.
     * Will be called exactly once in the  lifecycle of an Actor.
     * Access to the {@link ActorContext} can be obtained by using the {@link ActorContextHolder} methods
     *
     * @param destroyer     the {@link ActorRef} of the actor that requested this actor's destruction, if any
     * @throws Exception    when something unexpected happens
     * @see                 org.elasticsoftware.elasticactors.ActorContextHolder#getSelf()
     * @see                 ActorContextHolder#getState(Class)
     * @see                 org.elasticsoftware.elasticactors.ActorContextHolder#getSystem()
     */
    void preDestroy(ActorRef destroyer) throws Exception;

    /**
     * This is a factory method that will create a {@link Subscriber} for a given messageClass. The {@link TypedActor}
     * implementation will return a {@link TypedActor.DefaultSubscriber} that will delegate the
     * {@link Subscriber#onNext(Object)} to {@link TypedActor#onReceive(ActorRef, Object)} so normal messaging
     * semantics can be observed. When custom handling is needed override this method and return an implementation
     * that extends the {@link TypedSubscriber} abstract class.<br><br>
     *
     * Similar to implementing an actor the {@link TypedSubscriber} should not have any state. Instead the
     * {@link TypedSubscriber#getState(Class)} should be used to access the {@link ActorState}<br><br>
     *
     * NOTE: calling this method will only work of the implementing class has the {@link Actor} annotation. Otherwise
     * this method will throw an {@link IllegalStateException}
     *
     * @param messageClass  the message class to create a subscriber for
     * @param <MT>          the message class to create a subscriber for
     * @return              A {@link Subscriber} implementation
     * @throws  IllegalStateException if the implementing class is not annotated with {@link Actor}
     */
    <MT> Subscriber<MT> asSubscriber(Class<MT> messageClass);
}
