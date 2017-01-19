/*
 * Copyright 2013 - 2016 The Original Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.elasticsoftware.elasticactors;

import org.elasticsoftware.elasticactors.cluster.ActorSystemEventListenerRegistry;
import org.elasticsoftware.elasticactors.scheduler.Scheduler;

import javax.annotation.Nullable;
import java.util.Collection;

/**
 * An {@link ActorSystem} is a collection of {@link ElasticActor} instances. {@link ElasticActor}s are persistent and
 * have an {@link ActorState}. An application implementing and {@link ActorSystem} should create classes that implement
 * the {@link ElasticActor#onReceive(ActorRef, Object)} method. Within this method the associated {@link ActorState} can
 * be obtained by calling the {@link ActorContextHolder#getState(Class)} method.
 * The ElasticActors framework will take care of persisting the state. The application can control the serialization and
 * deserialization by providing appropriate {@link org.elasticsoftware.elasticactors.serialization.SerializationFramework}
 * in the {@link Actor} annotation
 *
 * @author Joost van de Wijgerd
 */
public interface ActorSystem {
    /**
     * The name of this {@link ActorSystem}. The name is unique within the same cluster
     *
     * @return
     */
    String getName();

    /**
     * Create a new {@link ElasticActor} of the given type. There will be no state passed (null). This is
     * an asynchronous method that could potentially fail. However the {@link ActorRef} is fully functional
     * and can be used to send messages immediately. All messages are handled on the same thread and will be
     * executed in order from the perspective of the receiving {@link ElasticActor}<br/>
     * This is a idempotent method, when an actor with the same actorId already exists, the method will silently
     * succeed. However the {@link ActorState} will NOT be overwritten.
     *
     * @param actorId       the actorId of the actor to create
     * @param actorClass    the type class of the actor. Needs to be annotated with {@link Actor}
     * @param <T>           generic type info
     * @return              the {@link ActorRef} pointing to the newly created actor
     * @throws Exception    when something unexpected happens
     */
    <T> ActorRef actorOf(String actorId, Class<T> actorClass) throws Exception;


    /**
     * Create a new {@link ElasticActor} of the given type. There will be no state passed (null). This is
     * an asynchronous method that could potentially fail. However the {@link ActorRef} is fully functional
     * and can be used to send messages immediately. All messages are handled on the same thread and will be
     * executed in order from the perspective of the receiving {@link ElasticActor}<br/>
     * This is a idempotent method, when an actor with the same actorId already exists, the method will silently
     * succeed. However the {@link ActorState} will NOT be overwritten.
     *
     * This method takes the name of the actor class as a parameter. This is mainly handy in the case where actors need
     * to be created on a remote ActorSystem. There is no strong type checking on the actor class and also no exception
     * will be thrown if the class doesn't exist on the remote end.
     *
     * When you are dealing with local actors, always prefer the stronger typed version of this method
     *
     * @param actorId           the actorId of the actor to create
     * @param actorClassName    the type class name of the actor. Needs to be annotated with {@link Actor}
     * @param <T>               generic type info
     * @return                  the {@link ActorRef} pointing to the newly created actor
     * @throws Exception        when something unexpected happens
     */
    ActorRef actorOf(String actorId, String actorClassName) throws Exception;

    /**
     * Create a new {@link ElasticActor} of the given type with the given initial {@link ActorState}. This is
     * an asynchronous method that could potentially fail. However the {@link ActorRef} is fully functional
     * and can be used to send messages immediately. All messages are handled on the same thread and will be
     * executed in order from the perspective of the receiving {@link ElasticActor}<br/>
     * This is a idempotent method, when an actor with the same actorId already exists, the method will silently
     * succeed. However the {@link ActorState} will NOT be overwritten.
     *
     * @param actorId       the actorId of the actor to create
     * @param actorClass    the type class of the actor. Needs to be annotated with {@link Actor}
     * @param initialState  the initial state, should be of type {@link org.elasticsoftware.elasticactors.Actor#stateClass()}
     * @param <T>           generic type info
     * @return              the {@link ActorRef} pointing to the newly created actor
     * @throws Exception    when something unexpected happens
     */
    <T> ActorRef actorOf(String actorId, Class<T> actorClass, ActorState initialState) throws Exception;

    /**
     * Create a new {@link ElasticActor} of the given type with the given initial {@link ActorState}. This is
     * an asynchronous method that could potentially fail. However the {@link ActorRef} is fully functional
     * and can be used to send messages immediately. All messages are handled on the same thread and will be
     * executed in order from the perspective of the receiving {@link ElasticActor}<br/>
     * This is a idempotent method, when an actor with the same actorId already exists, the method will silently
     * succeed. However the {@link ActorState} will NOT be overwritten.
     *
     * This method takes the name of the actor class as a parameter. This is mainly handy in the case where actors need
     * to be created on a remote ActorSystem. There is no strong type checking on the actor class and also no exception
     * will be thrown if the class doesn't exist on the remote end.
     *
     * When you are dealing with local actors, always prefer the stronger typed version of this method
     *
     * @param actorId           the actorId of the actor to create
     * @param actorClassName    the type class name of the actor. Needs to be annotated with {@link Actor}
     * @param initialState      the initial state, should be of type {@link org.elasticsoftware.elasticactors.Actor#stateClass()}
     * @param <T>               generic type info
     * @return                  the {@link ActorRef} pointing to the newly created actor
     * @throws Exception        when something unexpected happens
     */
    ActorRef actorOf(String actorId, String actorClassName, ActorState initialState) throws Exception;

    /**
     * Create a Temporary Actor with the given initial {@link ActorState}. A Temp Actor is an {@link ElasticActor}
     * instance that is annotated with {@link TempActor}. Temporary Actors are located on the Local ElasticActors
     * node only and will not survive a restart of the Node. Temporary Actors are most commonly used to bridge between
     * two systems, for instance within a Controller. It will implement a Reply Address.<br/>
     * A Temporary Actor will get and actorId assigned by the Runtime. The resulting {@link ActorRef} can be stored however
     * due to the ephemeral nature of the Temporary Actor this is not advisable.
     *
     * @param actorClass    the type class of the actor. Needs to be annotated with {@link TempActor}
     * @param initialState  the initial state, should be of type {@link org.elasticsoftware.elasticactors.TempActor#stateClass()}
     * @param <T>           generic type info
     * @throws Exception    when something unexpected happens
     * @return              the {@link ActorRef} pointing to the newly created actor
     */
    <T> ActorRef tempActorOf(Class<T> actorClass,@Nullable ActorState initialState) throws Exception;

    /**
     * Return an {@link ActorRef} to a (Standard) Actor. There is no guarantee that the Actor actually exists. If you need
     * to be sure that the Actor exists call: {@link ActorSystem#actorOf(String, Class, ActorState)}. This is an idempotent
     * operation that won't overwrite the {@link ActorState} if it already exists so it's safe to use in all cases. However
     * there is significant overhead to this so if you are sure the the Actor exists then it's better to use this method.<br/>
     * This can be used only to get an {@link ActorRef} to a (Standard) {@link ElasticActor}.
     * For Service Actors use {@link ActorSystem#serviceActorFor(String)}
     *
     * @param actorId   the id of the (Standard) Actor to reference
     * @return          the {@link ActorRef} to the Actor (not guaranteed to exist!)
     */
    ActorRef actorFor(String actorId);

    /**
     * Return an {@link ActorRef} to a Service Actor. There is no guarantee that the Service exists. However if you have
     * annotated an {@link ElasticActor} class with the {@link ServiceActor} annotation than the Runtime will ensure that
     * the Service Actor was created (also need to ensure your jar has the META-INF/elasticactors.properties).
     *
     * @param actorId   the id of the Service Actor to reference
     * @return          the {@link ActorRef} to the Service Actor
     */
    ActorRef serviceActorFor(String actorId);

    /**
     * Return an {@link ActorRef} to a Service Actor on a(nother) node in the cluster.
     * There is no guarantee that the Service exists. However if you have
     * annotated an {@link ElasticActor} class with the {@link ServiceActor} annotation than the Runtime will ensure that
     * the Service Actor was created<br/>
     * This method should normally not be used unless you know the id's of the nodes in the cluster.
     *
     * @param nodeId
     * @param actorId
     * @return
     */
    ActorRef serviceActorFor(String nodeId, String actorId);

    /**
     * Return the {@link Scheduler} for this {@link ActorSystem}. The scheduler can be used to schedule messages to be sent
     * at a later point in time.
     *
     * @return      the configured {@link Scheduler}
     */
    Scheduler getScheduler();

    /**
     * Return the parent of this {@link ActorSystem}. Can be used to get access to {@link ActorSystem} instances on remote
     * ElasticActor clusters
     *
     * @return
     */
    ActorSystems getParent();

    /**
     * Stop (destroy) the Actor that behind the {@link ActorRef}. Cannot be used with Service Actors. This is a
     * irreversible operation. The state will be destroyed and the {@link org.elasticsoftware.elasticactors.ActorRef#getActorId()}
     * will be deleted from the registry.
     *
     * @param actorRef          the {@link ActorRef} to destroy
     * @throws Exception        when something unexpected happens
     */
    void stop(ActorRef actorRef) throws Exception;

    /**
     * Gives access to the configuration object
     *
     * @return      the configuration object
     */
    ActorSystemConfiguration getConfiguration();

    /**
     * The {@link ActorSystemEventListenerRegistry} can be used to register messages that will be sent when an event
     * (as defined in {@link org.elasticsoftware.elasticactors.cluster.ActorSystemEvent} enum is triggered.
     *
     * The most interesting use case is to ensure that a certain Actor will always be activated when it's shard gets
     * initialized (by subscribing to the {@link org.elasticsoftware.elasticactors.cluster.ActorSystemEvent#ACTOR_SHARD_INITIALIZED}
     * event).
     *
     * @return  the {@link org.elasticsoftware.elasticactors.cluster.ActorSystemEventListenerRegistry} for this {@link org.elasticsoftware.elasticactors.ActorSystem}
     */
    ActorSystemEventListenerRegistry getEventListenerRegistry();

    /**
     * Creates a group of {@link org.elasticsoftware.elasticactors.ActorRef} instances that will all receive the
     * same message. Can be used to efficiently broadcast messages to large groups of Actors.
     *
     * Cannot be used to group {@link org.elasticsoftware.elasticactors.TempActor}s or {@link org.elasticsoftware.elasticactors.ServiceActor}s
     *
     * @param members   The members that compose this group
     *
     * @return
     * @throws IllegalArgumentException if one of the members is not a Persistent Actor
     * (i.e. not created with {@link #actorOf(String, Class, ActorState)}, {@link #actorOf(String, Class)} or {@link #actorFor(String)}   }
     */
    ActorRefGroup groupOf(Collection<ActorRef> members) throws IllegalArgumentException;

}
