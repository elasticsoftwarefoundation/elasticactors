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

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Collection;
import java.util.Map;
import java.util.Set;

/**
 * When an {@link org.elasticsoftware.elasticactors.state.ActorLifecycleStep} method is called or
 * when a {@link org.elasticsoftware.elasticactors.serialization.Message} is delivered to a
 * {@link ElasticActor} this is always done within the {@link ActorContext}. The context gives
 * access to some important variables such as {@link ActorRef} self, {@link ActorState} state,
 * and the {@link ActorSystem}.
 *
 * The {@link TypedActor} class provides some convenience methods to access the context by using
 * method calls. If you don't use this class as the base then {@link ActorContextHolder} needs to
 * be used.
 *
 * @author Joost van de Wijgerd
 * @see     ElasticActor#onReceive(ActorRef, Object)
 * @see     ElasticActor#onUndeliverable(ActorRef, Object)
 * @see     ElasticActor#postCreate(ActorRef)
 * @see     ElasticActor#postActivate(String)
 * @see     org.elasticsoftware.elasticactors.ElasticActor#prePassivate()
 * @see     ElasticActor#preDestroy(ActorRef)
 * @see     org.elasticsoftware.elasticactors.TypedActor#getSystem()
 * @see     TypedActor#getState(Class)
 * @see     org.elasticsoftware.elasticactors.TypedActor#getSelf()
 * @see     org.elasticsoftware.elasticactors.ActorContextHolder#getSystem()
 * @see     ActorContextHolder#getState(Class)
 * @see     org.elasticsoftware.elasticactors.ActorContextHolder#getSelf()
 */
public interface ActorContext {
    /**
     *
     * @return  the {@link ActorRef} instance that references the Actor
     */
    @Nonnull
    ActorRef getSelf();

    /**
     *
     * @return the current actor's type
     */
    @Nullable
    Class<?> getSelfType();

    /**
     *
     * @param stateClass    the class that implements {@link ActorState}
     * @param <T>           generic type info
     * @return              the {@link} ActorState instance for this Actor
     */
    <T extends ActorState> T getState(Class<T> stateClass);

    /**
     * Sets (overwrites!) the state. Use with care
     *
     * @param state     the new {@link ActorState} to set (and store)
     */
    @SuppressWarnings("rawtypes")
    void setState(@Nonnull ActorState state);

    /**
     *
     * @return  the {@link ActorSystem} this actor is part of
     */
    @Nonnull
    ActorSystem getActorSystem();

    /**
     * Provides access to the current list of subscriptions that this actor has. Use this to
     * select a {@link PersistentSubscription} and the call {@link PersistentSubscription#cancel()} in
     * order to cancel the subscription. Due to the asynchronous nature of ElasticActors the subscription
     * is not immediately removed. However the {@link PersistentSubscription#isCancelled()} is now true.
     *
     * From this moment onward the {@link PersistentSubscription#request(long)} and {@link PersistentSubscription#cancel()}
     * methods will be NOPs
     *
     * @return  the collection of {@link PersistentSubscription}s or and empty collection if there are none
     */
    @Nonnull
    Collection<PersistentSubscription> getSubscriptions();

    /**
     * Provide access to the current Actors that are subscribed to this actor. It is not possible to cancel the
     * subscribers. The subscribing actor will need to to call {@link PersistentSubscription#cancel()} in order
     *
     * @return  the current (Multi)Map of subscribed actors with as a key the {@link org.elasticsoftware.elasticactors.serialization.Message}
     *          (class)Name and as a value the Set of subscribed actors. If there are no subscribers and empty Map will
     *          be returned
     */
    @Nonnull
    Map<String, Set<ActorRef>> getSubscribers();
}
