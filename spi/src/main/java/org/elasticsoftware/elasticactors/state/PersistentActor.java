/*
 * Copyright 2013 - 2017 The Original Authors
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

package org.elasticsoftware.elasticactors.state;

import org.elasticsoftware.elasticactors.*;
import org.elasticsoftware.elasticactors.cluster.InternalActorSystem;

import java.util.Collection;
import java.util.Collections;

/**
 * @author Joost van de Wijgerd
 */
public final class PersistentActor<K> implements ActorContext {
    private final K key;
    private final InternalActorSystem actorSystem;
    private transient final String currentActorStateVersion;
    private final String previousActorSystemVersion;
    private final Class<? extends ElasticActor> actorClass;
    private final ActorRef ref;
    private transient volatile byte[] serializedState;
    private volatile ActorState actorState;

    /**
     * This Constructor should be used when creating a new PersistentActor in memory
     *
     * @param key
     * @param actorSystem
     * @param previousActorStateVersion
     * @param ref
     * @param actorClass
     * @param actorState
     */
    public PersistentActor(K key, InternalActorSystem actorSystem, String previousActorStateVersion, ActorRef ref, Class<? extends ElasticActor> actorClass,  ActorState actorState) {
        this.key = key;
        this.actorSystem = actorSystem;
        this.currentActorStateVersion = previousActorStateVersion;
        this.previousActorSystemVersion = previousActorStateVersion;
        this.actorClass = actorClass;
        this.ref = ref;
        this.actorState = actorState;
        this.serializedState = null;
    }

    /**
     * This Constructor should be used when the PersistentActor is deserialized
     *
     * @param key
     * @param actorSystem
     * @param currentActorStateVersion
     * @param previousActorSystemVersion
     * @param ref
     * @param actorClass
     * @param serializedState
     */
    public PersistentActor(K key, InternalActorSystem actorSystem,String currentActorStateVersion, String previousActorSystemVersion, ActorRef ref, Class<? extends ElasticActor> actorClass, byte[] serializedState) {
        this.key = key;
        this.actorSystem = actorSystem;
        this.ref = ref;
        this.actorClass = actorClass;
        this.serializedState = serializedState;
        this.actorState = null;
        this.currentActorStateVersion = currentActorStateVersion;
        this.previousActorSystemVersion = previousActorSystemVersion;
    }

    public K getKey() {
        return key;
    }

    public String getCurrentActorStateVersion() {
        return currentActorStateVersion;
    }

    public String getPreviousActorStateVersion() {
        return previousActorSystemVersion;
    }

    public Class<? extends ElasticActor> getActorClass() {
        return actorClass;
    }

    @Override
    public ActorRef getSelf() {
        return ref;
    }

    @Override
    public <T extends ActorState> T getState(Class<T> stateClass) {
        return stateClass.cast(actorState);
    }

    public ActorState getState() {
        return actorState;
    }

    public byte[] getSerializedState() {
        return serializedState;
    }

    public void setSerializedState(byte[] serializedState) {
        this.serializedState = serializedState;
    }

    @Override
    public void setState(ActorState state) {
        this.actorState = state;
    }

    @Override
    public ActorSystem getActorSystem() {
        return actorSystem;
    }

    @Override
    public Collection<? extends PersistentSubscription> getSubscriptions() {
        return Collections.emptySet();
    }
}
