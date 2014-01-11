/*
 * Copyright 2013 - 2014 The Original Authors
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

/**
 * @author Joost van de Wijgerd
 */
public final class PersistentActor<K> implements ActorContext {
    private final K key;
    private final InternalActorSystem actorSystem;
    private final String previousActorSystemVersion;
    private final Class<? extends ElasticActor> actorClass;
    private final ActorRef ref;
    private volatile ActorState actorState;

    public PersistentActor(K key,InternalActorSystem actorSystem,String previousActorSystemVersion, ActorRef ref, Class<? extends ElasticActor> actorClass) {
        this(key,actorSystem,previousActorSystemVersion,ref,actorClass,null);
    }

    public PersistentActor(K key, InternalActorSystem actorSystem,String previousActorSystemVersion, ActorRef ref, Class<? extends ElasticActor> actorClass, ActorState state) {
        this.key = key;
        this.actorSystem = actorSystem;
        this.ref = ref;
        this.actorClass = actorClass;
        this.actorState = state;
        this.previousActorSystemVersion = previousActorSystemVersion;
    }

    public K getKey() {
        return key;
    }

    public String getPreviousActorSystemVersion() {
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

    @Override
    public void setState(ActorState state) {
        this.actorState = state;
    }

    @Override
    public ActorSystem getActorSystem() {
        return actorSystem;
    }
}
