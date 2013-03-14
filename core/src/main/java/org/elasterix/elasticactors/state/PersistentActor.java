/*
 * Copyright (c) 2013 Joost van de Wijgerd <jwijgerd@gmail.com>
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *  	http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.elasterix.elasticactors.state;

import org.elasterix.elasticactors.*;
import org.elasterix.elasticactors.cluster.InternalActorSystem;
import org.elasterix.elasticactors.serialization.Deserializer;

import java.io.IOException;

/**
 * @author Joost van de Wijgerd
 */
public final class PersistentActor implements ActorContext {
    private final ShardKey shardKey;
    private final InternalActorSystem actorSystem;
    private final String previousActorSystemVersion;
    private final Class<? extends ElasticActor> actorClass;
    private final ActorRef ref;
    private volatile ActorState serializedState;

    public PersistentActor(ShardKey shardKey,InternalActorSystem actorSystem,String previousActorSystemVersion, ActorRef ref, Class<? extends ElasticActor> actorClass) {
        this(shardKey,actorSystem,previousActorSystemVersion,ref,actorClass,null);
    }

    public PersistentActor(ShardKey shardKey, InternalActorSystem actorSystem,String previousActorSystemVersion, ActorRef ref, Class<? extends ElasticActor> actorClass, ActorState state) {
        this.shardKey = shardKey;
        this.actorSystem = actorSystem;
        this.ref = ref;
        this.actorClass = actorClass;
        this.serializedState = state;
        this.previousActorSystemVersion = previousActorSystemVersion;
    }

    public ShardKey getShardKey() {
        return shardKey;
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
    public ActorState getState() {
        return serializedState;
    }

    @Override
    public void setState(ActorState state) {
        this.serializedState = state;
    }

    @Override
    public ActorSystem getActorSystem() {
        return actorSystem;
    }
}
