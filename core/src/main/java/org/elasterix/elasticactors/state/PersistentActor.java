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

import org.elasterix.elasticactors.ActorRef;
import org.elasterix.elasticactors.ActorState;
import org.elasterix.elasticactors.ElasticActor;
import org.elasterix.elasticactors.ShardKey;
import org.elasterix.elasticactors.serialization.Deserializer;

import java.io.IOException;

/**
 * @author Joost van de Wijgerd
 */
public final class PersistentActor {
    private final ShardKey shardKey;
    private final String actorSystemVersion;
    private final Class<? extends ElasticActor> actorClass;
    private final ActorRef ref;
    private volatile byte[] serializedState;

    public PersistentActor(ShardKey shardKey,String actorSystemVersion, ActorRef ref, Class<? extends ElasticActor> actorClass) {
        this(shardKey,actorSystemVersion,ref,actorClass,null);
    }

    public PersistentActor(ShardKey shardKey, String actorSystemVersion, ActorRef ref, Class<? extends ElasticActor> actorClass, byte[] serializedState) {
        this.shardKey = shardKey;
        this.actorSystemVersion = actorSystemVersion;
        this.ref = ref;
        this.actorClass = actorClass;
        this.serializedState = serializedState;
    }

    public ShardKey getShardKey() {
        return shardKey;
    }

    public String getActorSystemVersion() {
        return actorSystemVersion;
    }

    public Class<? extends ElasticActor> getActorClass() {
        return actorClass;
    }

    public ActorRef getRef() {
        return ref;
    }

    public byte[] getSerializedState() {
        return serializedState;
    }

    public void setSerializedState(byte[] serializedState) {
        this.serializedState = serializedState;
    }

    public ActorState getState(Deserializer<byte[],ActorState> stateDeserializer) throws IOException {
        if(serializedState == null) {
            return null;
        } else {
            return stateDeserializer.deserialize(this.serializedState);
        }
    }
}
