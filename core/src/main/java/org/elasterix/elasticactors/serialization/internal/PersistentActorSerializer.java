/*
 * Copyright 2013 Joost van de Wijgerd
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

package org.elasterix.elasticactors.serialization.internal;

import com.google.protobuf.ByteString;
import org.elasterix.elasticactors.cluster.InternalActorSystem;
import org.elasterix.elasticactors.cluster.InternalActorSystems;
import org.elasterix.elasticactors.serialization.Serializer;
import org.elasterix.elasticactors.serialization.protobuf.Elasticactors;
import org.elasterix.elasticactors.state.PersistentActor;
import org.springframework.beans.factory.annotation.Autowired;

import java.io.IOException;

/**
 * @author Joost van de Wijgerd
 */
public final class PersistentActorSerializer implements Serializer<PersistentActor,byte[]> {
    private static final PersistentActorSerializer INSTANCE = new PersistentActorSerializer();
    private InternalActorSystems actorSystems;

    public static PersistentActorSerializer get() {
        return INSTANCE;
    }

    @Override
    public byte[] serialize(PersistentActor persistentActor) throws IOException {
        Elasticactors.PersistentActor.Builder builder = Elasticactors.PersistentActor.newBuilder();
        builder.setShardKey(persistentActor.getShardKey().toString());
        builder.setActorSystemVersion(persistentActor.getActorSystemVersion());
        builder.setActorClass(persistentActor.getActorClass().getName());
        builder.setActorRef(persistentActor.getSelf().toString());

        if (persistentActor.getState() != null) {
            builder.setState(ByteString.copyFrom(getSerializedState(persistentActor)));
        }
        return builder.build().toByteArray();
    }

    private byte[] getSerializedState(PersistentActor persistentActor) throws IOException {
        InternalActorSystem actorSystem = actorSystems.get(persistentActor.getShardKey().getActorSystemName());
        return actorSystem.getActorStateSerializer().serialize(persistentActor.getState());
    }

    @Autowired
    public void setActorSystems(InternalActorSystems actorSystems) {
        this.actorSystems = actorSystems;
    }
}
