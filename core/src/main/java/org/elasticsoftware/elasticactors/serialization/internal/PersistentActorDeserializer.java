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

package org.elasticsoftware.elasticactors.serialization.internal;

import org.elasticsoftware.elasticactors.ActorState;
import org.elasticsoftware.elasticactors.ElasticActor;
import org.elasticsoftware.elasticactors.ShardKey;
import org.elasticsoftware.elasticactors.cluster.ActorRefFactory;
import org.elasticsoftware.elasticactors.cluster.InternalActorSystems;
import org.elasticsoftware.elasticactors.serialization.Deserializer;
import org.elasticsoftware.elasticactors.serialization.protobuf.Elasticactors;
import org.elasticsoftware.elasticactors.state.PersistentActor;
import org.elasticsoftware.elasticactors.util.ManifestTools;
import org.elasticsoftware.elasticactors.util.SerializationTools;
import org.springframework.beans.factory.annotation.Configurable;

import java.io.IOException;

/**
 * @author Joost van de Wijgerd
 */
@Configurable
public final class PersistentActorDeserializer implements Deserializer<byte[],PersistentActor<ShardKey>> {
    private final ActorRefFactory actorRefFactory;
    private final InternalActorSystems actorSystems;

    public PersistentActorDeserializer(ActorRefFactory actorRefFactory, InternalActorSystems actorSystems) {
        this.actorRefFactory = actorRefFactory;
        this.actorSystems = actorSystems;
    }

    @Override
    public PersistentActor<ShardKey> deserialize(byte[] serializedObject) throws IOException {
        Elasticactors.PersistentActor protobufMessage = Elasticactors.PersistentActor.parseFrom(serializedObject);
        final ShardKey shardKey = ShardKey.fromString(protobufMessage.getShardKey());
        try {
            Class<? extends ElasticActor> actorClass = (Class<? extends ElasticActor>) Class.forName(protobufMessage.getActorClass());
            // @todo: this needs to be cached somewhere
            final String currentActorStateVersion = ManifestTools.extractActorStateVersion(actorClass);

            return new PersistentActor<>(shardKey,
                                         actorSystems.get(shardKey.getActorSystemName()),
                                         currentActorStateVersion,
                                         protobufMessage.getActorSystemVersion(),
                                         actorRefFactory.create(protobufMessage.getActorRef()),
                                         actorClass,
                                         protobufMessage.hasState() ? protobufMessage.getState().toByteArray() : null);
        } catch(ClassNotFoundException e) {
            throw new IOException("Exception deserializing PersistentActor",e);
        }
    }

    private ActorState deserializeState(Class<? extends ElasticActor> actorClass, byte[] serializedState) throws IOException {
        return SerializationTools.deserializeActorState(actorSystems, actorClass, serializedState);
    }


}
