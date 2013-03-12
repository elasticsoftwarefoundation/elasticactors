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

import org.elasterix.elasticactors.ElasticActor;
import org.elasterix.elasticactors.ShardKey;
import org.elasterix.elasticactors.cluster.ActorRefFactory;
import org.elasterix.elasticactors.serialization.Deserializer;
import org.elasterix.elasticactors.serialization.protobuf.Elasticactors;
import org.elasterix.elasticactors.state.PersistentActor;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Configurable;

import java.io.IOException;

/**
 * @author Joost van de Wijgerd
 */
@Configurable
public final class PersistentActorDeserializer implements Deserializer<byte[],PersistentActor> {
    private ActorRefFactory actorRefFactory;

    @Autowired
    public void setActorRefFactory(ActorRefFactory actorRefFactory) {
        this.actorRefFactory = actorRefFactory;
    }

    @Override
    public PersistentActor deserialize(byte[] serializedObject) throws IOException {
        Elasticactors.PersistentActor protobufMessage = Elasticactors.PersistentActor.parseFrom(serializedObject);
        byte[] serializedState = protobufMessage.hasState() ? protobufMessage.getState().toByteArray(): null;
        try {
            return new PersistentActor(ShardKey.fromString(protobufMessage.getShardKey()),
                                       protobufMessage.getActorSystemVersion(),
                                       actorRefFactory.create(protobufMessage.getActorRef()),
                                       (Class<? extends ElasticActor>) Class.forName(protobufMessage.getActorClass()),
                                       serializedState);
        } catch(Exception e) {
            throw new IOException("Exception deserializing PersistentActor",e);
        }
    }
}
