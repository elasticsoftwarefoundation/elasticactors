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

package org.elasticsoftware.elasticactors.serialization.internal;

import com.google.protobuf.ByteString;
import org.elasticsoftware.elasticactors.ActorState;
import org.elasticsoftware.elasticactors.ElasticActor;
import org.elasticsoftware.elasticactors.cluster.InternalActorSystems;
import org.elasticsoftware.elasticactors.messaging.internal.ActorType;
import org.elasticsoftware.elasticactors.messaging.internal.CreateActorMessage;
import org.elasticsoftware.elasticactors.serialization.MessageDeserializer;
import org.elasticsoftware.elasticactors.serialization.protobuf.Elasticactors;
import org.elasticsoftware.elasticactors.util.SerializationTools;

import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * @author Joost van de Wijgerd
 */
public final class CreateActorMessageDeserializer implements MessageDeserializer<CreateActorMessage> {
    private final InternalActorSystems actorSystems;

    public CreateActorMessageDeserializer(InternalActorSystems actorSystems) {
        this.actorSystems = actorSystems;
    }

    @Override
    public CreateActorMessage deserialize(ByteBuffer serializedObject) throws IOException {
        Elasticactors.CreateActorMessage protobufMessage = Elasticactors.CreateActorMessage.parseFrom(ByteString.copyFrom(serializedObject));
        return new CreateActorMessage(protobufMessage.getActorSystem(),
                                      protobufMessage.getActorClass(),
                                      protobufMessage.getActorId(),
                                      protobufMessage.hasInitialState()
                                              ? deserializeState(protobufMessage.getActorClass(),
                                                                 protobufMessage.getInitialState().toByteArray())
                                              : null,
                                      protobufMessage.hasType()
                                              ? ActorType.values()[protobufMessage.getType().getNumber()]
                                              : ActorType.PERSISTENT);
    }

    private ActorState deserializeState(String actorClass,byte[] serializedState) throws IOException {
        try {
            return SerializationTools.deserializeActorState(actorSystems,
                                                    (Class<? extends ElasticActor>) Class.forName(actorClass),
                                                    serializedState);
        } catch (ClassNotFoundException e) {
            throw new IOException(e);
        }
    }

    @Override
    public Class<CreateActorMessage> getMessageClass() {
        return CreateActorMessage.class;
    }
}
