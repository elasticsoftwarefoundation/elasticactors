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
import org.elasterix.elasticactors.ActorState;
import org.elasterix.elasticactors.cluster.InternalActorSystem;
import org.elasterix.elasticactors.cluster.InternalActorSystems;
import org.elasterix.elasticactors.messaging.internal.CreateActorMessage;
import org.elasterix.elasticactors.serialization.MessageDeserializer;
import org.elasterix.elasticactors.serialization.protobuf.Elasticactors;

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
                                              ? deserializeState(protobufMessage.getActorSystem(),
                                                                 protobufMessage.getInitialState().toByteArray())
                                              : null,
                                      protobufMessage.hasType()
                                              ? CreateActorMessage.ActorType.values()[protobufMessage.getType().getNumber()]
                                              : CreateActorMessage.ActorType.PERSISTENT);
    }

    private ActorState deserializeState(String actorSystemName,byte[] serializedState) throws IOException {
        InternalActorSystem actorSystem = actorSystems.get(actorSystemName);
        return actorSystem.getActorStateDeserializer().deserialize(serializedState);
    }
}
