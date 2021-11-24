/*
 *   Copyright 2013 - 2019 The Original Authors
 *
 *   Licensed under the Apache License, Version 2.0 (the "License");
 *   you may not use this file except in compliance with the License.
 *   You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 */

package org.elasticsoftware.elasticactors.serialization.internal;

import org.elasticsoftware.elasticactors.ActorState;
import org.elasticsoftware.elasticactors.ElasticActor;
import org.elasticsoftware.elasticactors.messaging.internal.ActorType;
import org.elasticsoftware.elasticactors.messaging.internal.CreateActorMessage;
import org.elasticsoftware.elasticactors.serialization.MessageDeserializer;
import org.elasticsoftware.elasticactors.serialization.SerializationFrameworks;
import org.elasticsoftware.elasticactors.serialization.protobuf.Messaging;
import org.elasticsoftware.elasticactors.util.ByteBufferUtils;
import org.elasticsoftware.elasticactors.util.SerializationTools;

import java.io.IOException;
import java.nio.ByteBuffer;

import static org.elasticsoftware.elasticactors.util.ClassLoadingHelper.getClassHelper;

/**
 * @author Joost van de Wijgerd
 */
public final class CreateActorMessageDeserializer implements MessageDeserializer<CreateActorMessage> {
    private final SerializationFrameworks serializationFrameworks;

    public CreateActorMessageDeserializer(SerializationFrameworks serializationFrameworks) {
        this.serializationFrameworks = serializationFrameworks;
    }

    @Override
    public CreateActorMessage deserialize(ByteBuffer serializedObject) throws IOException {
        Messaging.CreateActorMessage protobufMessage = ByteBufferUtils.throwingApplyAndReset(
            serializedObject,
            Messaging.CreateActorMessage::parseFrom
        );
        return new CreateActorMessage(
            protobufMessage.getActorSystem(),
            protobufMessage.getActorClass(),
            protobufMessage.getActorId(),
            getDeserializedState(protobufMessage),
            ActorType.values()[protobufMessage.getType().getNumber()],
            protobufMessage.getAffinityKey()
        );
    }

    @Override
    public boolean isSafe() {
        return true;
    }

    private ActorState getDeserializedState(Messaging.CreateActorMessage protobufMessage) throws IOException {
        return protobufMessage.hasInitialState() && !protobufMessage.getInitialState().isEmpty()
            ? getDeserializeState(protobufMessage)
            : null;
    }

    private ActorState getDeserializeState(Messaging.CreateActorMessage protobufMessage) throws IOException {
        return deserializeState(
            protobufMessage.getActorClass(),
            protobufMessage.getInitialState().asReadOnlyByteBuffer()
        );
    }

    private ActorState deserializeState(String actorClass, ByteBuffer serializedState) throws IOException {
        try {
            return SerializationTools.deserializeActorState(
                    serializationFrameworks,
                    (Class<? extends ElasticActor>) getClassHelper().forName(actorClass),
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
