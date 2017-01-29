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

import com.google.common.collect.ImmutableList;
import com.google.protobuf.ByteString;
import org.elasticsoftware.elasticactors.ActorRef;
import org.elasticsoftware.elasticactors.cluster.InternalActorSystem;
import org.elasticsoftware.elasticactors.cluster.InternalActorSystems;
import org.elasticsoftware.elasticactors.messaging.ImmutableInternalMessage;
import org.elasticsoftware.elasticactors.messaging.InternalMessage;
import org.elasticsoftware.elasticactors.messaging.InternalMessageImpl;
import org.elasticsoftware.elasticactors.messaging.internal.ActorNodeMessage;
import org.elasticsoftware.elasticactors.serialization.Deserializer;
import org.elasticsoftware.elasticactors.serialization.Message;
import org.elasticsoftware.elasticactors.serialization.MessageDeserializer;
import org.elasticsoftware.elasticactors.serialization.protobuf.Elasticactors;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.UUID;

import static org.elasticsoftware.elasticactors.messaging.UUIDTools.toUUID;

/**
 * @author Joost van de Wijgerd
 */
public final class ActorNodeMessageDeserializer implements MessageDeserializer<ActorNodeMessage> {
    private final ActorRefDeserializer actorRefDeserializer;
    private final InternalActorSystems cluster;

    public ActorNodeMessageDeserializer(ActorRefDeserializer actorRefDeserializer, InternalActorSystems cluster) {
        this.actorRefDeserializer = actorRefDeserializer;
        this.cluster = cluster;
    }

    @Override
    public ActorNodeMessage deserialize(ByteBuffer serializedObject) throws IOException {
        try {
            Elasticactors.ActorNodeMessage protobufMessage = Elasticactors.ActorNodeMessage.parseFrom(ByteString.copyFrom(serializedObject));
            ActorRef receiverRef = (protobufMessage.hasReceiver()) ? actorRefDeserializer.deserialize(protobufMessage.getReceiver()) : null;
            String messageClassString = protobufMessage.getPayloadClass();
            Class<?> messageClass = Class.forName(messageClassString);
            Object payloadObject = cluster.get(null).getDeserializer(messageClass).deserialize(protobufMessage.getPayload().asReadOnlyByteBuffer());
            return new ActorNodeMessage(protobufMessage.getNodeId(), receiverRef, payloadObject, protobufMessage.getUndeliverable());
        } catch(Exception e) {
            throw new IOException(e);
        }
    }

    @Override
    public Class<ActorNodeMessage> getMessageClass() {
        return ActorNodeMessage.class;
    }
}
