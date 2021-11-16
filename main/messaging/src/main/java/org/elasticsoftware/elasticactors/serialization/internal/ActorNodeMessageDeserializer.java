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

import org.elasticsoftware.elasticactors.ActorRef;
import org.elasticsoftware.elasticactors.cluster.InternalActorSystems;
import org.elasticsoftware.elasticactors.messaging.internal.ActorNodeMessage;
import org.elasticsoftware.elasticactors.serialization.MessageDeserializer;
import org.elasticsoftware.elasticactors.serialization.protobuf.Messaging;

import java.io.IOException;
import java.nio.ByteBuffer;

import static org.elasticsoftware.elasticactors.util.ClassLoadingHelper.getClassHelper;

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
            // Using duplicate instead of asReadOnlyBuffer so implementations can optimize this in case
            // the original byte buffer has an array
            Messaging.ActorNodeMessage protobufMessage = Messaging.ActorNodeMessage.parseFrom(serializedObject.duplicate());
            ActorRef receiverRef = getReceiver(protobufMessage);
            String messageClassString = protobufMessage.getPayloadClass();
            Class<?> messageClass = getClassHelper().forName(messageClassString);
            Object payloadObject = cluster.get(null).getDeserializer(messageClass).deserialize(protobufMessage.getPayload().asReadOnlyByteBuffer());
            return new ActorNodeMessage(protobufMessage.getNodeId(), receiverRef, payloadObject, protobufMessage.getUndeliverable());
        } catch(Exception e) {
            throw new IOException(e);
        }
    }

    private ActorRef getReceiver(Messaging.ActorNodeMessage protobufMessage) throws IOException {
        String receiver = protobufMessage.getReceiver();
        return receiver.isEmpty() ? null : actorRefDeserializer.deserialize(receiver);
    }

    @Override
    public Class<ActorNodeMessage> getMessageClass() {
        return ActorNodeMessage.class;
    }
}
