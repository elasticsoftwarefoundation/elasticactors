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

import org.elasticsoftware.elasticactors.ActorRef;
import org.elasticsoftware.elasticactors.cluster.scheduler.ScheduledMessage;
import org.elasticsoftware.elasticactors.cluster.scheduler.ScheduledMessageImpl;
import org.elasticsoftware.elasticactors.messaging.UUIDTools;
import org.elasticsoftware.elasticactors.serialization.Deserializer;
import org.elasticsoftware.elasticactors.serialization.protobuf.Elasticactors;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.UUID;

/**
 * @author Joost van de Wijgerd
 */
public final class ScheduledMessageDeserializer implements Deserializer<byte[],ScheduledMessage> {
    private final ActorRefDeserializer actorRefDeserializer;

    public ScheduledMessageDeserializer(ActorRefDeserializer actorRefDeserializer) {
        this.actorRefDeserializer = actorRefDeserializer;
    }

    @Override
    public ScheduledMessage deserialize(byte[] serializedObject) throws IOException {
        try {
            Elasticactors.ScheduledMessage protobufMessage = Elasticactors.ScheduledMessage.parseFrom(serializedObject);
            ActorRef sender = (protobufMessage.hasSender()) ? actorRefDeserializer.deserialize(protobufMessage.getSender()) : null;
            ActorRef receiver = actorRefDeserializer.deserialize(protobufMessage.getReceiver());
            Class messageClass = Class.forName(protobufMessage.getMessageClass());
            byte[] messageBytes = protobufMessage.getMessage().toByteArray();
            UUID id = UUIDTools.toUUID(protobufMessage.getId().toByteArray());
            long fireTime = protobufMessage.getFireTime();
            return new ScheduledMessageImpl(id,fireTime,sender,receiver,messageClass,messageBytes);
        } catch(ClassNotFoundException e) {
            throw new IOException(e);
        }
    }
}
