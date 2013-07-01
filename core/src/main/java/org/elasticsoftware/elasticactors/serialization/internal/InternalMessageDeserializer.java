/*
 * Copyright 2013 the original authors
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
import org.elasticsoftware.elasticactors.messaging.InternalMessage;
import org.elasticsoftware.elasticactors.messaging.InternalMessageImpl;
import org.elasticsoftware.elasticactors.messaging.UUIDTools;
import org.elasticsoftware.elasticactors.serialization.Deserializer;
import org.elasticsoftware.elasticactors.serialization.protobuf.Elasticactors;

import java.io.IOException;
import java.util.UUID;

/**
 * @author Joost van de Wijgerd
 */
public final class InternalMessageDeserializer implements Deserializer<byte[],InternalMessage> {
    private static final InternalMessageDeserializer INSTANCE = new InternalMessageDeserializer();

    public static InternalMessageDeserializer get() {
        return INSTANCE;
    }

    @Override
    public InternalMessage deserialize(byte[] serializedObject) throws IOException {
        Elasticactors.InternalMessage protobufMessage = Elasticactors.InternalMessage.parseFrom(serializedObject);
        ActorRef sender = (protobufMessage.hasSender()) ? ActorRefDeserializer.get().deserialize(protobufMessage.getSender()) : null;
        ActorRef receiver = ActorRefDeserializer.get().deserialize(protobufMessage.getReceiver());
        String messageClass = protobufMessage.getPayloadClass();
        UUID id = UUIDTools.toUUID(protobufMessage.getId().toByteArray());
        boolean durable = (protobufMessage.hasDurable()) ? protobufMessage.getDurable() : true;
        boolean undeliverable = protobufMessage.hasUndeliverable() ? protobufMessage.getUndeliverable() : false;
        return new InternalMessageImpl(id,sender,receiver,protobufMessage.getPayload().asReadOnlyByteBuffer(),messageClass,durable,undeliverable);
    }
}
