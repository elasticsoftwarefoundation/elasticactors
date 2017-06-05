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
import org.elasticsoftware.elasticactors.ActorRef;
import org.elasticsoftware.elasticactors.cluster.InternalActorSystem;
import org.elasticsoftware.elasticactors.messaging.ImmutableInternalMessage;
import org.elasticsoftware.elasticactors.messaging.InternalMessage;
import org.elasticsoftware.elasticactors.messaging.InternalMessageImpl;
import org.elasticsoftware.elasticactors.serialization.Deserializer;
import org.elasticsoftware.elasticactors.serialization.Message;
import org.elasticsoftware.elasticactors.serialization.protobuf.Elasticactors;
import org.elasticsoftware.elasticactors.util.MessageDefinition;
import org.elasticsoftware.elasticactors.util.MessageTools;

import java.io.IOException;
import java.util.UUID;

import static org.elasticsoftware.elasticactors.messaging.UUIDTools.toUUID;

/**
 * @author Joost van de Wijgerd
 */
public final class InternalMessageDeserializer implements Deserializer<byte[],InternalMessage> {
    private final ActorRefDeserializer actorRefDeserializer;
    private final InternalActorSystem internalActorSystem;

    public InternalMessageDeserializer(ActorRefDeserializer actorRefDeserializer, InternalActorSystem internalActorSystem) {
        this.actorRefDeserializer = actorRefDeserializer;
        this.internalActorSystem = internalActorSystem;
    }

    @Override
    public InternalMessage deserialize(byte[] serializedObject) throws IOException {
        Elasticactors.InternalMessage protobufMessage = Elasticactors.InternalMessage.parseFrom(serializedObject);
        ActorRef sender = (protobufMessage.hasSender()) ? actorRefDeserializer.deserialize(protobufMessage.getSender()) : null;
        // there is either a receiver or a list of receivers
        ImmutableList<ActorRef> receivers;
        ActorRef singleReceiver = (protobufMessage.hasReceiver()) ? actorRefDeserializer.deserialize(protobufMessage.getReceiver()) : null;
        if(singleReceiver == null) {
            ImmutableList.Builder<ActorRef> listBuilder = ImmutableList.builder();
            for (String receiver : protobufMessage.getReceiversList()) {
                listBuilder.add(actorRefDeserializer.deserialize(receiver));
            }
            receivers = listBuilder.build();
        } else {
            receivers = ImmutableList.of(singleReceiver);
        }
        String messageType = protobufMessage.getPayloadType();
        String messageVersion = protobufMessage.hasPayloadVersion() ? protobufMessage.getPayloadVersion() : Message.DEFAULT_VERSION;
        MessageDefinition messageDefinition = MessageTools.getMessageDefinition(messageType, messageVersion);

        UUID id = toUUID(protobufMessage.getId().toByteArray());
        boolean durable = (!protobufMessage.hasDurable()) || protobufMessage.getDurable();
        boolean undeliverable = protobufMessage.hasUndeliverable() && protobufMessage.getUndeliverable();
        int timeout = protobufMessage.hasTimeout() ? protobufMessage.getTimeout() : InternalMessage.NO_TIMEOUT;
        //return new InternalMessageImpl(id, sender, receivers, protobufMessage.getPayload().asReadOnlyByteBuffer(), messageType, durable, undeliverable);
        // optimize immutable message if possible
        if(messageDefinition == null || messageDefinition.isImmutable()) {
            // @todo: this will fail later in case MessageDefinition is null!
            return new InternalMessageImpl(id, sender, receivers, protobufMessage.getPayload().asReadOnlyByteBuffer(),
                    messageType, messageVersion , durable, undeliverable, timeout);
        } else {
            Object payloadObject = internalActorSystem.getDeserializer(messageType, messageVersion).deserialize(protobufMessage.getPayload().asReadOnlyByteBuffer());
            return new ImmutableInternalMessage(id, sender, receivers, protobufMessage.getPayload().asReadOnlyByteBuffer(),
                    payloadObject, messageType, messageVersion, durable, undeliverable, timeout);
        }
    }

}
