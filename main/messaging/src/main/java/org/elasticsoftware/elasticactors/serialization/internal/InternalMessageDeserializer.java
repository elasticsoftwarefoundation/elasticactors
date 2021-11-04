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

import com.google.common.collect.ImmutableList;
import org.elasticsoftware.elasticactors.ActorRef;
import org.elasticsoftware.elasticactors.messaging.DefaultInternalMessage;
import org.elasticsoftware.elasticactors.messaging.ImmutableInternalMessage;
import org.elasticsoftware.elasticactors.messaging.InternalMessage;
import org.elasticsoftware.elasticactors.serialization.Deserializer;
import org.elasticsoftware.elasticactors.serialization.Message;
import org.elasticsoftware.elasticactors.serialization.SerializationAccessor;
import org.elasticsoftware.elasticactors.serialization.internal.tracing.CreationContextDeserializer;
import org.elasticsoftware.elasticactors.serialization.internal.tracing.TraceContextDeserializer;
import org.elasticsoftware.elasticactors.serialization.protobuf.Messaging;
import org.elasticsoftware.elasticactors.tracing.CreationContext;
import org.elasticsoftware.elasticactors.tracing.TraceContext;

import java.io.IOException;
import java.util.UUID;

import static org.elasticsoftware.elasticactors.messaging.UUIDTools.toUUID;
import static org.elasticsoftware.elasticactors.util.ClassLoadingHelper.getClassHelper;

/**
 * @author Joost van de Wijgerd
 */
public final class InternalMessageDeserializer implements Deserializer<byte[],InternalMessage> {
    private final ActorRefDeserializer actorRefDeserializer;
    private final SerializationAccessor serializationAccessor;

    public InternalMessageDeserializer(ActorRefDeserializer actorRefDeserializer, SerializationAccessor serializationAccessor) {
        this.actorRefDeserializer = actorRefDeserializer;
        this.serializationAccessor = serializationAccessor;
    }

    @Override
    public InternalMessage deserialize(byte[] serializedObject) throws IOException {
        Messaging.InternalMessage protobufMessage = Messaging.InternalMessage.parseFrom(serializedObject);
        ActorRef sender = (protobufMessage.getSender() != null && !protobufMessage.getSender().isEmpty()) ? actorRefDeserializer.deserialize(protobufMessage.getSender()) : null;
        // there is either a receiver or a list of receivers
        ImmutableList<ActorRef> receivers;
        ActorRef singleReceiver = (protobufMessage.getReceiver() != null && !protobufMessage.getReceiver().isEmpty()) ? actorRefDeserializer.deserialize(protobufMessage.getReceiver()) : null;
        if(singleReceiver == null) {
            ImmutableList.Builder<ActorRef> listBuilder = ImmutableList.builder();
            for (String receiver : protobufMessage.getReceiversList()) {
                listBuilder.add(actorRefDeserializer.deserialize(receiver));
            }
            receivers = listBuilder.build();
        } else {
            receivers = ImmutableList.of(singleReceiver);
        }
        String messageClassString = protobufMessage.getPayloadClass();
        UUID id = toUUID(protobufMessage.getId().toByteArray());
        boolean durable = protobufMessage.getDurable();
        boolean undeliverable = protobufMessage.getUndeliverable();
        int timeout = protobufMessage.getTimeout() != 0 ? protobufMessage.getTimeout() : InternalMessage.NO_TIMEOUT;
        TraceContext traceContext = protobufMessage.hasTraceContext()
                ? TraceContextDeserializer.deserialize(protobufMessage.getTraceContext())
                : null;
        CreationContext creationContext = protobufMessage.hasCreationContext()
                ? CreationContextDeserializer.deserialize(protobufMessage.getCreationContext())
                : null;
        //return new InternalMessageImpl(id, sender, receivers, protobufMessage.getPayload().asReadOnlyByteBuffer(), messageClassString, durable, undeliverable);
        // optimize immutable message if possible

        Class<?> messageClass = isImmutableMessageClass(messageClassString);
        if(messageClass == null) {
            return new DefaultInternalMessage(
                    id,
                    sender,
                    receivers,
                    protobufMessage.getPayload().asReadOnlyByteBuffer(),
                    messageClassString,
                    durable,
                    undeliverable,
                    timeout,
                    traceContext,
                    creationContext);
        } else {
            Object payloadObject = serializationAccessor.getDeserializer(messageClass).deserialize(protobufMessage.getPayload().asReadOnlyByteBuffer());
            return new ImmutableInternalMessage(
                    id,
                    sender,
                    receivers,
                    protobufMessage.getPayload().asReadOnlyByteBuffer(),
                    payloadObject,
                    durable,
                    undeliverable,
                    timeout,
                    traceContext,
                    creationContext);
        }
    }

    private Class<?> isImmutableMessageClass(String messageClassString) {
        try {
            Class<?> messageClass = getClassHelper().forName(messageClassString);
            Message messageAnnotation = messageClass.getAnnotation(Message.class);
            if(messageAnnotation != null &&  messageAnnotation.immutable()) {
                return messageClass;
            } else {
                return null;
            }
        } catch(Exception e) {
            return null;
        }
    }
}
