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
import org.elasticsoftware.elasticactors.serialization.MessageDeserializer;
import org.elasticsoftware.elasticactors.serialization.SerializationAccessor;
import org.elasticsoftware.elasticactors.serialization.internal.tracing.CreationContextDeserializer;
import org.elasticsoftware.elasticactors.serialization.internal.tracing.TraceContextDeserializer;
import org.elasticsoftware.elasticactors.serialization.protobuf.Messaging;
import org.elasticsoftware.elasticactors.tracing.CreationContext;
import org.elasticsoftware.elasticactors.tracing.TraceContext;
import org.elasticsoftware.elasticactors.util.ByteBufferUtils;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.UUID;

import static org.elasticsoftware.elasticactors.messaging.UUIDTools.toUUID;
import static org.elasticsoftware.elasticactors.util.ClassLoadingHelper.getClassHelper;

/**
 * @author Joost van de Wijgerd
 */
public final class InternalMessageDeserializer implements Deserializer<ByteBuffer,InternalMessage> {
    private final ActorRefDeserializer actorRefDeserializer;
    private final SerializationAccessor serializationAccessor;

    public InternalMessageDeserializer(ActorRefDeserializer actorRefDeserializer, SerializationAccessor serializationAccessor) {
        this.actorRefDeserializer = actorRefDeserializer;
        this.serializationAccessor = serializationAccessor;
    }

    @Override
    public InternalMessage deserialize(ByteBuffer serializedObject) throws IOException {
        Messaging.InternalMessage protobufMessage = ByteBufferUtils.throwingApplyAndReset(
            serializedObject,
            Messaging.InternalMessage::parseFrom
        );
        ActorRef sender = getSender(protobufMessage);
        // there is either a receiver or a list of receivers
        ImmutableList<ActorRef> receivers = getReceivers(protobufMessage);
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
        // optimize immutable message if possible
        Class<?> immutableMessageClass = getIfImmutableMessageClass(messageClassString);
        ByteBuffer payload = protobufMessage.getPayload().asReadOnlyByteBuffer();
        if (immutableMessageClass != null) {
            MessageDeserializer<?> deserializer = serializationAccessor.getDeserializer(immutableMessageClass);
            Object payloadObject = deserializer.isSafe()
                ? deserializer.deserialize(payload)
                : ByteBufferUtils.throwingApplyAndReset(payload, deserializer::deserialize);
            return new ImmutableInternalMessage(
                id,
                sender,
                receivers,
                payload,
                payloadObject,
                durable,
                undeliverable,
                timeout,
                traceContext,
                creationContext
            );
        } else {
            String messageQueueAffinityKey = protobufMessage.hasMessageQueueAffinityKey()
                ? protobufMessage.getMessageQueueAffinityKey()
                : "";
            return new DefaultInternalMessage(
                id,
                sender,
                receivers,
                payload,
                messageClassString,
                messageQueueAffinityKey.isEmpty() ? null : messageQueueAffinityKey,
                durable,
                undeliverable,
                timeout,
                traceContext,
                creationContext
            );
        }
    }

    @Override
    public boolean isSafe() {
        return true;
    }

    private ImmutableList<ActorRef> getReceivers(Messaging.InternalMessage protobufMessage) throws IOException {
        ActorRef singleReceiver = getSingleReceiver(protobufMessage);
        if (singleReceiver != null) {
            return ImmutableList.of(singleReceiver);
        } else {
            ImmutableList.Builder<ActorRef> listBuilder = ImmutableList.builder();
            for (String receiver : protobufMessage.getReceiversList()) {
                listBuilder.add(actorRefDeserializer.deserialize(receiver));
            }
            return listBuilder.build();
        }
    }

    private ActorRef getSender(Messaging.InternalMessage protobufMessage) throws IOException {
        if (protobufMessage.hasSender()) {
            return deserializeActorRef(protobufMessage.getSender());
        } else {
            return null;
        }
    }

    private ActorRef getSingleReceiver(Messaging.InternalMessage protobufMessage) throws IOException {
        if (protobufMessage.hasReceiver()) {
            return deserializeActorRef(protobufMessage.getReceiver());
        } else {
            return null;
        }
    }

    private ActorRef deserializeActorRef(String actorRef) throws IOException {
        return actorRef.isEmpty() ? null : actorRefDeserializer.deserialize(actorRef);
    }

    private Class<?> getIfImmutableMessageClass(String messageClassString) {
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
