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
import org.elasticsoftware.elasticactors.cluster.scheduler.ScheduledMessage;
import org.elasticsoftware.elasticactors.messaging.ScheduledMessageImpl;
import org.elasticsoftware.elasticactors.messaging.UUIDTools;
import org.elasticsoftware.elasticactors.serialization.Deserializer;
import org.elasticsoftware.elasticactors.serialization.protobuf.Messaging;
import org.elasticsoftware.elasticactors.tracing.CreationContext;
import org.elasticsoftware.elasticactors.tracing.TraceContext;

import java.io.IOException;
import java.util.UUID;

import static org.elasticsoftware.elasticactors.tracing.CreationContext.forScheduling;

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
            Messaging.ScheduledMessage protobufMessage = Messaging.ScheduledMessage.parseFrom(serializedObject);
            ActorRef sender = protobufMessage.getSender() != null && !protobufMessage.getSender().isEmpty() ? actorRefDeserializer.deserialize(protobufMessage.getSender()) : null;
            ActorRef receiver = actorRefDeserializer.deserialize(protobufMessage.getReceiver());
            Class messageClass = Class.forName(protobufMessage.getMessageClass());
            byte[] messageBytes = protobufMessage.getMessage().toByteArray();
            UUID id = UUIDTools.toUUID(protobufMessage.getId().toByteArray());
            long fireTime = protobufMessage.getFireTime();
            TraceContext traceContext = new TraceContext(
                    protobufMessage.getTraceContext().getSpanId(),
                    protobufMessage.getTraceContext().getTraceId(),
                    protobufMessage.getTraceContext().getParentSpanId());
            if (traceContext.isEmpty()) {
                traceContext = null;
            }
            CreationContext creationContext = new CreationContext(
                    protobufMessage.getCreationContext().getCreator(),
                    protobufMessage.getCreationContext().getCreatorType(),
                    protobufMessage.getCreationContext().getCreatorMethod());
            if (protobufMessage.getCreationContext().getScheduled()) {
                creationContext = forScheduling(creationContext);
            }
            if (creationContext.isEmpty()) {
                creationContext = null;
            }
            return new ScheduledMessageImpl(
                    id,
                    fireTime,
                    sender,
                    receiver,
                    messageClass,
                    messageBytes,
                    traceContext,
                    creationContext);
        } catch(ClassNotFoundException e) {
            throw new IOException(e);
        }
    }
}
