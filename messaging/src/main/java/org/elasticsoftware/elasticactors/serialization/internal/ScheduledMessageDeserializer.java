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
import org.elasticsoftware.elasticactors.tracing.RealSenderData;
import org.elasticsoftware.elasticactors.tracing.TraceData;

import java.io.IOException;
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
            Messaging.ScheduledMessage protobufMessage = Messaging.ScheduledMessage.parseFrom(serializedObject);
            ActorRef sender = protobufMessage.getSender() != null && !protobufMessage.getSender().isEmpty() ? actorRefDeserializer.deserialize(protobufMessage.getSender()) : null;
            ActorRef receiver = actorRefDeserializer.deserialize(protobufMessage.getReceiver());
            Class messageClass = Class.forName(protobufMessage.getMessageClass());
            byte[] messageBytes = protobufMessage.getMessage().toByteArray();
            UUID id = UUIDTools.toUUID(protobufMessage.getId().toByteArray());
            long fireTime = protobufMessage.getFireTime();
            RealSenderData realSenderData = new RealSenderData(
                    protobufMessage.getRealSenderData().getRealSender(),
                    protobufMessage.getRealSenderData().getRealSenderType());
            if (realSenderData.isEmpty()) {
                realSenderData = null;
            }
            TraceData traceData = new TraceData(
                    protobufMessage.getTraceData().getSpanId(),
                    protobufMessage.getTraceData().getTraceId(),
                    protobufMessage.getTraceData().getParentSpanId());
            if (traceData.isEmpty()) {
                traceData = null;
            }
            return new ScheduledMessageImpl(
                    id,
                    fireTime,
                    sender,
                    receiver,
                    messageClass,
                    messageBytes,
                    realSenderData,
                    traceData);
        } catch(ClassNotFoundException e) {
            throw new IOException(e);
        }
    }
}
