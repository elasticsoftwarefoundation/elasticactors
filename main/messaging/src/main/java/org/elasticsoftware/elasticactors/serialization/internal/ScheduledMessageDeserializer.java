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
import org.elasticsoftware.elasticactors.serialization.internal.tracing.CreationContextDeserializer;
import org.elasticsoftware.elasticactors.serialization.internal.tracing.TraceContextDeserializer;
import org.elasticsoftware.elasticactors.serialization.protobuf.Messaging;
import org.elasticsoftware.elasticactors.tracing.CreationContext;
import org.elasticsoftware.elasticactors.tracing.TraceContext;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.UUID;

import static org.elasticsoftware.elasticactors.util.ClassLoadingHelper.getClassHelper;

/**
 * @author Joost van de Wijgerd
 */
public final class ScheduledMessageDeserializer implements Deserializer<ByteBuffer, ScheduledMessage> {

    private final ActorRefDeserializer actorRefDeserializer;

    public ScheduledMessageDeserializer(ActorRefDeserializer actorRefDeserializer) {
        this.actorRefDeserializer = actorRefDeserializer;
    }

    @Override
    public ScheduledMessage deserialize(ByteBuffer serializedObject) throws IOException {
        try {
            // Using duplicate instead of asReadOnlyBuffer so implementations can optimize this in case
            // the original byte buffer has an array
            Messaging.ScheduledMessage protobufMessage = Messaging.ScheduledMessage.parseFrom(serializedObject.duplicate());
            ActorRef sender = getSender(protobufMessage);
            ActorRef receiver = actorRefDeserializer.deserialize(protobufMessage.getReceiver());
            Class messageClass = getClassHelper().forName(protobufMessage.getMessageClass());
            UUID id = UUIDTools.toUUID(protobufMessage.getId().toByteArray());
            long fireTime = protobufMessage.getFireTime();
            TraceContext traceContext = protobufMessage.hasTraceContext()
                    ? TraceContextDeserializer.deserialize(protobufMessage.getTraceContext())
                    : null;
            CreationContext creationContext = protobufMessage.hasCreationContext()
                    ? CreationContextDeserializer.deserialize(protobufMessage.getCreationContext())
                    : null;
            String messageQueueAffinityKey = protobufMessage.hasMessageQueueAffinityKey()
                ? protobufMessage.getMessageQueueAffinityKey()
                : "";
            return new ScheduledMessageImpl(
                id,
                fireTime,
                sender,
                receiver,
                messageClass,
                protobufMessage.getMessage().asReadOnlyByteBuffer(),
                messageQueueAffinityKey.isEmpty() ? null : messageQueueAffinityKey,
                traceContext,
                creationContext
            );
        } catch(ClassNotFoundException e) {
            throw new IOException(e);
        }
    }

    private ActorRef getSender(Messaging.ScheduledMessage protobufMessage) throws IOException {
        if (protobufMessage.hasSender()) {
            String senderStr = protobufMessage.getSender();
            return senderStr.isEmpty()
                ? null
                : actorRefDeserializer.deserialize(senderStr);
        } else {
            return null;
        }
    }
}
