/*
 *   Copyright 2013 - 2022 The Original Authors
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

package org.elasticsoftware.elasticactors.messaging;

import com.google.common.collect.ImmutableList;
import org.elasticsoftware.elasticactors.ActorRef;
import org.elasticsoftware.elasticactors.cluster.InternalActorSystem;
import org.elasticsoftware.elasticactors.serialization.Message;
import org.elasticsoftware.elasticactors.serialization.MessageSerializer;
import org.elasticsoftware.elasticactors.serialization.SerializationContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.List;

public final class InternalMessageFactory {

    private final static Logger logger = LoggerFactory.getLogger(InternalMessageFactory.class);

    private InternalMessageFactory() {
    }

    public static InternalMessage create(
        @Nullable ActorRef from,
        List<? extends ActorRef> to,
        InternalActorSystem actorSystem,
        Object message) throws IOException
    {
        // get the durable flag
        Message messageAnnotation = message.getClass().getAnnotation(Message.class);
        final boolean durable = (messageAnnotation != null) && messageAnnotation.durable();
        final boolean immutable = (messageAnnotation != null) && messageAnnotation.immutable();
        final int timeout = (messageAnnotation != null)
            ? messageAnnotation.timeout()
            : Message.NO_TIMEOUT;
        if (durable || !immutable) {
            // durable so it will go over the bus and needs to be serialized
            return createDefaultInternalMessage(from, to, actorSystem, message, durable, timeout);
        } else {
            // as the message is immutable we can safely send it as a TransientInternalMessage
            return createTransientInternalMessage(from, to, message);
        }
    }

    public static InternalMessage createWithSerializedPayload(
        @Nullable ActorRef from,
        List<? extends ActorRef> to,
        InternalActorSystem actorSystem,
        Object message) throws IOException
    {
        // get the durable flag
        Message messageAnnotation = message.getClass().getAnnotation(Message.class);
        final boolean durable = (messageAnnotation != null) && messageAnnotation.durable();
        final int timeout = (messageAnnotation != null)
            ? messageAnnotation.timeout()
            : Message.NO_TIMEOUT;
        return createDefaultInternalMessage(from, to, actorSystem, message, durable, timeout);
    }

    public static InternalMessage createWithSerializedPayload(
        @Nullable ActorRef from,
        List<? extends ActorRef> to,
        MessageSerializer messageSerializer,
        Object message) throws IOException
    {
        // get the durable flag
        Message messageAnnotation = message.getClass().getAnnotation(Message.class);
        final boolean durable = (messageAnnotation != null) && messageAnnotation.durable();
        final int timeout = (messageAnnotation != null)
            ? messageAnnotation.timeout()
            : Message.NO_TIMEOUT;
        return createDefaultInternalMessage(from, to, messageSerializer, message, durable, timeout);
    }

    public static InternalMessage copyForUndeliverable(
        InternalMessage message,
        ActorRef receiverRef) throws IOException
    {
        if (message instanceof TransientInternalMessage) {
            return new TransientInternalMessage(
                receiverRef,
                message.getSender(),
                message.getPayload(null),
                true
            );
        } else {
            return copyForUndeliverableWithSerializedPayload(message, receiverRef);
        }
    }

    public static DefaultInternalMessage copyForUndeliverableWithSerializedPayload(
        InternalMessage message,
        ActorRef receiverRef)
    {
        return new DefaultInternalMessage(
            receiverRef,
            message.getSender(),
            message.getPayload(),
            message.getPayloadClass(),
            message.isDurable(),
            true,
            message.getTimeout()
        );
    }

    private static DefaultInternalMessage createDefaultInternalMessage(
        ActorRef from,
        List<? extends ActorRef> to,
        InternalActorSystem actorSystem,
        Object message,
        boolean durable,
        int timeout) throws IOException
    {
        MessageSerializer messageSerializer = actorSystem.getSerializer(message.getClass());
        return createDefaultInternalMessage(from, to, messageSerializer, message, durable, timeout);
    }

    private static DefaultInternalMessage createDefaultInternalMessage(
        ActorRef from,
        List<? extends ActorRef> to,
        MessageSerializer messageSerializer,
        Object message,
        boolean durable,
        int timeout) throws IOException
    {
        if(messageSerializer == null) {
            logger.error(
                "No message serializer found for class: {}. NOT sending message",
                message.getClass().getName()
            );
            return null;
        }
        return new DefaultInternalMessage(
            from,
            ImmutableList.copyOf(to),
            SerializationContext.serialize(messageSerializer, message),
            message.getClass().getName(),
            message,
            durable,
            timeout
        );
    }

    private static TransientInternalMessage createTransientInternalMessage(
        ActorRef from,
        List<? extends ActorRef> to,
        Object message)
    {
        return new TransientInternalMessage(
            from,
            ImmutableList.copyOf(to),
            message
        );
    }

}
