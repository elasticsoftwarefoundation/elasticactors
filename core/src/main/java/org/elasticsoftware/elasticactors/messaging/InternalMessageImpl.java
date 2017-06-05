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

package org.elasticsoftware.elasticactors.messaging;

import com.google.common.collect.ImmutableList;
import org.elasticsoftware.elasticactors.ActorRef;
import org.elasticsoftware.elasticactors.serialization.MessageDeserializer;
import org.elasticsoftware.elasticactors.serialization.SerializationContext;
import org.elasticsoftware.elasticactors.serialization.internal.InternalMessageSerializer;

import java.io.IOException;
import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.UUID;

/**
 * @author Joost van de Wijgerd
 */
public final class InternalMessageImpl implements InternalMessage,Serializable {
    private final ActorRef sender;
    private final ImmutableList<ActorRef> receivers;
    private final UUID id;
    private final ByteBuffer payload;
    private final String payloadType;
    private final String payloadVersion;
    private final boolean durable;
    private final boolean undeliverable;
    private final int timeout;
    private transient byte[] serializedForm;

    public InternalMessageImpl(ActorRef sender, ActorRef receiver, ByteBuffer payload, String payloadType, String payloadVersion, boolean durable) {
        this(UUIDTools.createTimeBasedUUID(), sender, receiver, payload, payloadType, payloadVersion, durable, false);
    }

    public InternalMessageImpl(ActorRef sender, ImmutableList<ActorRef> receivers, ByteBuffer payload, String payloadType, String payloadVersion, boolean durable, int timeout) {
        this(UUIDTools.createTimeBasedUUID(), sender, receivers, payload, payloadType, payloadVersion , durable, false, timeout);
    }

    public InternalMessageImpl(ActorRef sender, ActorRef receiver, ByteBuffer payload, String payloadType, String payloadVersion, boolean durable, boolean undeliverable, int timeout) {
        this(UUIDTools.createTimeBasedUUID(), sender, ImmutableList.of(receiver), payload, payloadType, payloadVersion, durable, undeliverable, timeout);
    }

    public InternalMessageImpl(UUID id, ActorRef sender, ActorRef receiver, ByteBuffer payload, String payloadType, String payloadVersion, boolean durable, boolean undeliverable) {
        this(id, sender, ImmutableList.of(receiver), payload, payloadType, payloadVersion, durable, undeliverable, NO_TIMEOUT);
    }

    public InternalMessageImpl(UUID id,
                               ActorRef sender,
                               ImmutableList<ActorRef> receivers,
                               ByteBuffer payload,
                               String payloadType,
                               String payloadVersion,
                               boolean durable,
                               boolean undeliverable,
                               int timeout) {
        this.sender = sender;
        this.receivers = receivers;
        this.id = id;
        this.payload = payload;
        this.payloadType = payloadType;
        this.payloadVersion = payloadVersion;
        this.durable = durable;
        this.undeliverable = undeliverable;
        this.timeout = timeout;
    }

    public ActorRef getSender() {
        return sender;
    }

    @Override
    public ImmutableList<ActorRef> getReceivers() {
        return receivers;
    }

    public UUID getId() {
        return id;
    }

    public ByteBuffer getPayload() {
        return payload;
    }

    @Override
    public <T> T getPayload(MessageDeserializer<T> deserializer) throws IOException {
        //return deserializer.deserialize(payload);
        return SerializationContext.deserialize(deserializer, payload);
    }

    @Override
    public String getPayloadType() {
        return payloadType;
    }

    @Override
    public String getPayloadVersion() {
        return payloadVersion;
    }

    @Override
    public boolean isDurable() {
        return durable;
    }

    @Override
    public boolean isUndeliverable() {
        return undeliverable;
    }

    @Override
    public int getTimeout() {
        return timeout;
    }

    @Override
    public byte[] toByteArray() {
        if(serializedForm == null) {
            serializedForm = InternalMessageSerializer.get().serialize(this);
        }
        return serializedForm;
    }

    @Override
    public InternalMessage copyOf() {
        return new InternalMessageImpl(id, sender, receivers, payload.asReadOnlyBuffer(), payloadType, payloadVersion, durable, undeliverable, timeout);
    }
}
