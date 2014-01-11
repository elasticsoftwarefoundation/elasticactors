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

package org.elasticsoftware.elasticactors.messaging;

import org.elasticsoftware.elasticactors.ActorRef;
import org.elasticsoftware.elasticactors.serialization.MessageDeserializer;
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
    private final ActorRef receiver;
    private final UUID id;
    private final ByteBuffer payload;
    private final String payloadClass;
    private final boolean durable;
    private final boolean undeliverable;
    private transient byte[] serializedForm;

    public InternalMessageImpl(ActorRef sender, ActorRef receiver, ByteBuffer payload, String payloadClass) {
        this(UUIDTools.createTimeBasedUUID(), sender, receiver, payload, payloadClass);
    }

    public InternalMessageImpl(ActorRef sender, ActorRef receiver, ByteBuffer payload, String payloadClass,boolean durable) {
        this(UUIDTools.createTimeBasedUUID(), sender, receiver, payload, payloadClass,durable,false);
    }

    public InternalMessageImpl(UUID id, ActorRef sender, ActorRef receiver, ByteBuffer payload, String payloadClass) {
        this(id,sender,receiver,payload,payloadClass,true,false);
    }

    public InternalMessageImpl(ActorRef sender, ActorRef receiver,ByteBuffer payload, String payloadClass, boolean durable, boolean undeliverable) {
        this(UUIDTools.createTimeBasedUUID(), sender, receiver, payload, payloadClass,durable,undeliverable);
    }

    public InternalMessageImpl(UUID id,ActorRef sender, ActorRef receiver,ByteBuffer payload, String payloadClass, boolean durable, boolean undeliverable) {
        this.sender = sender;
        this.receiver = receiver;
        this.id = id;
        this.payload = payload;
        this.payloadClass = payloadClass;
        this.durable = durable;
        this.undeliverable = undeliverable;
    }

    public ActorRef getSender() {
        return sender;
    }

    public ActorRef getReceiver() {
        return receiver;
    }

    public UUID getId() {
        return id;
    }

    public ByteBuffer getPayload() {
        return payload;
    }

    @Override
    public <T> T getPayload(MessageDeserializer<T> deserializer) throws IOException {
        return deserializer.deserialize(payload);
    }

    @Override
    public String getPayloadClass() {
        return payloadClass;
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
    public byte[] toByteArray() {
        if(serializedForm == null) {
            serializedForm = InternalMessageSerializer.get().serialize(this);
        }
        return serializedForm;
    }
}
