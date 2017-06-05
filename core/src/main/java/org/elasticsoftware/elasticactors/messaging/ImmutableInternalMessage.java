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
import org.elasticsoftware.elasticactors.serialization.internal.InternalMessageSerializer;

import java.io.IOException;
import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.UUID;

/**
 * @author Joost van de Wijgerd
 */
public final class ImmutableInternalMessage implements InternalMessage,Serializable {
    private final ActorRef sender;
    private final ImmutableList<ActorRef> receivers;
    private final UUID id;
    private final ByteBuffer payload;
    private final Object payloadObject;
    private final String payloadType;
    private final String payloadVersion;
    private final boolean durable;
    private final boolean undeliverable;
    private final int timeout;
    private transient byte[] serializedForm;

    public ImmutableInternalMessage(UUID id,
                                    ActorRef sender,
                                    ImmutableList<ActorRef> receivers,
                                    ByteBuffer payload,
                                    Object payloadObject,
                                    String payloadType,
                                    String payloadVersion,
                                    boolean durable,
                                    boolean undeliverable,
                                    int timeout) {
        this.sender = sender;
        this.receivers = receivers;
        this.id = id;
        this.payload = payload;
        this.payloadObject = payloadObject;
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
        return (T) payloadObject;
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
        return this;
    }
}
