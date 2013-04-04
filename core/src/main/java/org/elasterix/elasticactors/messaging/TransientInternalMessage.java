/*
 * Copyright 2013 Joost van de Wijgerd
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

package org.elasterix.elasticactors.messaging;

import org.elasterix.elasticactors.ActorRef;
import org.elasterix.elasticactors.serialization.MessageDeserializer;

import java.io.IOException;
import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.UUID;

/**
 * @author Joost van de Wijgerd
 */
public final class TransientInternalMessage implements InternalMessage,Serializable {
    private final ActorRef sender;
    private final ActorRef receiver;
    private final UUID id;
    private final Object payload;

    public TransientInternalMessage(ActorRef sender, ActorRef receiver, Object payload) {
        this.sender = sender;
        this.receiver = receiver;
        this.id = UUIDTools.createTimeBasedUUID();
        this.payload = payload;
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
        throw new UnsupportedOperationException(String.format("This implementation is intended to be used local only, for remote use [%s]",InternalMessageImpl.class.getSimpleName()));
    }

    @Override
    public <T> T getPayload(MessageDeserializer<T> deserializer) throws IOException {
        return (T) payload;
    }

    @Override
    public String getPayloadClass() {
        return payload.getClass().getName();
    }

    @Override
    public boolean isDurable() {
        return false;
    }

    @Override
    public byte[] toByteArray() {
        throw new UnsupportedOperationException(String.format("This implementation is intended to be used local only, for remote use [%s]",InternalMessageImpl.class.getSimpleName()));
    }
}
