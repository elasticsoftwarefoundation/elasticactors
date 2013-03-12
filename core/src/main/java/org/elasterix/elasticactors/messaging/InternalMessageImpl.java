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

    public InternalMessageImpl(ActorRef sender, ActorRef receiver, ByteBuffer payload, String payloadClass) {
        this(UUIDTools.createTimeBasedUUID(), sender, receiver, payload, payloadClass);
    }

    public InternalMessageImpl(UUID id, ActorRef sender, ActorRef receiver, ByteBuffer payload, String payloadClass) {
        this.id = id;
        this.sender = sender;
        this.receiver = receiver;
        this.payload = payload;
        this.payloadClass = payloadClass;
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
    public String getPayloadClass() {
        return payloadClass;
    }
}
