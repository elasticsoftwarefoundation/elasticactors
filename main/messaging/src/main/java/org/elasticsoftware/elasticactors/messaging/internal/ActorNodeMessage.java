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

package org.elasticsoftware.elasticactors.messaging.internal;

import org.elasticsoftware.elasticactors.ActorRef;
import org.elasticsoftware.elasticactors.serialization.Message;
import org.elasticsoftware.elasticactors.serialization.SystemSerializationFramework;

import javax.annotation.Nullable;

/**
 * This message is used to call a Temp or a Service actor ref from a remote cluster
 *
 * @author Joost van de Wijgerd
 */
@Message(immutable = true, durable = false, serializationFramework = SystemSerializationFramework.class)
public final class ActorNodeMessage implements MessageQueueBoundPayload {
    private final String nodeId;
    private final ActorRef receiverRef;
    private final Object message;
    private final boolean undeliverable;

    public ActorNodeMessage(String nodeId, ActorRef receiverRef, Object message) {
        this(nodeId, receiverRef, message, false);
    }

    public ActorNodeMessage(String nodeId, ActorRef receiverRef, Object message, boolean undeliverable) {
        this.nodeId = nodeId;
        this.receiverRef = receiverRef;
        this.message = message;
        this.undeliverable = undeliverable;
    }

    public String getNodeId() {
        return nodeId;
    }

    public ActorRef getReceiverRef() {
        return receiverRef;
    }

    public Object getMessage() {
        return message;
    }

    public boolean isUndeliverable() {
        return undeliverable;
    }

    @Nullable
    @Override
    public String getMessageQueueAffinityKey() {
        return receiverRef != null ? receiverRef.getActorId() : null;
    }
}
