/*
 * Copyright 2013 - 2025 The Original Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 *     you may not use this file except in compliance with the License.
 *     You may obtain a copy of the License at
 *
 *           http://www.apache.org/licenses/LICENSE-2.0
 *
 *     Unless required by applicable law or agreed to in writing, software
 *     distributed under the License is distributed on an "AS IS" BASIS,
 *     WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *     See the License for the specific language governing permissions and
 *     limitations under the License.
 *
 */

package org.elasticsoftware.elasticactors.messaging.internal;

import jakarta.annotation.Nullable;
import org.elasticsoftware.elasticactors.serialization.Message;
import org.elasticsoftware.elasticactors.serialization.SystemSerializationFramework;

import java.io.Serializable;

/**
 * @author Joost van de Wijgerd
 */
@Message(immutable = true, durable = false, serializationFramework = SystemSerializationFramework.class)
public final class ActivateActorMessage implements Serializable, MessageQueueBoundPayload {
    private final String actorSystem;
    private final String actorId;
    private final ActorType actorType;

    public ActivateActorMessage(String actorSystem, String actorId, ActorType actorType) {
        this.actorSystem = actorSystem;
        this.actorId = actorId;
        this.actorType = actorType;
    }

    public String getActorSystem() {
        return actorSystem;
    }

    public String getActorId() {
        return actorId;
    }

    public ActorType getActorType() {
        return actorType;
    }

    @Nullable
    @Override
    public String getMessageQueueAffinityKey() {
        return actorId;
    }
}
