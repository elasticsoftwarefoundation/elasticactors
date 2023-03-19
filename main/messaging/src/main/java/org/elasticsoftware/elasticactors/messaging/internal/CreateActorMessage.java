/*
 * Copyright 2013 - 2023 The Original Authors
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

import org.elasticsoftware.elasticactors.ActorState;
import org.elasticsoftware.elasticactors.serialization.Message;
import org.elasticsoftware.elasticactors.serialization.SystemSerializationFramework;

import jakarta.annotation.Nullable;
import java.io.Serializable;

/**
 * @author Joost van de Wijgerd
 */
@Message(immutable = true, durable = false, serializationFramework = SystemSerializationFramework.class)
public final class CreateActorMessage implements Serializable, MessageQueueBoundPayload {
    private final String actorSystem;
    private final String actorId;
    private final String actorClass;
    private final ActorState initialState;
    private final ActorType type;
    private final String affinityKey;

    public CreateActorMessage(String actorSystem, String actorClass, String actorId, ActorState initialState) {
        this(actorSystem,actorClass,actorId,initialState,ActorType.PERSISTENT);
    }

    public CreateActorMessage(String actorSystem, String actorClass, String actorId, ActorState initialState, ActorType type) {
        this(actorSystem, actorClass, actorId, initialState, type, null);
    }

    public CreateActorMessage(String actorSystem, String actorClass, String actorId, ActorState initialState, ActorType type, String affinityKey) {
        this.actorSystem = actorSystem;
        this.actorId = actorId;
        this.actorClass = actorClass;
        this.initialState = initialState;
        this.type = type;
        this.affinityKey = affinityKey;
    }

    public String getActorSystem() {
        return actorSystem;
    }

    public String getActorClass() {
        return actorClass;
    }

    public String getActorId() {
        return actorId;
    }

    public ActorState getInitialState() {
        return initialState;
    }

    public ActorType getType() {
        return type;
    }

    public String getAffinityKey() {
        return affinityKey;
    }

    @Nullable
    @Override
    public String getMessageQueueAffinityKey() {
        return actorId;
    }
}
