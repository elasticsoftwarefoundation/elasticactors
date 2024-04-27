/*
 * Copyright 2013 - 2024 The Original Authors
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

package org.elasticsoftware.elasticactors.cluster;

import jakarta.annotation.Nullable;

import java.nio.ByteBuffer;

/**
 * @author Joost van de Wijgerd
 */
public final class ActorSystemEventListenerImpl implements ActorSystemEventListener {
    private final String actorId;
    private final Class messageClass;
    private final ByteBuffer messageBytes;
    private final String messageQueueAffinityKey;

    public ActorSystemEventListenerImpl(
        String actorId,
        Class messageClass,
        ByteBuffer messageBytes,
        String messageQueueAffinityKey)
    {
        this.actorId = actorId;
        this.messageClass = messageClass;
        this.messageBytes = messageBytes;
        this.messageQueueAffinityKey = messageQueueAffinityKey;
    }

    @Override
    public String getActorId() {
        return actorId;
    }

    @Override
    public Class getMessageClass() {
        return messageClass;
    }

    @Override
    public ByteBuffer getMessageBytes() {
        // Using duplicate to give implementations a chance to access the internal byte array
        return messageBytes.duplicate();
    }

    @Nullable
    @Override
    public String getMessageQueueAffinityKey() {
        if (messageQueueAffinityKey != null) {
            return messageQueueAffinityKey;
        }
        return actorId;
    }
}
