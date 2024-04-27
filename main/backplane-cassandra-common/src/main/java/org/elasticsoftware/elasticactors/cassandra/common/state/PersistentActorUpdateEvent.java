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

package org.elasticsoftware.elasticactors.cassandra.common.state;

import jakarta.annotation.Nullable;
import org.elasticsoftware.elasticactors.ShardKey;
import org.elasticsoftware.elasticactors.messaging.InternalMessage;
import org.elasticsoftware.elasticactors.messaging.MessageHandlerEventListener;
import org.elasticsoftware.elasticactors.util.concurrent.ThreadBoundEvent;

import java.nio.ByteBuffer;

/**
 * @author Joost van de Wijgerd
 */
public record PersistentActorUpdateEvent(
        String[] rowKey,
        ShardKey shardKey,
        String persistentActorId,
        @Nullable ByteBuffer persistentActorBytes,
        @Nullable InternalMessage message,
        @Nullable MessageHandlerEventListener eventListener
) implements ThreadBoundEvent<Integer> {

    @Override
    public Integer getKey() {
        return shardKey.getShardId();
    }

    public boolean hasPersistentActorBytes() {
        return persistentActorBytes != null;
    }

    @Override
    @Nullable
    public ByteBuffer persistentActorBytes() {
        // Using duplicate to give implementations a chance to access the internal byte array
        return persistentActorBytes != null ? persistentActorBytes.duplicate() : null;
    }
}
