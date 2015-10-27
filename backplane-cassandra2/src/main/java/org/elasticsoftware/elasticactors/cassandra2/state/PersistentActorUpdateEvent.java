/*
 * Copyright 2013 - 2015 The Original Authors
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

package org.elasticsoftware.elasticactors.cassandra2.state;

import org.elasticsoftware.elasticactors.ShardKey;
import org.elasticsoftware.elasticactors.messaging.InternalMessage;
import org.elasticsoftware.elasticactors.messaging.MessageHandlerEventListener;
import org.elasticsoftware.elasticactors.util.concurrent.ThreadBoundEvent;

import javax.annotation.Nullable;
import java.nio.ByteBuffer;

/**
 * @author Joost van de Wijgerd
 */
public final class PersistentActorUpdateEvent implements ThreadBoundEvent<Integer> {
    private final String[] rowKey;
    private final ShardKey shardKey;
    private final String persistentActorId;
    private final ByteBuffer persistentActorBytes;
    private final InternalMessage message;
    private final MessageHandlerEventListener eventListener;

    public PersistentActorUpdateEvent(String[] rowKey,
                                      ShardKey shardKey,
                                      String persistentActorId,
                                      @Nullable ByteBuffer persistentActorBytes,
                                      @Nullable InternalMessage message,
                                      @Nullable MessageHandlerEventListener eventListener) {
        this.rowKey = rowKey;
        this.shardKey = shardKey;
        this.persistentActorId = persistentActorId;
        this.persistentActorBytes = persistentActorBytes;
        this.message = message;
        this.eventListener = eventListener;
    }

    @Override
    public Integer getKey() {
        return shardKey.getShardId();
    }

    public String[] getRowKey() {
        return rowKey;
    }

    public ShardKey getShardKey() {
        return shardKey;
    }

    public String getPersistentActorId() {
        return persistentActorId;
    }

    @Nullable
    public ByteBuffer getPersistentActorBytes() {
        return persistentActorBytes;
    }

    @Nullable
    public InternalMessage getMessage() {
        return message;
    }

    @Nullable
    public MessageHandlerEventListener getEventListener() {
        return eventListener;
    }
}
