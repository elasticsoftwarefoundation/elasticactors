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

package org.elasticsoftware.elasticactors.kafka.state;

import org.elasticsoftware.elasticactors.ShardKey;
import org.elasticsoftware.elasticactors.serialization.Deserializer;
import org.elasticsoftware.elasticactors.state.PersistentActor;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public final class InMemoryPersistentActorStore implements PersistentActorStore {
    private final ShardKey shardKey;
    private final Map<String, byte[]> backingMap;
    private final Deserializer<byte[], PersistentActor<ShardKey>> deserializer;

    public InMemoryPersistentActorStore(ShardKey shardKey, Deserializer<byte[], PersistentActor<ShardKey>> deserializer) {
        this.shardKey = shardKey;
        this.deserializer = deserializer;
        this.backingMap = new HashMap<>();
    }

    @Override
    public ShardKey getShardKey() {
        return shardKey;
    }

    @Override
    public void put(String actorId, byte[] persistentActorBytes) {
        backingMap.put(actorId, persistentActorBytes);
    }

    @Override
    public void put(String actorId, byte[] persistentActorBytes, long offset) {
        backingMap.put(actorId, persistentActorBytes);
    }

    @Override
    public void remove(String actorId) {
        backingMap.remove(actorId);
    }

    @Override
    public boolean containsKey(String actorId) {
        return backingMap.containsKey(actorId);
    }

    @Override
    public PersistentActor<ShardKey> getPersistentActor(String actorId) {
        byte[] persistentActorBytes = backingMap.get(actorId);
        try {
            return persistentActorBytes != null ? deserializer.deserialize(persistentActorBytes) : null;
        } catch(IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public int count() {
        return backingMap.size();
    }
}
