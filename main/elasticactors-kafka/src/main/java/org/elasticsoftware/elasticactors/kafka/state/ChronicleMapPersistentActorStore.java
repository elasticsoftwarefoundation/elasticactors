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

package org.elasticsoftware.elasticactors.kafka.state;

import com.google.common.primitives.Longs;
import net.openhft.chronicle.map.ChronicleMap;
import org.elasticsoftware.elasticactors.ShardKey;
import org.elasticsoftware.elasticactors.serialization.Deserializer;
import org.elasticsoftware.elasticactors.state.PersistentActor;

import java.io.File;
import java.io.IOException;

public final class ChronicleMapPersistentActorStore implements PersistentActorStore {
    private final ShardKey shardKey;
    private final Deserializer<byte[], PersistentActor<ShardKey>> deserializer;
    private final ChronicleMap<String, byte[]> backingMap;
    // special key to store the kafka offset in
    private static final String OFFSET_KEY = "____OFFSET___";
    private long offset = -1L;

    ChronicleMapPersistentActorStore(ShardKey shardKey,
                                     Deserializer<byte[], PersistentActor<ShardKey>> deserializer) throws IOException {
        this(shardKey, deserializer, System.getProperty("java.io.tmpdir"), 42d, 512d, 1048576L);
    }

    ChronicleMapPersistentActorStore(ShardKey shardKey,
                                     Deserializer<byte[], PersistentActor<ShardKey>> deserializer,
                                     String dataDirectory,
                                     double averageKeySize,
                                     double averageValueSize,
                                     long maxEntries) throws IOException {
        this.shardKey = shardKey;
        this.deserializer = deserializer;
        File backingFile = new File(dataDirectory+"/"+shardKey.getActorSystemName()+"-"+shardKey.getShardId()+".cmp");
        backingMap = ChronicleMap.of(String.class, byte[].class)
                .averageKeySize(averageKeySize)
                .averageValueSize(averageValueSize)
                .entries(maxEntries)
                .createOrRecoverPersistedTo(backingFile, false);
        // see if we can recover the offset
        byte[] offsetBytes = backingMap.get(OFFSET_KEY);
        if(offsetBytes != null) {
            this.offset = Longs.fromByteArray(offsetBytes);
        }
    }

    @Override
    public void init() {

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
        // first store the bytes
        backingMap.put(actorId, persistentActorBytes);
        // if this fails it is not a problem as we will just reload the state from kafka
        backingMap.put(OFFSET_KEY, Longs.toByteArray(offset));
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
    public void remove(String actorId) {
        backingMap.remove(actorId);
    }

    @Override
    public int count() {
        // we have to subtract the OFFSETS_KEY entry
        return Math.max(0, backingMap.size() -1);
    }

    @Override
    public void destroy() {
        backingMap.close();
    }

    @Override
    public boolean isConcurrent() {
        return true;
    }

    @Override
    public long getOffset() {
        return offset;
    }
}
