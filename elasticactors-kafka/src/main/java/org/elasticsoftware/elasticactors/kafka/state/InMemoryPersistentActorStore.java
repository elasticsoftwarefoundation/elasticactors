package org.elasticsoftware.elasticactors.kafka.state;

import org.elasticsoftware.elasticactors.ShardKey;
import org.elasticsoftware.elasticactors.serialization.Deserializer;
import org.elasticsoftware.elasticactors.state.PersistentActor;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public final class InMemoryPersistentActorStore implements PersistentActorStore {
    private final Map<String, byte[]> backingMap;
    private final Deserializer<byte[], PersistentActor<ShardKey>> deserializer;

    public InMemoryPersistentActorStore(Deserializer<byte[], PersistentActor<ShardKey>> deserializer) {
        this.deserializer = deserializer;
        this.backingMap = new HashMap<>();
    }

    @Override
    public void put(String actorId, byte[] persistentActorBytes) {
        backingMap.put(actorId, persistentActorBytes);
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
            // @todo: log an error
            throw new RuntimeException(e);
        }
    }


}
