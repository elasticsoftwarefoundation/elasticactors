package org.elasticsoftware.elasticactors.kafka.state;

import org.elasticsoftware.elasticactors.ShardKey;
import org.elasticsoftware.elasticactors.state.PersistentActor;

public interface PersistentActorStore {
    ShardKey getShardKey();

    void put(String actorId, byte[] persistentActorBytes);

    void put(String actorId, byte[] persistentActorBytes, long offset);

    boolean containsKey(String actorId);

    PersistentActor<ShardKey> getPersistentActor(String actorId);

    void remove(String actorId);

    default void init() { }

    default void destroy() {}

    int count();

    default long getOffset() {
        return -1L;
    }

    default boolean isConcurrent() {
        return false;
    }
}
