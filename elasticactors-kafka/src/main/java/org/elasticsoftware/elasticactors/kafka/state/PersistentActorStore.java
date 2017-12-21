package org.elasticsoftware.elasticactors.kafka.state;

import org.elasticsoftware.elasticactors.ShardKey;
import org.elasticsoftware.elasticactors.state.PersistentActor;

public interface PersistentActorStore {
    ShardKey getShardKey();

    void put(String actorId, byte[] persistentActorBytes);

    boolean containsKey(String actorId);

    PersistentActor<ShardKey> getPersistentActor(String actorId);

    void remove(String actorId);

    default void destroy() {}

    int count();
}
