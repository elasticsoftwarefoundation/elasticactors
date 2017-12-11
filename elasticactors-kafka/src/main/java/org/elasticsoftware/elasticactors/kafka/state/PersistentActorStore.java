package org.elasticsoftware.elasticactors.kafka.state;

import org.elasticsoftware.elasticactors.ShardKey;
import org.elasticsoftware.elasticactors.state.PersistentActor;

public interface PersistentActorStore {
    void put(String actorId, byte[] persistentActorBytes);

    boolean containsKey(String actorId);

    PersistentActor<ShardKey> getPersistentActor(String actorId);
}
