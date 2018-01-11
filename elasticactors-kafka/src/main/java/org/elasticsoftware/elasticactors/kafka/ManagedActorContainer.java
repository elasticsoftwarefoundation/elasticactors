package org.elasticsoftware.elasticactors.kafka;

import com.google.common.cache.Cache;
import org.elasticsoftware.elasticactors.ActorRef;
import org.elasticsoftware.elasticactors.NodeKey;
import org.elasticsoftware.elasticactors.ShardKey;
import org.elasticsoftware.elasticactors.state.PersistentActor;

public interface ManagedActorContainer<K> {
    PersistentActor<K> getPersistentActor(ActorRef actorRef);

    void persistActor(PersistentActor<K> persistentActor);

    void deleteActor(PersistentActor<K> persistentActor);

    boolean containsKey(String actorId);

    K getKey();

    Cache<ActorRef, PersistentActor<K>> getActorCache();
}
