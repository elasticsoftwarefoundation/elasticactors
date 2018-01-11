package org.elasticsoftware.elasticactors.kafka.state;

import org.elasticsoftware.elasticactors.ShardKey;
import org.elasticsoftware.elasticactors.serialization.Deserializer;
import org.elasticsoftware.elasticactors.state.PersistentActor;

public final class InMemoryPeristentActorStoreFactory implements PersistentActorStoreFactory {
    @Override
    public PersistentActorStore create(ShardKey shardKey, Deserializer<byte[], PersistentActor<ShardKey>> deserializer) {
        return new InMemoryPersistentActorStore(shardKey, deserializer);
    }
}
