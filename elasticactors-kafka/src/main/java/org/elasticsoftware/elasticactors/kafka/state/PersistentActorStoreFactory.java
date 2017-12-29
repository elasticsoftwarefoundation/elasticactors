package org.elasticsoftware.elasticactors.kafka.state;

import org.elasticsoftware.elasticactors.ShardKey;
import org.elasticsoftware.elasticactors.serialization.Deserializer;
import org.elasticsoftware.elasticactors.state.PersistentActor;

public interface PersistentActorStoreFactory {
    PersistentActorStore create(ShardKey shardKey, Deserializer<byte[], PersistentActor<ShardKey>> deserializer);
}
