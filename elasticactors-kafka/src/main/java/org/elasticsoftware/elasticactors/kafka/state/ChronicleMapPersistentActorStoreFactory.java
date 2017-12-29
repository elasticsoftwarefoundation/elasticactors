package org.elasticsoftware.elasticactors.kafka.state;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsoftware.elasticactors.ShardKey;
import org.elasticsoftware.elasticactors.serialization.Deserializer;
import org.elasticsoftware.elasticactors.state.PersistentActor;

import java.io.IOException;

public final class ChronicleMapPersistentActorStoreFactory implements PersistentActorStoreFactory {
    private static final Logger logger = LogManager.getLogger(ChronicleMapPersistentActorStoreFactory.class);

    @Override
    public PersistentActorStore create(ShardKey shardKey, Deserializer<byte[], PersistentActor<ShardKey>> deserializer) {
        try {
            return new ChronicleMapPersistentActorStore(shardKey, deserializer);
        } catch(IOException e) {
            logger.warn("IOException while creating ChronicleMapPersistenActorStore, falling back to InMemoryPersistentActorStore", e);
            return new InMemoryPersistentActorStore(shardKey, deserializer);
        }
    }
}
