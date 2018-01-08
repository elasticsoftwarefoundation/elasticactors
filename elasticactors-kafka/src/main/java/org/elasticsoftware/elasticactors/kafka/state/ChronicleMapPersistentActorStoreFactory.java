package org.elasticsoftware.elasticactors.kafka.state;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsoftware.elasticactors.ShardKey;
import org.elasticsoftware.elasticactors.serialization.Deserializer;
import org.elasticsoftware.elasticactors.state.PersistentActor;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.env.Environment;

import java.io.IOException;

public final class ChronicleMapPersistentActorStoreFactory implements PersistentActorStoreFactory {
    private static final Logger logger = LogManager.getLogger(ChronicleMapPersistentActorStoreFactory.class);
    private Environment environment;

    @Autowired
    public void setEnvironment(Environment environment) {
        this.environment = environment;
    }

    @Override
    public PersistentActorStore create(ShardKey shardKey, Deserializer<byte[], PersistentActor<ShardKey>> deserializer) {
        try {
            String dataDir = environment.getProperty("ea.kafka.persistentActorStore.dataDirectory", String.class, System.getProperty("java.io.tmpdir"));
            final Double averageKeySize = environment.getProperty("ea.kafka.persistentActorStore.averageKeySize", Double.class, 45d);
            final Double averageValueSize = environment.getProperty("ea.kafka.persistentActorStore.averageValueSize", Double.class, 512d);
            final Long maxEntries = environment.getProperty("ea.kafka.persistentActorStore.averageValueSize", Long.class, 1048576L);
            return new ChronicleMapPersistentActorStore(shardKey, deserializer, dataDir, averageKeySize, averageValueSize, maxEntries);
        } catch(IOException e) {
            logger.warn("IOException while creating ChronicleMapPersistenActorStore, falling back to InMemoryPersistentActorStore", e);
            return new InMemoryPersistentActorStore(shardKey, deserializer);
        }
    }
}
