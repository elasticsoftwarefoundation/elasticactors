/*
 * Copyright 2013 - 2023 The Original Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 *     you may not use this file except in compliance with the License.
 *     You may obtain a copy of the License at
 *
 *           http://www.apache.org/licenses/LICENSE-2.0
 *
 *     Unless required by applicable law or agreed to in writing, software
 *     distributed under the License is distributed on an "AS IS" BASIS,
 *     WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *     See the License for the specific language governing permissions and
 *     limitations under the License.
 *
 */

package org.elasticsoftware.elasticactors.kafka.state;

import org.elasticsoftware.elasticactors.ShardKey;
import org.elasticsoftware.elasticactors.serialization.Deserializer;
import org.elasticsoftware.elasticactors.state.PersistentActor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.env.Environment;

import java.io.IOException;

public final class ChronicleMapPersistentActorStoreFactory implements PersistentActorStoreFactory {
    private static final Logger logger = LoggerFactory.getLogger(ChronicleMapPersistentActorStoreFactory.class);
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
