/*
 *   Copyright 2013 - 2022 The Original Authors
 *
 *   Licensed under the Apache License, Version 2.0 (the "License");
 *   you may not use this file except in compliance with the License.
 *   You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 */

package org.elasticsoftware.elasticactors.client.configuration;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import org.elasticsoftware.elasticactors.ActorRef;
import org.elasticsoftware.elasticactors.RemoteActorSystemConfiguration;
import org.elasticsoftware.elasticactors.client.cluster.RemoteActorShardRefFactory;
import org.elasticsoftware.elasticactors.client.cluster.RemoteActorSystems;
import org.elasticsoftware.elasticactors.client.serialization.ClientSerializationFrameworks;
import org.elasticsoftware.elasticactors.client.serialization.SerializedActorStateSerializationFramework;
import org.elasticsoftware.elasticactors.messaging.MessageQueueFactoryFactory;
import org.elasticsoftware.elasticactors.serialization.SerializationFramework;
import org.elasticsoftware.elasticactors.serialization.SerializationFrameworks;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Bean;

import java.util.List;

/**
 * The base configuration for a write-only client. MessageQueueFactoryFactory is not implemented
 * here as that depends on the chosen implementation for MessageQueueFactory.
 */
public class ClientConfiguration {

    @Bean
    public ClientSerializationFrameworks clientSerializationFrameworks(
            RemoteActorShardRefFactory actorShardRefFactory,
            List<SerializationFramework> serializationFrameworks) {
        return new ClientSerializationFrameworks(actorShardRefFactory, serializationFrameworks);
    }

    @Bean
    public RemoteActorShardRefFactory remoteActorShardRefFactory(
            ApplicationContext applicationContext,
            @Value("${ea.actorRefCache.maximumSize:10240}") Integer maximumSize) {
        Cache<String, ActorRef> actorRefCache =
                CacheBuilder.newBuilder().maximumSize(maximumSize).build();
        return new RemoteActorShardRefFactory(applicationContext, actorRefCache);
    }

    @Bean
    public RemoteActorSystems remoteActorSystems(
            List<RemoteActorSystemConfiguration> remoteConfigurations,
            SerializationFrameworks serializationFrameworks,
            MessageQueueFactoryFactory remoteActorSystemMessageQueueFactoryFactory) {
        return new RemoteActorSystems(
                remoteConfigurations,
                serializationFrameworks,
                remoteActorSystemMessageQueueFactoryFactory);
    }

    @Bean
    public SerializedActorStateSerializationFramework serializedActorStateSerializationFramework() {
        return new SerializedActorStateSerializationFramework();
    }

}
