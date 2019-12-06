/*
 *   Copyright 2013 - 2019 The Original Authors
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
import org.elasticsoftware.elasticactors.client.cluster.RemoteActorSystemActorShardRefFactory;
import org.elasticsoftware.elasticactors.client.serialization.ClientSerializationFrameworks;
import org.elasticsoftware.elasticactors.cluster.ActorRefFactory;
import org.elasticsoftware.elasticactors.cluster.ShardAccessor;
import org.elasticsoftware.elasticactors.serialization.SerializationFramework;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;

import java.util.List;

public class ClientConfiguration {

    @Bean
    public ClientSerializationFrameworks serializationFrameworks(
            ActorRefFactory actorRefFactory,
            List<SerializationFramework> serializationFrameworks) {
        return new ClientSerializationFrameworks(actorRefFactory, serializationFrameworks);
    }

    @Bean
    public ActorRefFactory remoteActorShardRefFactory(
            ShardAccessor shardAccessor,
            @Value("${ea.actorRefCache.maximumSize:10240}") Integer maximumSize) {
        Cache<String, ActorRef> actorRefCache =
                CacheBuilder.newBuilder().maximumSize(maximumSize).build();
        return new RemoteActorSystemActorShardRefFactory(shardAccessor, actorRefCache);
    }

}
