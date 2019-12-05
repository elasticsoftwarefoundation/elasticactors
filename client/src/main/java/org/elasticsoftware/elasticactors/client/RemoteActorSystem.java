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

package org.elasticsoftware.elasticactors.client;

import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;
import org.elasticsoftware.elasticactors.ActorRef;
import org.elasticsoftware.elasticactors.ActorRefGroup;
import org.elasticsoftware.elasticactors.ActorShard;
import org.elasticsoftware.elasticactors.ActorState;
import org.elasticsoftware.elasticactors.ActorSystem;
import org.elasticsoftware.elasticactors.ActorSystemConfiguration;
import org.elasticsoftware.elasticactors.ActorSystems;
import org.elasticsoftware.elasticactors.ShardKey;
import org.elasticsoftware.elasticactors.cluster.ActorSystemEventListenerRegistry;
import org.elasticsoftware.elasticactors.messaging.MessageQueueFactory;
import org.elasticsoftware.elasticactors.scheduler.Scheduler;

import javax.annotation.Nullable;
import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import java.nio.charset.StandardCharsets;
import java.util.Collection;

public class RemoteActorSystem implements ActorSystem {

    private final HashFunction hashFunction = Hashing.murmur3_32();
    private final String clusterName;
    private final ActorSystemConfiguration configuration;
    private final ActorShard[] shards;

    private final SerializationFrameworkCache serializationFrameworkCache;
    private final MessageQueueFactory messageQueueFactory;

    public RemoteActorSystem(
            String clusterName,
            ActorSystemConfiguration configuration,
            SerializationFrameworkCache serializationFrameworkCache,
            MessageQueueFactory messageQueueFactory) {
        this.clusterName = clusterName;
        this.configuration = configuration;
        this.serializationFrameworkCache = serializationFrameworkCache;
        this.messageQueueFactory = messageQueueFactory;
        this.shards = new ActorShard[configuration.getNumberOfShards()];
    }

    @Override
    public String getName() {
        return configuration.getName();
    }

    @Override
    public <T> ActorRef actorOf(String actorId, Class<T> actorClass) throws Exception {
        throw new UnsupportedOperationException("Not yet implemented");
    }

    @Override
    public ActorRef actorOf(String actorId, String actorClassName) throws Exception {
        throw new UnsupportedOperationException("Not yet implemented");
    }

    @Override
    public <T> ActorRef actorOf(String actorId, Class<T> actorClass, ActorState initialState)
            throws Exception {
        throw new UnsupportedOperationException("Not yet implemented");
    }

    @Override
    public ActorRef actorOf(String actorId, String actorClassName, ActorState initialState)
            throws Exception {
        throw new UnsupportedOperationException("Not yet implemented");
    }

    @Override
    public ActorRef actorOf(
            String actorId,
            String actorClassName,
            ActorState initialState,
            ActorRef creatorRef) throws Exception {
        throw new UnsupportedOperationException("Not yet implemented");
    }

    @Override
    public <T> ActorRef tempActorOf(Class<T> actorClass, @Nullable ActorState initialState)
            throws Exception {
        throw new UnsupportedOperationException("Temporary Actors are not supported for Remote ActorSystem instances");
    }

    @Override
    public ActorRef actorFor(String actorId) {
        int shardNum = Math.abs(hashFunction.hashString(actorId, StandardCharsets.UTF_8).asInt())
                % configuration.getNumberOfShards();
        return new RemoteActorShardRef(clusterName, shards[shardNum], actorId);
    }

    @Override
    public ActorRef serviceActorFor(String actorId) {
        throw new UnsupportedOperationException("Service Actors are not supported for Remote ActorSystem instances");
    }

    @Override
    public ActorRef serviceActorFor(String nodeId, String actorId) {
        throw new UnsupportedOperationException("Service Actors are not supported for Remote ActorSystem instances");
    }

    @Override
    public Scheduler getScheduler() {
        throw new UnsupportedOperationException("Scheduler is not supported for Remote ActorSystem instances");
    }

    @Override
    public ActorSystems getParent() {
        throw new UnsupportedOperationException("Parent ActorSystem not available for remote ActorSystem instances");
    }

    @Override
    public void stop(ActorRef actorRef) throws Exception {
        throw new UnsupportedOperationException("Not yet implemented");
    }

    @Override
    public ActorSystemConfiguration getConfiguration() {
        return configuration;
    }

    @Override
    public ActorSystemEventListenerRegistry getEventListenerRegistry() {
        throw new UnsupportedOperationException("Event listener registry is not supported for remote ActorSystem instances");
    }

    @Override
    public ActorRefGroup groupOf(Collection<ActorRef> members) throws IllegalArgumentException {
        throw new UnsupportedOperationException("Creating Remote ActorRefGroup objects is not supported for Remote ActorSystem instances");
    }

    @PostConstruct
    public void init() throws Exception {
        for (int i = 0; i < shards.length; i++) {
            this.shards[i] = new RemoteActorShard(
                    new ShardKey(configuration.getName(), i),
                    messageQueueFactory,
                    serializationFrameworkCache);
        }
        for (ActorShard shard : shards) {
            shard.init();
        }
    }

    @PreDestroy
    public void destroy() {
        for (ActorShard shard : shards) {
            shard.destroy();
        }
    }

}
