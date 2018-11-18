/*
 * Copyright 2013 - 2017 The Original Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.elasticsoftware.elasticactors.cluster;

import com.google.common.base.Charsets;
import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;
import org.elasticsoftware.elasticactors.*;
import org.elasticsoftware.elasticactors.messaging.MessageQueueFactory;
import org.elasticsoftware.elasticactors.messaging.internal.CreateActorMessage;
import org.elasticsoftware.elasticactors.messaging.internal.DestroyActorMessage;
import org.elasticsoftware.elasticactors.scheduler.Scheduler;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import java.util.Collection;

/**
 * @author Joost van de Wijgerd
 */
public final class  RemoteActorSystemInstance implements ActorSystem, ShardAccessor {
    private final HashFunction hashFunction = Hashing.murmur3_32();
    private final RemoteActorSystemConfiguration configuration;
    private final InternalActorSystems localActorSystems;
    private final ActorShard[] shards;
    private final MessageQueueFactory messageQueueFactory;

    public RemoteActorSystemInstance(RemoteActorSystemConfiguration configuration,
                                     InternalActorSystems localActorSystems,
                                     MessageQueueFactory messageQueueFactory) {
        this.configuration = configuration;
        this.localActorSystems = localActorSystems;
        this.messageQueueFactory = messageQueueFactory;
        this.shards = new ActorShard[configuration.getNumberOfShards()];
    }

    @PostConstruct
    public void init() throws Exception {
        for (int i = 0; i < shards.length; i++) {
            shards[i] = new RemoteActorSystemActorShard(localActorSystems,configuration.getClusterName(),configuration.getName(),i,messageQueueFactory);
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

    @Override
    public String getName() {
        return configuration.getName();
    }

    @Override
    public <T> ActorRef actorOf(String actorId, Class<T> actorClass) throws Exception {
        return actorOf(actorId, actorClass.getName(), null);
    }

    @Override
    public ActorRef actorOf(String actorId, String actorClassName) throws Exception {
        return actorOf(actorId, actorClassName, null);
    }

    @Override
    public <T> ActorRef actorOf(String actorId, Class<T> actorClass, ActorState initialState) throws Exception {
        return actorOf(actorId, actorClass.getName(), initialState);
    }

    @Override
    public ActorRef actorOf(String actorId, String actorClassName, ActorState initialState) throws Exception {
        return actorOf(actorId, actorClassName, initialState, ActorContextHolder.getSelf());
    }

    @Override
    public ActorRef actorOf(String actorId, String actorClassName, ActorState initialState, ActorRef creator) throws Exception {
        // determine shard
        ActorShard shard = shardFor(actorId);
        // send CreateActorMessage to shard
        CreateActorMessage createActorMessage = new CreateActorMessage(getName(), actorClassName, actorId, initialState);
        shard.sendMessage(creator, shard.getActorRef(), createActorMessage);
        // create actor ref
        // @todo: add cache to speed up performance
        return new ActorShardRef(configuration.getClusterName(), shard, actorId, localActorSystems.get(null));
    }

    @Override
    public <T extends ElasticActor> ActorRef tempActorOf(Class<T> actorClass, ActorState initialState) throws Exception {
        throw new UnsupportedOperationException("Temporary Actors are not supported for Remote ActorSystem instances");
    }

    @Override
    public ActorRef actorFor(String actorId) {
        // determine shard
        ActorShard shard = shardFor(actorId);
        // return actor ref
        // @todo: add cache to speed up performance
        return new ActorShardRef(configuration.getClusterName(), shard, actorId, localActorSystems.get(null));
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
    public ActorRefGroup groupOf(Collection<ActorRef> members) throws IllegalArgumentException {
        throw new UnsupportedOperationException("Creating Remote ActorRefGroup objects is not supported for Remote ActorSystem instances");
    }

    @Override
    public ActorSystems getParent() {
        return localActorSystems;
    }

    @Override
    public void stop(ActorRef actorRef) throws Exception {
        // set sender if we have any in the current context
        ActorRef sender = ActorContextHolder.getSelf();
        ActorContainer handlingContainer = ((ActorContainerRef) actorRef).getActorContainer();
        handlingContainer.sendMessage(sender, handlingContainer.getActorRef(), new DestroyActorMessage(actorRef));
    }

    @Override
    public ActorShard getShard(String actorPath) {
        // for now we support only <ActorSystemName>/shards/<shardId>
        // @todo: do this with actorRef tools
        String[] pathElements = actorPath.split("/");
        if (pathElements[1].equals("shards")) {
            return getShard(Integer.parseInt(pathElements[2]));
        } else {
            throw new IllegalArgumentException(String.format("No ActorShard found for actorPath [%s]", actorPath));
        }
    }

    @Override
    public ActorShard getShard(int shardId) {
        return shards[shardId];
    }

    @Override
    public int getNumberOfShards() {
        return shards.length;
    }

    @Override
    public ActorSystemConfiguration getConfiguration() {
        throw new UnsupportedOperationException("Access to the ActorSystemConfiguration is not supported for Remote ActorSystem instances");
    }

    @Override
    public ActorSystemEventListenerRegistry getEventListenerRegistry() {
        throw new UnsupportedOperationException("ActorSystemEventListenerRegistry is not supported for Remote ActorSystem instances");
    }

    private ActorShard shardFor(String actorId) {
        return shards[Math.abs(hashFunction.hashString(actorId, Charsets.UTF_8).asInt()) % shards.length];
    }
}
