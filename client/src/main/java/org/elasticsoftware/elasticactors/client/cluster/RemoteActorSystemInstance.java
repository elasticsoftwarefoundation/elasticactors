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

package org.elasticsoftware.elasticactors.client.cluster;

import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;
import org.elasticsoftware.elasticactors.ActorRef;
import org.elasticsoftware.elasticactors.ActorRefGroup;
import org.elasticsoftware.elasticactors.ActorShard;
import org.elasticsoftware.elasticactors.ActorState;
import org.elasticsoftware.elasticactors.ActorSystem;
import org.elasticsoftware.elasticactors.ActorSystemConfiguration;
import org.elasticsoftware.elasticactors.ActorSystems;
import org.elasticsoftware.elasticactors.RemoteActorSystemConfiguration;
import org.elasticsoftware.elasticactors.ShardKey;
import org.elasticsoftware.elasticactors.cluster.ActorSystemEventListenerRegistry;
import org.elasticsoftware.elasticactors.cluster.ShardAccessor;
import org.elasticsoftware.elasticactors.messaging.MessageQueueFactory;
import org.elasticsoftware.elasticactors.messaging.internal.CreateActorMessage;
import org.elasticsoftware.elasticactors.scheduler.Scheduler;
import org.elasticsoftware.elasticactors.serialization.Message;
import org.elasticsoftware.elasticactors.serialization.MessageDeserializer;
import org.elasticsoftware.elasticactors.serialization.MessageSerializer;
import org.elasticsoftware.elasticactors.serialization.SerializationAccessor;
import org.elasticsoftware.elasticactors.serialization.SerializationFramework;
import org.elasticsoftware.elasticactors.serialization.SerializationFrameworks;

import javax.annotation.Nullable;
import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import java.nio.charset.StandardCharsets;
import java.util.Collection;

public final class RemoteActorSystemInstance implements ActorSystem, ShardAccessor, SerializationAccessor {

    private final HashFunction hashFunction = Hashing.murmur3_32();
    private final RemoteActorSystemConfiguration configuration;
    private final ActorShard[] shards;

    private final SerializationFrameworks serializationFrameworks;
    private final MessageQueueFactory messageQueueFactory;

    public RemoteActorSystemInstance(
            RemoteActorSystemConfiguration configuration,
            SerializationFrameworks serializationFrameworks,
            MessageQueueFactory messageQueueFactory) {
        this.configuration = configuration;
        this.serializationFrameworks = serializationFrameworks;
        this.messageQueueFactory = messageQueueFactory;
        this.shards = new ActorShard[configuration.getNumberOfShards()];
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
    public <T> ActorRef actorOf(String actorId, Class<T> actorClass, ActorState initialState)
            throws Exception {
        return actorOf(actorId, actorClass.getName(), initialState);
    }

    @Override
    public ActorRef actorOf(String actorId, String actorClassName, ActorState initialState)
            throws Exception {
        return actorOf(actorId, actorClassName, initialState, null);
    }

    /**
     * A specialization of {@link ActorSystem#actorOf(String, String, ActorState, ActorRef)} for a
     * RemoteActorSystem. <br/><br/>
     * <strong>
     * The creatorRef is always ignored.
     * </strong>
     */
    @Override
    public ActorRef actorOf(
            String actorId,
            String actorClassName,
            ActorState initialState,
            ActorRef creatorRef) throws Exception {
        ActorShard shard = shardFor(actorId);
        // send CreateActorMessage to shard
        CreateActorMessage createActorMessage =
                new CreateActorMessage(getName(), actorClassName, actorId, initialState);
        shard.sendMessage(null, shard.getActorRef(), createActorMessage);
        // create actor ref
        // @todo: add cache to speed up performance
        return new RemoteActorSystemActorShardRef(configuration.getClusterName(), shard, actorId);
    }

    @Override
    public <T> ActorRef tempActorOf(Class<T> actorClass, @Nullable ActorState initialState)
            throws Exception {
        throw new UnsupportedOperationException("Temporary Actors are not supported for Remote ActorSystem instances");
    }

    @Override
    public ActorRef actorFor(String actorId) {
        ActorShard shard = shardFor(actorId);
        return new RemoteActorSystemActorShardRef(configuration.getClusterName(), shard, actorId);
    }

    private ActorShard shardFor(String actorId) {
        return shards[Math.abs(hashFunction.hashString(actorId, StandardCharsets.UTF_8).asInt()) % shards.length];
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
        return new ActorSystemDelegateConfiguration(configuration);
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
            this.shards[i] = new RemoteActorSystemActorShard(
                    configuration.getClusterName(),
                    new ShardKey(configuration.getName(), i),
                    messageQueueFactory,
                    serializationFrameworks);
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
    public <T> MessageSerializer<T> getSerializer(Class<T> messageClass) {
        MessageSerializer<T> messageSerializer = serializationFrameworks.getSystemMessageSerializer(messageClass);
        if(messageSerializer == null) {
            Message messageAnnotation = messageClass.getAnnotation(Message.class);
            if(messageAnnotation != null) {
                SerializationFramework framework = serializationFrameworks.getSerializationFramework(messageAnnotation.serializationFramework());
                messageSerializer = framework.getSerializer(messageClass);
            }
        }
        return messageSerializer;
    }

    @Override
    public <T> MessageDeserializer<T> getDeserializer(Class<T> messageClass) {
        MessageDeserializer<T> messageDeserializer = serializationFrameworks.getSystemMessageDeserializer(messageClass);
        if(messageDeserializer == null) {
            Message messageAnnotation = messageClass.getAnnotation(Message.class);
            if(messageAnnotation != null) {
                SerializationFramework framework = serializationFrameworks.getSerializationFramework(messageAnnotation.serializationFramework());
                messageDeserializer = framework.getDeserializer(messageClass);
            }
        }
        return messageDeserializer;
    }
}
