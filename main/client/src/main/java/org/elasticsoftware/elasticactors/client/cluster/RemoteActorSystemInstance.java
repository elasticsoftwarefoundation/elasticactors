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

import org.elasticsoftware.elasticactors.ActorContainer;
import org.elasticsoftware.elasticactors.ActorContainerRef;
import org.elasticsoftware.elasticactors.ActorRef;
import org.elasticsoftware.elasticactors.ActorRefGroup;
import org.elasticsoftware.elasticactors.ActorShard;
import org.elasticsoftware.elasticactors.ActorState;
import org.elasticsoftware.elasticactors.ActorSystem;
import org.elasticsoftware.elasticactors.ActorSystemConfiguration;
import org.elasticsoftware.elasticactors.ActorSystems;
import org.elasticsoftware.elasticactors.RemoteActorSystemConfiguration;
import org.elasticsoftware.elasticactors.ShardKey;
import org.elasticsoftware.elasticactors.client.messaging.ActorSystemMessage;
import org.elasticsoftware.elasticactors.client.serialization.ActorSystemMessageSerializer;
import org.elasticsoftware.elasticactors.cluster.ActorSystemEventListenerRegistry;
import org.elasticsoftware.elasticactors.cluster.ShardAccessor;
import org.elasticsoftware.elasticactors.messaging.ActorShardHasher;
import org.elasticsoftware.elasticactors.messaging.Hasher;
import org.elasticsoftware.elasticactors.messaging.MessageQueueFactory;
import org.elasticsoftware.elasticactors.messaging.internal.CreateActorMessage;
import org.elasticsoftware.elasticactors.messaging.internal.DestroyActorMessage;
import org.elasticsoftware.elasticactors.scheduler.Scheduler;
import org.elasticsoftware.elasticactors.serialization.Message;
import org.elasticsoftware.elasticactors.serialization.MessageDeserializer;
import org.elasticsoftware.elasticactors.serialization.MessageSerializer;
import org.elasticsoftware.elasticactors.serialization.SerializationAccessor;
import org.elasticsoftware.elasticactors.serialization.SerializationFramework;
import org.elasticsoftware.elasticactors.serialization.SerializationFrameworks;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import java.util.Collection;

public final class RemoteActorSystemInstance
        implements ActorSystem, ShardAccessor, SerializationAccessor {

    private final static Logger logger = LoggerFactory.getLogger(RemoteActorSystemInstance.class);

    private final RemoteActorSystemConfiguration configuration;
    private final ActorShard[] shards;
    private final Hasher hasher;

    private final SerializationFrameworks serializationFrameworks;
    private final MessageQueueFactory messageQueueFactory;

    RemoteActorSystemInstance(
            RemoteActorSystemConfiguration configuration,
            SerializationFrameworks serializationFrameworks,
            MessageQueueFactory messageQueueFactory) {
        this.configuration = configuration;
        this.serializationFrameworks = serializationFrameworks;
        this.messageQueueFactory = messageQueueFactory;
        this.shards = new ActorShard[configuration.getNumberOfShards()];
        this.hasher = new ActorShardHasher(configuration.getShardHashSeed());
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
     * RemoteActorSystem. <br><br>
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
        return new RemoteActorShardRef(configuration.getClusterName(), shard, actorId);
    }

    @Override
    public <T> ActorRef tempActorOf(Class<T> actorClass, @Nullable ActorState initialState)
            throws Exception {
        throw new UnsupportedOperationException(
                "Temporary Actors are not supported for Remote ActorSystem instances");
    }

    @Override
    public ActorRef actorFor(String actorId) {
        ActorShard shard = shardFor(actorId);
        return new RemoteActorShardRef(configuration.getClusterName(), shard, actorId);
    }

    private ActorShard shardFor(String actorId) {
        return shards[Math.abs(hasher.hashStringToInt(actorId)) % shards.length];
    }

    @Override
    public ActorRef serviceActorFor(String actorId) {
        throw new UnsupportedOperationException(
                "Service Actors are not supported for Remote ActorSystem instances");
    }

    @Override
    public ActorRef serviceActorFor(String nodeId, String actorId) {
        throw new UnsupportedOperationException(
                "Service Actors are not supported for Remote ActorSystem instances");
    }

    @Override
    public Scheduler getScheduler() {
        throw new UnsupportedOperationException(
                "Scheduler is not supported for Remote ActorSystem instances");
    }

    @Override
    public ActorSystems getParent() {
        throw new UnsupportedOperationException(
                "Parent ActorSystem not available for remote ActorSystem instances");
    }

    @Override
    public void stop(ActorRef actorRef) throws Exception {
        ActorContainer handlingContainer = ((ActorContainerRef) actorRef).getActorContainer();
        handlingContainer.sendMessage(
                null,
                handlingContainer.getActorRef(),
                new DestroyActorMessage(actorRef));
    }

    @Override
    public ActorSystemConfiguration getConfiguration() {
        return new ActorSystemDelegateConfiguration(configuration);
    }

    @Override
    public ActorSystemEventListenerRegistry getEventListenerRegistry() {
        throw new UnsupportedOperationException(
                "Event listener registry is not supported for remote ActorSystem instances");
    }

    @Override
    public ActorRefGroup groupOf(Collection<ActorRef> members) throws IllegalArgumentException {
        throw new UnsupportedOperationException(
                "Creating Remote ActorRefGroup objects is not supported for Remote ActorSystem "
                        + "instances");
    }

    @PostConstruct
    public void init() throws Exception {
        logger.info(
            "Initializing Remote Actor System [{}/{}]",
            configuration.getClusterName(),
            configuration.getName()
        );
        for (int i = 0; i < shards.length; i++) {
            this.shards[i] = new RemoteActorShard(
                    configuration,
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
        logger.info(
            "Destroying Remote Actor System [{}/{}]",
            configuration.getClusterName(),
            configuration.getName()
        );
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
            throw new IllegalArgumentException(String.format(
                    "No ActorShard found for actorPath [%s]",
                    actorPath));
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

    @SuppressWarnings("unchecked")
    @Override
    public <T> MessageSerializer<T> getSerializer(Class<T> messageClass) {
        MessageSerializer<T> messageSerializer =
                serializationFrameworks.getSystemMessageSerializer(messageClass);
        if (messageSerializer == null) {
            Message messageAnnotation = messageClass.getAnnotation(Message.class);
            if (messageAnnotation != null) {
                SerializationFramework framework =
                        serializationFrameworks.getSerializationFramework(messageAnnotation.serializationFramework());
                messageSerializer = framework.getSerializer(messageClass);
            } else if (ActorSystemMessage.class.isAssignableFrom(messageClass)) {
                messageSerializer = (MessageSerializer<T>) ActorSystemMessageSerializer.get();
            }
        }
        return messageSerializer;
    }

    @Override
    public <T> MessageDeserializer<T> getDeserializer(Class<T> messageClass) {
        MessageDeserializer<T> messageDeserializer =
                serializationFrameworks.getSystemMessageDeserializer(messageClass);
        if (messageDeserializer == null) {
            Message messageAnnotation = messageClass.getAnnotation(Message.class);
            if (messageAnnotation != null) {
                SerializationFramework framework =
                        serializationFrameworks.getSerializationFramework(messageAnnotation.serializationFramework());
                messageDeserializer = framework.getDeserializer(messageClass);
            }
        }
        return messageDeserializer;
    }
}
