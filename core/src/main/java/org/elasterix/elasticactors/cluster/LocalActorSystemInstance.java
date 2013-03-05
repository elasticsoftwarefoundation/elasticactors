/*
 * Copyright 2013 Joost van de Wijgerd
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

package org.elasterix.elasticactors.cluster;

import org.apache.log4j.Logger;
import org.elasterix.elasticactors.*;
import org.elasterix.elasticactors.messaging.MessageQueueFactory;
import org.elasterix.elasticactors.serialization.Deserializer;
import org.elasterix.elasticactors.serialization.MessageDeserializer;
import org.elasterix.elasticactors.serialization.MessageSerializer;
import org.elasterix.elasticactors.serialization.Serializer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Configurable;
import org.springframework.beans.factory.annotation.Qualifier;

import java.util.List;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * @author Joost van de Wijgerd
 */
@Configurable
public class LocalActorSystemInstance implements InternalActorSystem {
    private static final Logger log = Logger.getLogger(LocalActorSystemInstance.class);
    private final ActorSystemConfiguration configuration;
    private final ActorShard[] shards;
    private final ReadWriteLock[] shardLocks;
    private final ActorShardAdapter[] shardAdapters;
    private NodeSelectorFactory nodeSelectorFactory;
    private MessageQueueFactory localMessageQueueFactory;
    private MessageQueueFactory remoteMessageQueueFactory;

    public LocalActorSystemInstance(ActorSystemConfiguration actorSystem) {
        this.configuration = actorSystem;
        this.shards = new ActorShard[configuration.getNumberOfShards()];
        this.shardLocks = new ReadWriteLock[shards.length];
        this.shardAdapters = new ActorShardAdapter[shards.length];
        for (int i = 0; i < shards.length; i++) {
            shardLocks[i] = new ReentrantReadWriteLock();
            shardAdapters[i] = new ActorShardAdapter(new ShardKey(actorSystem.getName(),i));
        }
    }

    /**
     * Distribute the shards over the list of physical nodes
     *
     * @param nodes
     */
    public void distributeShards(List<PhysicalNode> nodes) throws Exception {
        NodeSelector nodeSelector = nodeSelectorFactory.create(nodes);
        for(int i = 0; i < configuration.getNumberOfShards(); i++) {
            PhysicalNode node = nodeSelector.getPrimary(new ShardKey(configuration.getName(),i));
            if(node.isLocal()) {
                // this instance should start owning the shard now
                final ActorShard currentShard = shards[i];
                if(currentShard == null || !currentShard.getOwningNode().isLocal()) {
                    // first we need to obtain the writeLock on the shard
                    final Lock writeLock = shardLocks[i].writeLock();
                    try {
                        writeLock.lock();
                        // destroy the current remote shard instance
                        if(currentShard != null) {
                            currentShard.destroy();
                        }
                        // create a new local shard and swap it
                        LocalActorShard newShard = new LocalActorShard(node,this,i,localMessageQueueFactory);
                        // initialize
                        newShard.init();
                        shards[i] = newShard;
                    } finally {
                        writeLock.unlock();
                    }
                } else {
                    // we own the shard already, no change needed
                    log.info("");
                }
            } else {
                // the shard will be managed by another node
                final ActorShard currentShard = shards[i];
                if(currentShard == null || currentShard.getOwningNode().isLocal()) {
                    // first we need to obtain the writeLock on the shard
                    final Lock writeLock = shardLocks[i].writeLock();
                    try {
                        writeLock.lock();
                        // destroy the current remote shard instance
                        if(currentShard != null) {
                            currentShard.destroy();
                        }
                        // create a new local shard and swap it
                        RemoteActorShard newShard = new RemoteActorShard(node,this,i,remoteMessageQueueFactory);
                        // initialize
                        newShard.init();
                        shards[i] = newShard;
                    } finally {
                        writeLock.unlock();
                    }
                } else {
                    // shard was already remote
                }
            }
        }
    }

    public int getNumberOfShards() {
        return configuration.getNumberOfShards();
    }

    @Override
    public ActorShard getShard(int shardId) {
        return shardAdapters[shardId];
    }

    @Override
    public ElasticActor<?> getActorInstance(ActorRef actorRef) {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public String getName() {
        return configuration.getName();
    }

    @Override
    public <T> ActorRef actorOf(String actorId, Class<T> actorClass) {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public ActorRef actorFor(String actorId) {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public <T> MessageSerializer<T> getSerializer(Class<T> messageClass) {
        return configuration.getSerializer(messageClass);
    }

    @Override
    public <T> MessageDeserializer<T> getDeserializer(Class<T> messageClass) {
        return configuration.getDeserializer(messageClass);
    }

    @Override
    public Serializer<ActorState, byte[]> getActorStateSerializer() {
        return configuration.getActorStateSerializer();
    }

    @Override
    public Deserializer<byte[], ActorState> getActorStateDeserializer() {
        return configuration.getActorStateDeserializer();
    }

    @Autowired
    public void setNodeSelectorFactory(NodeSelectorFactory nodeSelectorFactory) {
        this.nodeSelectorFactory = nodeSelectorFactory;
    }

    @Autowired
    public void setLocalMessageQueueFactory(@Qualifier("localMessageQueueFactory") MessageQueueFactory localMessageQueueFactory) {
        this.localMessageQueueFactory = localMessageQueueFactory;
    }

    @Autowired
    public void setRemoteMessageQueueFactory(@Qualifier("remoteMessageQueueFactory") MessageQueueFactory remoteMessageQueueFactory) {
        this.remoteMessageQueueFactory = remoteMessageQueueFactory;
    }

    private final class ActorShardAdapter implements ActorShard {
        private final ShardKey key;

        private ActorShardAdapter(ShardKey key) {
            this.key = key;
        }

        @Override
        public ShardKey getKey() {
            return key;
        }

        @Override
        public PhysicalNode getOwningNode() {
            final Lock readLock = shardLocks[key.getShardId()].readLock();
            try {
                readLock.lock();
                return shards[key.getShardId()].getOwningNode();
            } finally {
                readLock.unlock();
            }

        }

        @Override
        public void sendMessage(ActorRef sender, ActorRef receiver, Object message) throws Exception {
            final Lock readLock = shardLocks[key.getShardId()].readLock();
            try {
                readLock.lock();
                shards[key.getShardId()].sendMessage(sender,receiver,message);
            } finally {
                readLock.unlock();
            }
        }

        @Override
        public void init() throws Exception {
            // should not be called on the adapter, just do nothing
        }

        @Override
        public void destroy() {
            // should not be called on the adapter, just do nothing
        }
    }


}
