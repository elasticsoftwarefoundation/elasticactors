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
import org.elasterix.elasticactors.messaging.internal.CreateActorMessage;
import org.elasterix.elasticactors.scheduler.Scheduler;
import org.elasterix.elasticactors.serialization.Deserializer;
import org.elasterix.elasticactors.serialization.MessageDeserializer;
import org.elasterix.elasticactors.serialization.MessageSerializer;
import org.elasterix.elasticactors.serialization.Serializer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Configurable;
import org.springframework.beans.factory.annotation.Qualifier;

import java.math.BigInteger;
import java.nio.charset.Charset;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * @author Joost van de Wijgerd
 */
@Configurable
public final class LocalActorSystemInstance implements InternalActorSystem {
    private static final Logger log = Logger.getLogger(LocalActorSystemInstance.class);
    private final ActorSystemConfiguration configuration;
    private final ActorShard[] shards;
    private final ReadWriteLock[] shardLocks;
    private final ActorShardAdapter[] shardAdapters;
    private final NodeSelectorFactory nodeSelectorFactory;
    private final ConcurrentMap<Class,ElasticActor> actorInstances = new ConcurrentHashMap<Class,ElasticActor>();
    private final KetamaHashAlgorithm hashAlgorithm = new KetamaHashAlgorithm();
    private final ActorSystems cluster;
    private MessageQueueFactory localMessageQueueFactory;
    private MessageQueueFactory remoteMessageQueueFactory;
    private Scheduler scheduler;

    public LocalActorSystemInstance(ActorSystems cluster,ActorSystemConfiguration actorSystem, NodeSelectorFactory nodeSelectorFactory) {
        this.configuration = actorSystem;
        this.nodeSelectorFactory = nodeSelectorFactory;
        this.cluster = cluster;
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
    public ElasticActor getActorInstance(ActorRef actorRef,Class<? extends ElasticActor> actorClass) {
        // ensure the actor instance is created
        ElasticActor actorInstance = actorInstances.get(actorClass);
        if(actorInstance == null) {
            try {
                actorInstance = actorClass.newInstance();
                ElasticActor existingInstance = actorInstances.putIfAbsent(actorClass, actorInstance);
                return existingInstance == null ? actorInstance : existingInstance;
            } catch(Exception e) {
                //@todo: throw a proper exception here
                return null;
            }
        } else {
            return actorInstance;
        }
    }

    @Override
    public String getName() {
        return configuration.getName();
    }

    @Override
    public String getVersion() {
        return configuration.getVersion();
    }

    @Override
    public Scheduler getScheduler() {
        return scheduler;
    }

    @Override
    public <T> ActorRef actorOf(String actorId, Class<T> actorClass) throws Exception {
        return actorOf(actorId,actorClass,null,true);
    }

    @Override
    public <T> ActorRef actorOf(String actorId, Class<T> actorClass, ActorState initialState) throws Exception {
        return actorOf(actorId,actorClass,initialState,true);
    }

    @Override
    public <T> ActorRef actorOf(String actorId, Class<T> actorClass, ActorState initialState, boolean persistent) throws Exception {
        // determine shard
        ActorShard shard = shardFor(actorId);
        // send CreateActorMessage to shard
        CreateActorMessage createActorMessage = new CreateActorMessage(actorClass.getName(),actorId, null);
        //@todo: see if we need to get the sender from the context
        shard.sendMessage(null,shard.getActorRef(),createActorMessage);
        // create actor ref
        // @todo: add cache to speed up performance
        return new LocalClusterActorRef(cluster.getClusterName(),shard,actorId);
    }

    private ActorShard shardFor(String actorId) {
        return shardAdapters[hashAlgorithm.hash(actorId).mod(BigInteger.valueOf(shards.length)).intValue()];
    }

    @Override
    public ActorRef actorFor(String actorId) {
        // determine shard
        ActorShard shard = shardFor(actorId);
        // return actor ref
        // @todo: add cache to speed up performance
        return new LocalClusterActorRef(cluster.getClusterName(),shard,actorId);
    }

    @Override
    public <T> MessageSerializer<T> getSerializer(Class<T> messageClass) {
        MessageSerializer<T> messageSerializer = cluster.getSystemMessageSerializer(messageClass);
        return messageSerializer == null ? configuration.getSerializer(messageClass) : messageSerializer;
    }

    @Override
    public <T> MessageDeserializer<T> getDeserializer(Class<T> messageClass) {
        MessageDeserializer<T> messageDeserializer = cluster.getSystemMessageDeserializer(messageClass);
        return messageDeserializer == null ? configuration.getDeserializer(messageClass) : messageDeserializer;
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
    public void setLocalMessageQueueFactory(@Qualifier("localMessageQueueFactory") MessageQueueFactory localMessageQueueFactory) {
        this.localMessageQueueFactory = localMessageQueueFactory;
    }

    @Autowired
    public void setRemoteMessageQueueFactory(@Qualifier("remoteMessageQueueFactory") MessageQueueFactory remoteMessageQueueFactory) {
        this.remoteMessageQueueFactory = remoteMessageQueueFactory;
    }

    @Autowired
    public void setScheduler(Scheduler scheduler) {
        this.scheduler = scheduler;
    }

    private final class ActorShardAdapter implements ActorShard {
        private final ShardKey key;
        private final ActorRef myRef;

        private ActorShardAdapter(ShardKey key) {
            this.key = key;
            this.myRef = new LocalClusterActorRef(cluster.getClusterName(),this);
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
        public ActorRef getActorRef() {
            return myRef;
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

    public static final class KetamaHashAlgorithm {
        private static final Charset UTF_8 = Charset.forName("UTF-8");
    	private final ConcurrentLinkedQueue<MessageDigest> digestCache = new ConcurrentLinkedQueue<MessageDigest>();

        /**
    	 * Compute the hash for the given key.
    	 *
    	 * @param k the key to hash
         * @return a positive integer hash of 128 bits
    	 */
        public BigInteger hash(final String k) {
            return new BigInteger(computeMd5(k)).abs();
        }

        /**
    	 * Get the md5 of the given key.
         * @param k the key to compute an MD5 on
         * @return an MD5 hash
         */
        public byte[] computeMd5(String k) {
    		MessageDigest md5 = borrow();
    		try {
    			md5.reset();
    			md5.update(k.getBytes(UTF_8));
    			return md5.digest();
    		} finally {
    			release(md5);
    		}
    	}

    	private MessageDigest borrow() {
    		MessageDigest md5 = digestCache.poll();
    		if(md5 == null) {
    			try {
    				md5 = MessageDigest.getInstance("MD5");
    			} catch (NoSuchAlgorithmException e) {
    				throw new RuntimeException("MD5 not supported", e);
    			}
    		}
    		return md5;
    	}

    	private void release(MessageDigest digest) {
    		digestCache.offer(digest);
    	}
    }


}
