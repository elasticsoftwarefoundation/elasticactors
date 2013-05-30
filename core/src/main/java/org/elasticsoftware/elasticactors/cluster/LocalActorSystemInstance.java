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

package org.elasticsoftware.elasticactors.cluster;

import org.apache.log4j.Logger;
import org.elasticsoftware.elasticactors.*;
import org.elasticsoftware.elasticactors.cache.NodeActorCacheManager;
import org.elasticsoftware.elasticactors.cache.ShardActorCacheManager;
import org.elasticsoftware.elasticactors.messaging.InternalMessage;
import org.elasticsoftware.elasticactors.messaging.MessageQueueFactory;
import org.elasticsoftware.elasticactors.messaging.internal.ActivateActorMessage;
import org.elasticsoftware.elasticactors.messaging.internal.ActorType;
import org.elasticsoftware.elasticactors.messaging.internal.CreateActorMessage;
import org.elasticsoftware.elasticactors.messaging.internal.DestroyActorMessage;
import org.elasticsoftware.elasticactors.scheduler.Scheduler;
import org.elasticsoftware.elasticactors.serialization.Deserializer;
import org.elasticsoftware.elasticactors.serialization.MessageDeserializer;
import org.elasticsoftware.elasticactors.serialization.MessageSerializer;
import org.elasticsoftware.elasticactors.serialization.Serializer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Configurable;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.core.annotation.AnnotationUtils;
import org.springframework.core.io.ClassPathResource;

import java.math.BigInteger;
import java.nio.charset.Charset;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * @author Joost van de Wijgerd
 */
@Configurable
public final class LocalActorSystemInstance implements InternalActorSystem {
    private static final Logger logger = Logger.getLogger(LocalActorSystemInstance.class);
    private final ActorSystemConfiguration configuration;
    private final ActorShard[] shards;
    private final ReadWriteLock[] shardLocks;
    private final ActorShardAdapter[] shardAdapters;
    private final NodeSelectorFactory nodeSelectorFactory;
    private final ConcurrentMap<Class, ElasticActor> actorInstances = new ConcurrentHashMap<Class, ElasticActor>();
    private final KetamaHashAlgorithm hashAlgorithm = new KetamaHashAlgorithm();
    private final ActorSystems cluster;
    private MessageQueueFactory localMessageQueueFactory;
    private MessageQueueFactory remoteMessageQueueFactory;
    private Scheduler scheduler;
    private NodeActorCacheManager nodeActorCacheManager;
    private ShardActorCacheManager shardActorCacheManager;
    private final AtomicBoolean initialized = new AtomicBoolean(false);
    private final ConcurrentMap<String, ActorNode> activeNodes = new ConcurrentHashMap<String, ActorNode>();
    private final ConcurrentMap<String, ActorNodeAdapter> activeNodeAdapters = new ConcurrentHashMap<String, ActorNodeAdapter>();
    private final ActorNodeAdapter localNodeAdapter;

    public LocalActorSystemInstance(PhysicalNode localNode, ActorSystems cluster, ActorSystemConfiguration actorSystem, NodeSelectorFactory nodeSelectorFactory) {
        this.configuration = actorSystem;
        this.nodeSelectorFactory = nodeSelectorFactory;
        this.cluster = cluster;
        this.shards = new ActorShard[configuration.getNumberOfShards()];
        this.shardLocks = new ReadWriteLock[shards.length];
        this.shardAdapters = new ActorShardAdapter[shards.length];
        for (int i = 0; i < shards.length; i++) {
            shardLocks[i] = new ReentrantReadWriteLock();
            shardAdapters[i] = new ActorShardAdapter(new ShardKey(actorSystem.getName(), i));
        }
        this.localNodeAdapter = new ActorNodeAdapter(new NodeKey(actorSystem.getName(), localNode.getId()));
    }

    @Override
    public String toString() {
        return String.format("%s[%s]", configuration.getClass(), getName());
    }

    @Override
    public ActorSystems getParent() {
        return cluster;
    }

    public void shutdown() {
        // @todo: run shutdown sequences on nodes and shards
        if(configuration instanceof ActorSystemBootstrapper) {
            try {
                ((ActorSystemBootstrapper)configuration).destroy();
            } catch (Exception e) {
                logger.error(String.format("Exception while destroying ActorSystem [%s]",getName()),e);
            }
        }
    }

    public void updateNodes(List<PhysicalNode> nodes) throws Exception {
        // first see if we need to remove nodes
        // make a map
        HashMap<String, PhysicalNode> nodeMap = new HashMap<String, PhysicalNode>();
        for (PhysicalNode node : nodes) {
            nodeMap.put(node.getId(), node);
        }
        // need to find the one that's not there anymore and destroy it
        Iterator<Map.Entry<String, ActorNode>> entryIterator = activeNodes.entrySet().iterator();
        while (entryIterator.hasNext()) {
            Map.Entry<String, ActorNode> actorNodeEntry = entryIterator.next();
            if (!nodeMap.containsKey(actorNodeEntry.getKey())) {
                actorNodeEntry.getValue().destroy();
                entryIterator.remove();
                activeNodeAdapters.remove(actorNodeEntry.getKey());
            }
        }
        // now see if we need to add nodes
        for (PhysicalNode node : nodes) {
            if (!activeNodes.containsKey(node.getId())) {
                if (node.isLocal()) {
                    LocalActorNode localActorNode =
                            new LocalActorNode(node,
                                                this,
                                                localNodeAdapter.myRef,
                                                localMessageQueueFactory,
                                                nodeActorCacheManager);
                    activeNodes.put(node.getId(), localActorNode);
                    activeNodeAdapters.put(node.getId(),localNodeAdapter);
                    localActorNode.init();
                } else {
                    RemoteActorNode remoteActorNode = new RemoteActorNode(node,
                            this,
                            new ActorNodeAdapter(new NodeKey(getName(), node.getId())).myRef,
                            remoteMessageQueueFactory);
                    activeNodes.put(node.getId(), remoteActorNode);
                    activeNodeAdapters.put(node.getId(),new ActorNodeAdapter(remoteActorNode.getKey()));
                    remoteActorNode.init();
                }
            }
        }
    }

    /**
     * Distribute the shards over the list of physical nodes
     *
     * @param nodes
     */
    public void distributeShards(List<PhysicalNode> nodes) throws Exception {

        NodeSelector nodeSelector = nodeSelectorFactory.create(nodes);
        // lock all
        final Lock[] writeLocks = new Lock[shardLocks.length];
        for (int j = 0; j < shardLocks.length; j++) {
            writeLocks[j] = shardLocks[j].writeLock();
        }
        try {
            for (Lock writeLock : writeLocks) {
                writeLock.lock();
            }

            for (int i = 0; i < configuration.getNumberOfShards(); i++) {
                ShardKey shardKey = new ShardKey(configuration.getName(), i);
                PhysicalNode node = nodeSelector.getPrimary(shardKey.toString());
                if (node.isLocal()) {

                    // this instance should start owning the shard now
                    final ActorShard currentShard = shards[i];
                    if (currentShard == null || !currentShard.getOwningNode().isLocal()) {
                        logger.info(String.format("I will own %s", shardKey.toString()));

                        // destroy the current remote shard instance
                        if (currentShard != null) {
                            currentShard.destroy();
                        }
                        // create a new local shard and swap it
                        LocalActorShard newShard = new LocalActorShard(node,
                                this, i, shardAdapters[i].myRef, localMessageQueueFactory, shardActorCacheManager);

                        shards[i] = newShard;
                        // initialize
                        newShard.init();

                    } else {
                        // we own the shard already, no change needed
                        logger.info(String.format("I already own %s", shardKey.toString()));
                    }
                } else {
                    // the shard will be managed by another node
                    final ActorShard currentShard = shards[i];
                    if (currentShard == null || currentShard.getOwningNode().isLocal()) {
                        logger.info(String.format("%s will own %s", node, shardKey));

                        // destroy the current remote shard instance
                        if (currentShard != null) {
                            currentShard.destroy();
                        }
                        // create a new local shard and swap it
                        RemoteActorShard newShard = new RemoteActorShard(node, this, i, shardAdapters[i].myRef, remoteMessageQueueFactory);

                        shards[i] = newShard;
                        // initialize
                        newShard.init();

                    } else {
                        // shard was already remote
                        logger.info(String.format("%s will own %s", node, shardKey));
                    }
                }
            }
        } finally {
            // unlock all
            for (Lock writeLock : writeLocks) {
                writeLock.unlock();
            }
        }
        // see if this was the first time, if so we need to initialize the ActorSystem
        if (initialized.compareAndSet(false, true)) {
            logger.info(String.format("Initializing ActorSystem [%s]", getName()));
            ActorSystemBootstrapper bootstrapper = null;
            if (configuration instanceof ActorSystemBootstrapper) {
                logger.info(String.format("Bootstrapping ActorSystem [%s]", getName()));
                bootstrapper = (ActorSystemBootstrapper) configuration;
            }
            if (bootstrapper != null) {
                try {
                    Properties p = new Properties();
                    // load any properties for this ActorSystem
                    ClassPathResource runtimeProps = new ClassPathResource(String.format("%s.properties",configuration.getName()));
                    if(runtimeProps.exists()) {
                        p.load(runtimeProps.getInputStream());
                    } else {
                        ClassPathResource defaultProps = new ClassPathResource(String.format("%s-default.properties",configuration.getName()));
                        if(defaultProps.exists()) {
                            p.load(defaultProps.getInputStream());
                        }
                    }
                    bootstrapper.initialize(this, p);
                } catch (Exception e) {
                    // @todo: we should probably abort here
                    logger.error(String.format("Exception while initializing ActorSystem [%s]", getName()), e);
                }
            }

            // initialize the services
            Set<String> serviceActors = configuration.getServices();
            if (serviceActors != null && !serviceActors.isEmpty()) {
                // initialize the service actors in the context
                for (String elasticActorEntry : serviceActors) {
                    localNodeAdapter.sendMessage(null,
                            localNodeAdapter.myRef,
                            new ActivateActorMessage(getName(), elasticActorEntry, ActorType.SERVICE));
                }
            }
            if (bootstrapper != null) {
                // @todo: we should only do this on original creation of the ActorSystem, not when reloading
                try {
                    bootstrapper.create(this);
                } catch (Exception e) {
                    // @todo: we should probably abort here
                    logger.error(String.format("Exception while creating ActorSystem [%s]", getName()), e);
                }
                try {
                    bootstrapper.activate(this);
                } catch (Exception e) {
                    // @todo: we should probably abort here
                    logger.error(String.format("Exception while activating ActorSystem [%s]", getName()), e);
                }
            }

        }
    }

    public int getNumberOfShards() {
        return configuration.getNumberOfShards();
    }

    @Override
    public ActorShard getShard(String actorPath) {
        // for now we support only <ActorSystemName>/shards/<shardId>
        // @todo: do this with actorRef tools
        String[] pathElements = actorPath.split("/");
        if (pathElements[1].equals("shards")) {
            return shardAdapters[Integer.parseInt(pathElements[2])];
        } else {
            throw new IllegalArgumentException(String.format("No ActorShard found for actorPath [%s]", actorPath));
        }
    }

    @Override
    public ActorNode getNode(String nodeId) {
        return activeNodeAdapters.get(nodeId);
    }

    @Override
    public ActorNode getNode() {
        return this.localNodeAdapter;
    }

    @Override
    public ElasticActor getActorInstance(ActorRef actorRef, Class<? extends ElasticActor> actorClass) {
        // ensure the actor instance is created
        ElasticActor actorInstance = actorInstances.get(actorClass);
        if (actorInstance == null) {
            try {
                actorInstance = actorClass.newInstance();
                ElasticActor existingInstance = actorInstances.putIfAbsent(actorClass, actorInstance);
                return existingInstance == null ? actorInstance : existingInstance;
            } catch (Exception e) {
                logger.error("Exception creating actor instance", e);
                return null;
            }
        } else {
            return actorInstance;
        }
    }

    @Override
    public ElasticActor getServiceInstance(ActorRef serviceRef) {
        if (ActorRefTools.isService(serviceRef)) {
            return configuration.getService(serviceRef.getActorId());
        }
        return null;
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
        return actorOf(actorId, actorClass, null);
    }

    @Override
    public <T> ActorRef actorOf(String actorId, Class<T> actorClass, Object initialState) throws Exception {
        // determine shard
        ActorShard shard = shardFor(actorId);
        // if we have state we need to wrap it
        ActorState actorState = initialState != null ? configuration.getActorStateFactory().create(initialState) : null;
        // send CreateActorMessage to shard
        CreateActorMessage createActorMessage = new CreateActorMessage(getName(), actorClass.getName(), actorId, actorState);
        //@todo: see if we need to get the sender from the context
        shard.sendMessage(null, shard.getActorRef(), createActorMessage);
        // create actor ref
        // @todo: add cache to speed up performance
        return new LocalClusterActorShardRef(cluster.getClusterName(), shard, actorId);
    }

    @Override
    public <T> ActorRef tempActorOf(Class<T> actorClass, Object initialState) throws Exception {
        // if we have state we need to wrap it
        ActorState actorState = initialState != null ? configuration.getActorStateFactory().create(initialState) : null;
        String actorId = UUID.randomUUID().toString();
        CreateActorMessage createActorMessage = new CreateActorMessage(getName(),
                                                                       actorClass.getName(),
                                                                       actorId,
                                                                       actorState,
                                                                       ActorType.TEMP);
        this.localNodeAdapter.sendMessage(null, localNodeAdapter.getActorRef(), createActorMessage);
        return new LocalClusterActorNodeRef(cluster.getClusterName(), localNodeAdapter, actorId);
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
        return new LocalClusterActorShardRef(cluster.getClusterName(), shard, actorId);
    }

    @Override
    public ActorRef tempActorFor(String actorId) {
        return new LocalClusterActorNodeRef(cluster.getClusterName(), this.localNodeAdapter, actorId);
    }

    @Override
    public ActorRef serviceActorFor(String actorId) {
        return new ServiceActorRef(cluster.getClusterName(), this.localNodeAdapter, actorId);
    }

    @Override
    public void stop(ActorRef actorRef) throws Exception {
        // set sender if we have any in the current context
        ActorRef sender = ActorContextHolder.getSelf();
        ActorContainer handlingContainer = ((ActorContainerRef) actorRef).get();
        handlingContainer.sendMessage(sender, handlingContainer.getActorRef(), new DestroyActorMessage(actorRef));
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

    @Override
    public ActorStateFactory getActorStateFactory() {
        return configuration.getActorStateFactory();
    }

    @Override
    public ElasticActor getService(String serviceId) {
        return configuration.getService(serviceId);
    }

    @Override
    public Set<String> getServices() {
        return configuration.getServices();
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

    @Autowired
    public void setNodeActorCacheManager(NodeActorCacheManager nodeActorCacheManager) {
        this.nodeActorCacheManager = nodeActorCacheManager;
    }

    @Autowired
    public void setShardActorCacheManager(ShardActorCacheManager shardActorCacheManager) {
        this.shardActorCacheManager = shardActorCacheManager;
    }

    @Override
    public List<String> getDependencies() {
        DependsOn dependsOn = AnnotationUtils.findAnnotation(configuration.getClass(), DependsOn.class);
        if (dependsOn != null) {
            return Arrays.<String>asList(dependsOn.dependencies());
        } else {
            return Collections.<String>emptyList();
        }
    }

    private final class ActorShardAdapter implements ActorShard {
        private final ShardKey key;
        private final ActorRef myRef;

        private ActorShardAdapter(ShardKey key) {
            this.key = key;
            this.myRef = new LocalClusterActorShardRef(cluster.getClusterName(), this);
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
                shards[key.getShardId()].sendMessage(sender, receiver, message);
            } finally {
                readLock.unlock();
            }
        }

        @Override
        public void undeliverableMessage(InternalMessage message) throws Exception {
            final Lock readLock = shardLocks[key.getShardId()].readLock();
            try {
                readLock.lock();
                shards[key.getShardId()].undeliverableMessage(message);
            } finally {
                readLock.unlock();
            }
        }

        @Override
        public void offerInternalMessage(InternalMessage message) {
            final Lock readLock = shardLocks[key.getShardId()].readLock();
            try {
                readLock.lock();
                shards[key.getShardId()].offerInternalMessage(message);
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

    private final class ActorNodeAdapter implements ActorNode {
        private final NodeKey key;
        private final ActorRef myRef;

        private ActorNodeAdapter(NodeKey key) {
            this.key = key;
            this.myRef = new LocalClusterActorNodeRef(cluster.getClusterName(), this);
        }

        @Override
        public NodeKey getKey() {
            return key;
        }

        @Override
        public ActorRef getActorRef() {
            return myRef;
        }

        @Override
        public void sendMessage(ActorRef sender, ActorRef receiver, Object message) throws Exception {
            // @todo: check if we need to lock here like with ActorShards
            activeNodes.get(key.getNodeId()).sendMessage(sender, receiver, message);
        }

        @Override
        public void undeliverableMessage(InternalMessage message) throws Exception {
            activeNodes.get(key.getNodeId()).undeliverableMessage(message);
        }

        @Override
        public void offerInternalMessage(InternalMessage message) {
            activeNodes.get(key.getNodeId()).offerInternalMessage(message);
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
         *
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
            if (md5 == null) {
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
