/*
 * Copyright 2013 - 2014 The Original Authors
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
import com.google.common.collect.HashMultimap;
import com.google.common.collect.ImmutableListMultimap;
import com.google.common.collect.SetMultimap;
import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsoftware.elasticactors.*;
import org.elasticsoftware.elasticactors.cache.NodeActorCacheManager;
import org.elasticsoftware.elasticactors.cache.ShardActorCacheManager;
import org.elasticsoftware.elasticactors.cluster.scheduler.InternalScheduler;
import org.elasticsoftware.elasticactors.cluster.scheduler.SchedulerService;
import org.elasticsoftware.elasticactors.messaging.InternalMessage;
import org.elasticsoftware.elasticactors.messaging.MessageQueueFactory;
import org.elasticsoftware.elasticactors.messaging.internal.ActivateActorMessage;
import org.elasticsoftware.elasticactors.messaging.internal.ActorType;
import org.elasticsoftware.elasticactors.messaging.internal.CreateActorMessage;
import org.elasticsoftware.elasticactors.messaging.internal.DestroyActorMessage;
import org.elasticsoftware.elasticactors.scheduler.Scheduler;
import org.elasticsoftware.elasticactors.serialization.Message;
import org.elasticsoftware.elasticactors.serialization.MessageDeserializer;
import org.elasticsoftware.elasticactors.serialization.MessageSerializer;
import org.elasticsoftware.elasticactors.serialization.SerializationFramework;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;

import javax.annotation.Nullable;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Function;

import static java.lang.String.format;
import static java.util.stream.Collectors.counting;
import static java.util.stream.Collectors.groupingBy;
import static org.elasticsoftware.elasticactors.cluster.ActorSystemEvent.ACTOR_SHARD_INITIALIZED;

/**
 * @author Joost van de Wijgerd
 */
public final class LocalActorSystemInstance implements InternalActorSystem {
    private static final Logger logger = LogManager.getLogger(LocalActorSystemInstance.class);
    private final InternalActorSystemConfiguration configuration;
    private final ActorShard[] shards;
    private final ReadWriteLock[] shardLocks;
    private final ActorShardAdapter[] shardAdapters;
    private final NodeSelectorFactory nodeSelectorFactory;
    private final ConcurrentMap<Class, ElasticActor> actorInstances = new ConcurrentHashMap<Class, ElasticActor>();
    private final InternalActorSystems cluster;
    private MessageQueueFactory localMessageQueueFactory;
    private MessageQueueFactory remoteMessageQueueFactory;
    private SchedulerService scheduler;
    private ActorSystemEventListenerService actorSystemEventListenerService;
    private NodeActorCacheManager nodeActorCacheManager;
    private ShardActorCacheManager shardActorCacheManager;
    private ActorLifecycleListenerRegistry actorLifecycleListenerRegistry;
    private final AtomicBoolean initialized = new AtomicBoolean(false);
    private final ConcurrentMap<String, ActorNode> activeNodes = new ConcurrentHashMap<>();
    private final ConcurrentMap<String, ActorNodeAdapter> activeNodeAdapters = new ConcurrentHashMap<>();
    private final ActorNodeAdapter localNodeAdapter;
    private final HashFunction hashFunction = Hashing.murmur3_32();

    public LocalActorSystemInstance(PhysicalNode localNode, InternalActorSystems cluster, InternalActorSystemConfiguration configuration, NodeSelectorFactory nodeSelectorFactory) {
        this.configuration = configuration;
        this.nodeSelectorFactory = nodeSelectorFactory;
        this.cluster = cluster;
        this.shards = new ActorShard[configuration.getNumberOfShards()];
        this.shardLocks = new ReadWriteLock[shards.length];
        this.shardAdapters = new ActorShardAdapter[shards.length];
        for (int i = 0; i < shards.length; i++) {
            shardLocks[i] = new ReentrantReadWriteLock();
            shardAdapters[i] = new ActorShardAdapter(new ShardKey(configuration.getName(), i));
        }
        this.localNodeAdapter = new ActorNodeAdapter(new NodeKey(configuration.getName(), localNode.getId()));
    }

    @Override
    public String toString() {
        return format("%s[%s]", configuration.getClass(), getName());
    }

    @Override
    public InternalActorSystems getParent() {
        return cluster;
    }

    @Override
    public InternalActorSystemConfiguration getConfiguration() {
        return configuration;
    }

    public void shutdown() {
        // The Messaging subsystem is closed before this instance
        // Need to sort out the order
        /*
        logger.info(format("Shutting down %d ActorNode instances",activeNodes.size()));
        for (ActorNode node : activeNodes.values()) {
            node.destroy();
        }
        logger.info(format("Shutting down %d ActorShards",shards.length));
        for (ActorShard shard : shards) {
            shard.destroy();
        }
        */
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
    public void distributeShards(List<PhysicalNode> nodes,ShardDistributionStrategy strategy) throws Exception {
        final boolean initializing = initialized.compareAndSet(false, true);
        // see if this was the first time, if so we need to initialize the ActorSystem
        if (initializing) {
            logger.info(format("Initializing ActorSystem [%s]", getName()));
        }

        NodeSelector nodeSelector = nodeSelectorFactory.create(nodes);
        // fetch all writelocks
        final Lock[] writeLocks = new Lock[shardLocks.length];
        for (int j = 0; j < shardLocks.length; j++) {
            writeLocks[j] = shardLocks[j].writeLock();
        }
        // store the id's of the new local shard in order to generate the events later
        final List<Integer> newLocalShards = new ArrayList<>(shards.length);
        // this is for reporting the number of shards per node
        final List<String> nodeCount = new ArrayList<>(shards.length);

        try {
            for (Lock writeLock : writeLocks) {
                writeLock.lock();
            }

            for (int i = 0; i < configuration.getNumberOfShards(); i++) {
                ShardKey shardKey = new ShardKey(configuration.getName(), i);
                PhysicalNode node = nodeSelector.getPrimary(shardKey.toString());
                nodeCount.add(node.getId());
                if (node.isLocal()) {
                    // this instance should start owning the shard now
                    final ActorShard currentShard = shards[i];
                    if (currentShard == null || !currentShard.getOwningNode().isLocal()) {
                        String owningNodeId = currentShard != null ? currentShard.getOwningNode().getId() : "<No Node>";
                        logger.info(format("I will own %s", shardKey.toString()));
                        // destroy the current remote shard instance
                        if (currentShard != null) {
                            currentShard.destroy();
                        }
                        // create a new local shard and swap it
                        LocalActorShard newShard = new LocalActorShard(node,
                                this, i, shardAdapters[i].myRef, localMessageQueueFactory, shardActorCacheManager);

                        shards[i] = newShard;
                        try {
                            // register with the strategy to wait for shard to be released
                            strategy.registerWaitForRelease(newShard, node);
                        } catch(Exception e) {
                            logger.error(format("IMPORTANT: waiting on release of shard %s from node %s failed,  ElasticActors cluster is unstable. Please check all nodes", shardKey, owningNodeId), e);
                        } finally {
                            // add it to the new local shards
                            newLocalShards.add(i);
                            // initialize
                            // newShard.init();
                            // start owning the scheduler shard (this will start sending messages, but everything is blocked so it should be no problem)
                            scheduler.registerShard(newShard.getKey());
                        }
                    } else {
                        // we own the shard already, no change needed
                        logger.info(format("I already own %s", shardKey.toString()));
                    }
                } else {
                    // the shard will be managed by another node
                    final ActorShard currentShard = shards[i];
                    if (currentShard == null || currentShard.getOwningNode().isLocal()) {
                        logger.info(format("%s will own %s", node, shardKey));
                        try {
                            // destroy the current local shard instance
                            if (currentShard != null) {
                                // stop owning the scheduler shard
                                scheduler.unregisterShard(currentShard.getKey());
                                currentShard.destroy();
                                strategy.signalRelease(currentShard, node);
                            }
                        } catch(Exception e) {
                            logger.error(format("IMPORTANT: signalling release of shard %s to node %s failed, ElasticActors cluster is unstable. Please check all nodes", shardKey, node), e);
                        } finally {
                            // create a new remote shard and swap it
                            RemoteActorShard newShard = new RemoteActorShard(node, this, i, shardAdapters[i].myRef, remoteMessageQueueFactory);
                            shards[i] = newShard;
                            // initialize
                            newShard.init();
                        }
                    } else {
                        // shard was already remote
                        logger.info(format("%s will own %s", node, shardKey));
                    }
                }
            }
            // now we have released all local shards, wait for the new local shards to become available
            if(!strategy.waitForReleasedShards(10, TimeUnit.SECONDS)) {
                // timeout while waiting for the shards
                // @todo: what to do now?
            }

        } finally {
            // unlock all
            for (Lock writeLock : writeLocks) {
                writeLock.unlock();
            }
        }
        // This needs to happen after we initialize the shards as services expect the system to be initialized and
        // should be allowed to send messages to shards
        if(initializing) {
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
        }
        // print out the shard distribution here
        Map<String, Long> collect = nodeCount.stream().collect(groupingBy(Function.identity(), counting()));
        SortedMap<String, Long> sortedNodes = new TreeMap<>(collect);
        logger.info("Cluster shard mapping summary:");
        for (Map.Entry<String, Long> entry : sortedNodes.entrySet()) {
            logger.info(format("\t%s has %d shards assigned", entry.getKey(), entry.getValue()));
        }
        // now we need to generate the events for the new local shards (if any)
        logger.info(format("Generating ACTOR_SHARD_INITIALIZED events for %d new shards",newLocalShards.size()));
        for (Integer newLocalShard : newLocalShards) {
            this.actorSystemEventListenerService.generateEvents(shardAdapters[newLocalShard], ACTOR_SHARD_INITIALIZED);
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
            return getShard(Integer.parseInt(pathElements[2]));
        } else {
            throw new IllegalArgumentException(format("No ActorShard found for actorPath [%s]", actorPath));
        }
    }

    @Override
    public ActorShard getShard(int shardId) {
        return shardAdapters[shardId];
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
                logger.error(format("Exception creating actor instance for actorClass [%s]",actorClass.getName()), e);
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
    public <T> MessageSerializer<T> getSerializer(Class<T> messageClass) {
        MessageSerializer<T> messageSerializer = cluster.getSystemMessageSerializer(messageClass);
        if(messageSerializer == null) {
            Message messageAnnotation = messageClass.getAnnotation(Message.class);
            if(messageAnnotation != null) {
                SerializationFramework framework = cluster.getSerializationFramework(messageAnnotation.serializationFramework());
                messageSerializer = framework.getSerializer(messageClass);
            }
        }
        return messageSerializer;
    }

    @Override
    public <T> MessageDeserializer<T> getDeserializer(Class<T> messageClass) {
        MessageDeserializer<T> messageDeserializer = cluster.getSystemMessageDeserializer(messageClass);
        if(messageDeserializer == null) {
            Message messageAnnotation = messageClass.getAnnotation(Message.class);
            if(messageAnnotation != null) {
                SerializationFramework framework = cluster.getSerializationFramework(messageAnnotation.serializationFramework());
                messageDeserializer = framework.getDeserializer(messageClass);
            }
        }
        return messageDeserializer;
    }

    @Override
    public String getName() {
        return configuration.getName();
    }

    //@Override
    public String getVersion() {
        return configuration.getVersion();
    }

    @Override
    public Scheduler getScheduler() {
        return scheduler;
    }

    @Override
    public ActorSystemEventListenerRegistry getEventListenerRegistry() {
        return this.actorSystemEventListenerService;
    }

    @Override
    public InternalScheduler getInternalScheduler() {
        return scheduler;
    }

    @Override
    public List<ActorLifecycleListener<?>> getActorLifecycleListeners(Class<? extends ElasticActor> actorClass) {
        return actorLifecycleListenerRegistry.getListeners(actorClass);
    }

    @Override
    public <T> ActorRef actorOf(String actorId, Class<T> actorClass) throws Exception {
        return actorOf(actorId, actorClass, null);
    }

    @Override
    public <T> ActorRef actorOf(String actorId, Class<T> actorClass,@Nullable ActorState initialState) throws Exception {
        // @todo: do a sanity check on the actor class here
        // determine shard
        final ActorShard shard = shardFor(actorId);
        // send CreateActorMessage to shard
        CreateActorMessage createActorMessage = new CreateActorMessage(getName(), actorClass.getName(), actorId, initialState);
        ActorRef creator = ActorContextHolder.getSelf();
        shard.sendMessage(creator, shard.getActorRef(), createActorMessage);
        // create actor ref
        return cluster.createPersistentActorRef(shard, actorId);
    }

    @Override
    public <T> ActorRef tempActorOf(Class<T> actorClass, ActorState initialState) throws Exception {
        // if we have state we need to wrap it
        String actorId = UUID.randomUUID().toString();
        CreateActorMessage createActorMessage = new CreateActorMessage(getName(),
                                                                       actorClass.getName(),
                                                                       actorId,
                                                                       initialState,
                                                                       ActorType.TEMP);
        this.localNodeAdapter.sendMessage(null, localNodeAdapter.getActorRef(), createActorMessage);
        return cluster.createTempActorRef(localNodeAdapter, actorId);
    }

    private ActorShard shardFor(String actorId) {
        return shardAdapters[Math.abs(hashFunction.hashString(actorId, Charsets.UTF_8).asInt()) % shards.length];
    }


    @Override
    public ActorRef actorFor(final String actorId) {
        // determine shard
        final ActorShard shard = shardFor(actorId);
        // return actor ref
        return cluster.createPersistentActorRef(shard, actorId);
    }

    @Override
    public ActorRef tempActorFor(String actorId) {
        return cluster.createTempActorRef(this.localNodeAdapter, actorId);
    }

    @Override
    public ActorRef serviceActorFor(String actorId) {
        return cluster.createServiceActorRef(this.localNodeAdapter, actorId);
    }

    @Override
    public ActorRef serviceActorFor(String nodeId, String actorId) {
        final ActorNodeAdapter nodeAdapter = this.activeNodeAdapters.get(nodeId);
        if(nodeAdapter != null) {
            return cluster.createServiceActorRef(nodeAdapter, actorId);
        } else {
            throw new IllegalArgumentException(format("Unknown node [%s]",nodeId));
        }
    }

    @Override
    public ActorRefGroup groupOf(ActorRef... members) throws IllegalArgumentException {
        // all members have to be persistent actor refs
        for (ActorRef member : members) {
            if(!(member instanceof ActorShardRef)) {
                throw new IllegalArgumentException("Only Persistent Actors (annotated with @Actor) of the same ElasticActors cluster are allowed to form a group");
            }
        }
        // build the map
        ImmutableListMultimap.Builder<ActorShardRef, ActorRef> memberMap = ImmutableListMultimap.builder();
        for (ActorRef member : members) {
            memberMap.put((ActorShardRef)((ActorShardRef)member).getActorContainer().getActorRef(), member);
        }

        return new LocalActorRefGroup(memberMap.build());
    }

    @Override
    public void stop(ActorRef actorRef) throws Exception {
        // set sender if we have any in the current context
        ActorRef sender = ActorContextHolder.getSelf();
        ActorContainer handlingContainer = ((ActorContainerRef) actorRef).getActorContainer();
        handlingContainer.sendMessage(sender, handlingContainer.getActorRef(), new DestroyActorMessage(actorRef));
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
    public void setScheduler(SchedulerService scheduler) {
        this.scheduler = scheduler;
    }

    @Autowired
    public void setActorSystemEventListenerService(ActorSystemEventListenerService actorSystemEventListenerService) {
        this.actorSystemEventListenerService = actorSystemEventListenerService;
    }

    @Autowired
    public void setNodeActorCacheManager(NodeActorCacheManager nodeActorCacheManager) {
        this.nodeActorCacheManager = nodeActorCacheManager;
    }

    @Autowired
    public void setShardActorCacheManager(ShardActorCacheManager shardActorCacheManager) {
        this.shardActorCacheManager = shardActorCacheManager;
    }

    @Autowired
    public void setActorLifecycleListenerRegistry(ActorLifecycleListenerRegistry actorLifecycleListenerRegistry) {
        this.actorLifecycleListenerRegistry = actorLifecycleListenerRegistry;
    }

    private final class ActorShardAdapter implements ActorShard {
        private final ShardKey key;
        private final ActorRef myRef;

        private ActorShardAdapter(ShardKey key) {
            this.key = key;
            this.myRef = new ActorShardRef(cluster.getClusterName(), this);
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
        public void sendMessage(ActorRef sender, List<? extends ActorRef> receivers, Object message) throws Exception {
            final Lock readLock = shardLocks[key.getShardId()].readLock();
            try {
                readLock.lock();
                shards[key.getShardId()].sendMessage(sender, receivers, message);
            } finally {
                readLock.unlock();
            }
        }

        @Override
        public void undeliverableMessage(InternalMessage message, ActorRef receiverRef) throws Exception {
            final Lock readLock = shardLocks[key.getShardId()].readLock();
            try {
                readLock.lock();
                shards[key.getShardId()].undeliverableMessage(message, receiverRef);
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
        public void sendMessage(ActorRef sender, List<? extends ActorRef> receivers, Object message) throws Exception {
            // @todo: check if we need to lock here like with ActorShards
            activeNodes.get(key.getNodeId()).sendMessage(sender, receivers, message);
        }

        @Override
        public void undeliverableMessage(InternalMessage message, ActorRef receiverRef) throws Exception {
            activeNodes.get(key.getNodeId()).undeliverableMessage(message, receiverRef);
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

        @Override
        public boolean isLocal() {
            ActorNode node = activeNodes.get(key.getNodeId());
            return node != null && node.isLocal();
        }
    }

}
