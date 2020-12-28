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

package org.elasticsoftware.elasticactors.kafka;

import com.google.common.cache.Cache;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.ImmutableListMultimap;
import com.google.common.collect.Multimap;
import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;
import org.elasticsoftware.elasticactors.Actor;
import org.elasticsoftware.elasticactors.ActorContainer;
import org.elasticsoftware.elasticactors.ActorContainerRef;
import org.elasticsoftware.elasticactors.ActorContextHolder;
import org.elasticsoftware.elasticactors.ActorLifecycleListener;
import org.elasticsoftware.elasticactors.ActorLifecycleListenerRegistry;
import org.elasticsoftware.elasticactors.ActorNode;
import org.elasticsoftware.elasticactors.ActorRef;
import org.elasticsoftware.elasticactors.ActorRefGroup;
import org.elasticsoftware.elasticactors.ActorShard;
import org.elasticsoftware.elasticactors.ActorState;
import org.elasticsoftware.elasticactors.ElasticActor;
import org.elasticsoftware.elasticactors.InternalActorSystemConfiguration;
import org.elasticsoftware.elasticactors.MethodActor;
import org.elasticsoftware.elasticactors.PhysicalNode;
import org.elasticsoftware.elasticactors.ShardKey;
import org.elasticsoftware.elasticactors.SingletonActor;
import org.elasticsoftware.elasticactors.TempActor;
import org.elasticsoftware.elasticactors.cache.NodeActorCacheManager;
import org.elasticsoftware.elasticactors.cache.ShardActorCacheManager;
import org.elasticsoftware.elasticactors.cluster.ActorRefFactory;
import org.elasticsoftware.elasticactors.cluster.ActorRefTools;
import org.elasticsoftware.elasticactors.cluster.ActorShardRef;
import org.elasticsoftware.elasticactors.cluster.ActorSystemEvent;
import org.elasticsoftware.elasticactors.cluster.ActorSystemEventListenerImpl;
import org.elasticsoftware.elasticactors.cluster.ActorSystemEventListenerRegistry;
import org.elasticsoftware.elasticactors.cluster.InternalActorSystem;
import org.elasticsoftware.elasticactors.cluster.InternalActorSystems;
import org.elasticsoftware.elasticactors.cluster.LocalActorRefGroup;
import org.elasticsoftware.elasticactors.cluster.NodeSelector;
import org.elasticsoftware.elasticactors.cluster.NodeSelectorFactory;
import org.elasticsoftware.elasticactors.cluster.ShardDistributionStrategy;
import org.elasticsoftware.elasticactors.cluster.ShardDistributor;
import org.elasticsoftware.elasticactors.cluster.scheduler.InternalScheduler;
import org.elasticsoftware.elasticactors.kafka.cluster.KafkaInternalActorSystems;
import org.elasticsoftware.elasticactors.kafka.scheduler.KafkaTopicScheduler;
import org.elasticsoftware.elasticactors.kafka.state.PersistentActorStoreFactory;
import org.elasticsoftware.elasticactors.kafka.utils.TopicHelper;
import org.elasticsoftware.elasticactors.logging.LogLevel;
import org.elasticsoftware.elasticactors.messaging.internal.ActorType;
import org.elasticsoftware.elasticactors.messaging.internal.CreateActorMessage;
import org.elasticsoftware.elasticactors.messaging.internal.DestroyActorMessage;
import org.elasticsoftware.elasticactors.runtime.ElasticActorsNode;
import org.elasticsoftware.elasticactors.scheduler.Scheduler;
import org.elasticsoftware.elasticactors.serialization.Deserializer;
import org.elasticsoftware.elasticactors.serialization.Message;
import org.elasticsoftware.elasticactors.serialization.MessageDeserializer;
import org.elasticsoftware.elasticactors.serialization.MessageSerializer;
import org.elasticsoftware.elasticactors.serialization.SerializationFramework;
import org.elasticsoftware.elasticactors.serialization.Serializer;
import org.elasticsoftware.elasticactors.state.PersistentActor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

import static java.lang.String.format;

public final class KafkaActorSystemInstance implements InternalActorSystem, ShardDistributor, ActorSystemEventListenerRegistry {
    private static final Logger logger = LoggerFactory.getLogger(KafkaActorSystemInstance.class);
    private final InternalActorSystemConfiguration configuration;
    private final NodeSelectorFactory nodeSelectorFactory;
    private final KafkaInternalActorSystems cluster;
    private final PhysicalNode localNode;
    private final ActorRefFactory actorRefFactory;
    private final KafkaActorThread[] shardThreads;
    private final KafkaActorShard[] actorShards;
    private final KafkaActorNode localActorNode;
    private final List<KafkaActorNode> activeNodes = new LinkedList<>();
    private final AtomicBoolean initialized = new AtomicBoolean(false);
    private final AtomicBoolean stable = new AtomicBoolean(false);
    private final ConcurrentMap<Class, ElasticActor> actorInstances = new ConcurrentHashMap<>();
    private final KafkaTopicScheduler schedulerService;
    private final HashFunction hashFunction = Hashing.murmur3_32();
    private final ActorLifecycleListenerRegistry actorLifecycleListenerRegistry;
    private final LogLevel onUnhandledLogLevel;

    public KafkaActorSystemInstance(
            ElasticActorsNode node,
            InternalActorSystemConfiguration configuration,
            NodeSelectorFactory nodeSelectorFactory,
            Integer numberOfShardThreads,
            String bootstrapServers,
            Cache<String, ActorRef> actorRefCache,
            ShardActorCacheManager shardActorCacheManager,
            NodeActorCacheManager nodeActorCacheManager,
            Serializer<PersistentActor<ShardKey>, byte[]> stateSerializer,
            Deserializer<byte[], PersistentActor<ShardKey>> stateDeserializer,
            ActorLifecycleListenerRegistry actorLifecycleListenerRegistry,
            PersistentActorStoreFactory persistentActorStoreFactory,
            LogLevel onUnhandledLogLevel) {
        this.actorLifecycleListenerRegistry = actorLifecycleListenerRegistry;
        this.schedulerService = new KafkaTopicScheduler(this);
        this.localNode = node;
        // we need a wrapper around the default implementation that adds the partition for the node topics
        this.cluster = new KafkaInternalActorSystems(node, actorRefCache);
        this.configuration = configuration;
        this.nodeSelectorFactory = nodeSelectorFactory;
        this.actorRefFactory = cluster;
        this.actorShards = new KafkaActorShard[configuration.getNumberOfShards()];
        this.shardThreads = new KafkaActorThread[numberOfShardThreads];
        // make sure all the topics exist and are properly configured before staring the system
        try {
            TopicHelper.ensureTopicsExists(bootstrapServers, node.getId(), numberOfShardThreads, this);
        } catch(Exception e) {
            throw new RuntimeException("FATAL Exception on ensureTopicsExist", e);
        }

        for(int i = 0 ; i < numberOfShardThreads ; i++) {
            this.shardThreads[i] = new KafkaActorThread(cluster.getClusterName(), bootstrapServers, localNode.getId(),
                    this, actorRefFactory, shardActorCacheManager, nodeActorCacheManager, stateSerializer,
                    stateDeserializer, persistentActorStoreFactory);
        }
        for(int i = 0 ; i < configuration.getNumberOfShards() ; i++) {
            this.actorShards[i] = new KafkaActorShard(new ShardKey(configuration.getName(), i),
                    this.shardThreads[i % numberOfShardThreads], this);
        }

        // add the local node to the first shard as primary
        this.localActorNode = new KafkaActorNode(localNode, this.shardThreads[0], this);
        this.activeNodes.add(localActorNode);
        // each KafkaActorThread will have a copy of the ManagedActorNode - all managing one partition
        for (int i = 1; i < shardThreads.length; i++) {
            shardThreads[i].assign(localActorNode, false);
        }
        this.onUnhandledLogLevel = onUnhandledLogLevel;
    }

    @PostConstruct
    public void init() {
        // @todo: start the shard threads here
        for (KafkaActorThread shardThread : shardThreads) {
            shardThread.start();
        }
    }

    @PreDestroy
    public void destroy() {
        for (KafkaActorThread shardThread : shardThreads) {
            shardThread.stopRunning();
        }
    }

    @Override
    public InternalActorSystemConfiguration getConfiguration() {
        return configuration;
    }

    @Override
    public ActorSystemEventListenerRegistry getEventListenerRegistry() {
        return this;
    }

    @Override
    public ActorRefGroup groupOf(Collection<ActorRef> members) throws IllegalArgumentException {
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
    public ActorRef tempActorFor(String actorId) {
        throw new UnsupportedOperationException("KafkaActorSystemInstance does not support tempActorFor logic because node partition cannot be determined");
    }

    @Override
    public ElasticActor getActorInstance(ActorRef actorRef, Class<? extends ElasticActor> actorClass) {
        // ensure the actor instance is created
        return actorInstances.computeIfAbsent(actorClass, k -> {
            try {
                ElasticActor actorInstance = actorClass.newInstance();
                if (actorInstance instanceof MethodActor) {
                    ((MethodActor) actorInstance).setOnUnhandledLogLevel(onUnhandledLogLevel);
                }
                return actorInstance;
            } catch (Exception e) {
                logger.error(
                        "Exception creating actor instance for actorClass [{}]",
                        actorClass.getName(),
                        e);
                return null;
            }
        });
    }

    @Override
    public ElasticActor getServiceInstance(ActorRef serviceRef) {
        if (ActorRefTools.isService(serviceRef)) {
            return configuration.getService(serviceRef.getActorId());
        }
        return null;
    }

    @Override
    public ActorNode getNode(String nodeId) {
        return activeNodes.stream().filter(kafkaActorNode -> kafkaActorNode.getKey().getNodeId().equals(nodeId)).findFirst().orElse(null);
    }

    @Override
    public ActorNode getNode() {
        return localActorNode;
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
    public InternalScheduler getInternalScheduler() {
        throw new UnsupportedOperationException();
    }

    @Override
    public String getName() {
        return configuration.getName();
    }

    @Override
    public <T> ActorRef actorOf(String actorId, Class<T> actorClass) throws Exception {
        if(actorClass.getAnnotation(Actor.class) == null) {
            throw new IllegalArgumentException("actorClass has to be annotated with @Actor");
        }
        SingletonActor singletonActor = actorClass.getAnnotation(SingletonActor.class);
        if (singletonActor != null && !singletonActor.value().equals(actorId)) {
            throw new IllegalArgumentException(String.format(
                    "Diverging ID for SingletonActor. Expected: '%s'. Found: '%s'",
                    singletonActor.value(),
                    actorId));
        }
        return actorOf(actorId, actorClass.getName(), null);
    }

    @Override
    public ActorRef actorOf(String actorId, String actorClassName) throws Exception {
        return actorOf(actorId,actorClassName, null);
    }

    @Override
    public <T> ActorRef actorOf(String actorId, Class<T> actorClass, ActorState initialState) throws Exception {
        if(actorClass.getAnnotation(Actor.class) == null) {
            throw new IllegalArgumentException("actorClass has to be annotated with @Actor");
        }
        SingletonActor singletonActor = actorClass.getAnnotation(SingletonActor.class);
        if (singletonActor != null && !singletonActor.value().equals(actorId)) {
            throw new IllegalArgumentException(String.format(
                    "Diverging ID for SingletonActor. Expected: '%s'. Found: '%s'",
                    singletonActor.value(),
                    actorId));
        }
        return actorOf(actorId, actorClass.getName(), initialState);
    }

    @Override
    public ActorRef actorOf(String actorId, String actorClassName, ActorState initialState) throws Exception {
        return actorOf(actorId, actorClassName, initialState, ActorContextHolder.getSelf());
    }

    @Override
    public ActorRef actorOf(String actorId, String actorClassName, ActorState initialState, ActorRef creator) throws Exception {
        // determine shard
        final KafkaActorShard shard = shardFor(actorId);
        // send CreateActorMessage to shard
        CreateActorMessage createActorMessage = new CreateActorMessage(getName(), actorClassName, actorId, initialState);
        shard.sendMessage(creator, shard.getActorRef(), createActorMessage);
        // create actor ref
        return cluster.createPersistentActorRef(shard, actorId);
    }

    private KafkaActorShard shardFor(String actorId) {
        return this.actorShards[Math.abs(hashFunction.hashString(actorId, StandardCharsets.UTF_8).asInt()) % this.actorShards.length];
    }


    @Override
    public <T> ActorRef tempActorOf(Class<T> actorClass, @Nullable ActorState initialState) throws Exception {
        if(actorClass.getAnnotation(TempActor.class) == null) {
            throw new IllegalArgumentException("actorClass has to be annotated with @TempActor");
        }
        String actorId = UUID.randomUUID().toString();
        // see if we are being called in the context of another actor (and set the affinity key)
        String affinityKey = ActorContextHolder.hasActorContext() ? ActorContextHolder.getSelf().getActorId() : null;
        CreateActorMessage createActorMessage = new CreateActorMessage(getName(),
                actorClass.getName(),
                actorId,
                initialState,
                ActorType.TEMP,
                affinityKey);
        // create this actor on the correct thread (using affinity if that is set)
        KafkaActorThread actorThread = (affinityKey != null) ? shardFor(affinityKey).getActorThread() : shardFor(actorId).getActorThread();
        ActorRef tempActorRef = cluster.createTempActorRef(localActorNode, actorThread.getNodeTopicPartitionId(), actorId);
        actorThread.createTempActor(tempActorRef, createActorMessage);
        return tempActorRef;
    }

    @Override
    public ActorRef actorFor(String actorId) {
        // determine shard
        final ActorShard shard = shardFor(actorId);
        // return actor ref
        return cluster.createPersistentActorRef(shard, actorId);
    }

    @Override
    public ActorRef serviceActorFor(String actorId) {
        return cluster.createServiceActorRef(this.localActorNode, actorId);
    }

    @Override
    public ActorRef serviceActorFor(String nodeId, String actorId) {
        final ActorNode node = getNode(nodeId);
        if(node != null) {
            return cluster.createServiceActorRef(node, actorId);
        } else {
            throw new IllegalArgumentException(format("Unknown node [%s]",nodeId));
        }
    }

    @Override
    public Scheduler getScheduler() {
        return schedulerService;
    }

    @Override
    public InternalActorSystems getParent() {
        return cluster;
    }

    @Override
    public void stop(ActorRef actorRef) throws Exception {
        // set sender if we have any in the current context
        ActorRef sender = ActorContextHolder.getSelf();
        ActorContainer handlingContainer = ((ActorContainerRef) actorRef).getActorContainer();
        handlingContainer.sendMessage(sender, handlingContainer.getActorRef(), new DestroyActorMessage(actorRef));
    }

    @Override
    public List<ActorLifecycleListener<?>> getActorLifecycleListeners(Class<? extends ElasticActor> actorClass) {
        return actorLifecycleListenerRegistry.getListeners(actorClass);
    }

    @Override
    public boolean isStable() {
        return stable.get();
    }

    @Override
    public ActorShard getShard(String actorPath) {
        String[] pathElements = actorPath.split("/");
        if (pathElements[1].equals("shards")) {
            return getShard(Integer.parseInt(pathElements[2]));
        } else {
            throw new IllegalArgumentException(format("No ActorShard found for actorPath [%s]", actorPath));
        }
    }

    @Override
    public ActorShard getShard(int shardId) {
        return this.actorShards[shardId];
    }

    @Override
    public int getNumberOfShards() {
        return configuration.getNumberOfShards();
    }

    @Override
    public void updateNodes(List<PhysicalNode> nodes) throws Exception {
        // first see if we need to remove nodes
        // make a map
        HashMap<String, PhysicalNode> nodeMap = new HashMap<>();
        for (PhysicalNode node : nodes) {
            nodeMap.put(node.getId(), node);
        }

        Set<String> activeNodeIds = activeNodes.stream().map(kafkaActorNode -> kafkaActorNode.getKey().getNodeId()).collect(Collectors.toSet());
        // need to find the one that's not there anymore and destroy it
        Iterator<KafkaActorNode> nodeIterator = this.activeNodes.iterator();
        while(nodeIterator.hasNext()) {
            KafkaActorNode node = nodeIterator.next();
            if (!nodeMap.containsKey(node.getKey().getNodeId())) {
                // not sure if we need to do anything here as this will be a remote node
                node.destroy();
                nodeIterator.remove();
            }
        }
        // find the nodes that are new (not in the active nodes list)
        nodes.stream().filter(physicalNode -> !activeNodeIds.contains(physicalNode.getId()))
                .forEach(physicalNode -> activeNodes.add(new KafkaActorNode(physicalNode, this.shardThreads[activeNodes.size() % this.shardThreads.length], this)));

    }

    @Override
    public void distributeShards(List<PhysicalNode> nodes, ShardDistributionStrategy strategy) throws Exception {
        final boolean initializing = initialized.compareAndSet(false, true);
        // see if this was the first time, if so we need to initialize the ActorSystem
        if (initializing) {
            logger.info("Initializing ActorSystem [{}]", getName());
        }

        NodeSelector nodeSelector = nodeSelectorFactory.create(nodes);

        Multimap<PhysicalNode, ShardKey> shardDistribution = HashMultimap.create();

        // assume we are stable until the resharding process tells us otherwise
        boolean stable = true;

        // find the new distribution
        for (int i = 0; i < configuration.getNumberOfShards(); i++) {
            ShardKey shardKey = new ShardKey(configuration.getName(), i);
            PhysicalNode node = nodeSelector.getPrimary(shardKey.toString());
            shardDistribution.put(node, shardKey);
        }

        CompletionStage<Boolean> result = null;

        for (KafkaActorThread shardThread : shardThreads) {
            if (result == null) {
                result = shardThread.prepareRebalance(shardDistribution, strategy);
            } else {
                result = result.thenCombine(shardThread.prepareRebalance(shardDistribution, strategy), (b1, b2) -> b1 && b2);
            }
        }
        // wait for all to finish computing
        try {
            stable = result.toCompletableFuture().get();
        } catch(ExecutionException e) {
            logger.error("FATAL Exception while executing prepareRebalance operation", e.getCause());
            stable = false;
        } catch(Exception e) {
            logger.error("Unexpected Exception while executing prepareRebalance operation", e);
            stable = false;
        }

        // now we have released all local shards, wait for the new local shards to become available
        if(!strategy.waitForReleasedShards(60, TimeUnit.SECONDS)) {
            // timeout while waiting for the shards
            stable = false;
        }

        Integer newLocalShards = 0;

        // we are good to go
        if(stable) {
            CompletionStage<Integer> performRebalanceResult = null;
            for (KafkaActorThread shardThread : shardThreads) {
                if (performRebalanceResult == null) {
                    performRebalanceResult = shardThread.performRebalance();
                } else {
                    performRebalanceResult = performRebalanceResult.thenCombine(shardThread.performRebalance(), (i1, i2) -> i1 + i2);
                }
            }
            // and wait for completion (this can take a while as state needs to be loaded)
            try {
                newLocalShards = performRebalanceResult.toCompletableFuture().get();
            } catch(ExecutionException e) {
                logger.error("FATAL Exception while executing performRebalance operation", e.getCause());
                stable = false;
            } catch(Exception e) {
                logger.error("Unexpected Exception while executing performRebalancer operation", e);
                stable = false;
            }
        }

        this.stable.set(stable);

        // This needs to happen after we initialize the shards as services expect the system to be initialized and
        // should be allowed to send messages to shards
        if(initializing) {
            // initialize the services
            localActorNode.initializeServiceActors();
        }
        // print out the shard distribution here

        logger.info("Cluster shard mapping summary:");
        for (Map.Entry<PhysicalNode, Collection<ShardKey>> entry : shardDistribution.asMap().entrySet()) {
            logger.info("\t{} has {} shards assigned", entry.getKey(), entry.getValue().size());
        }
    }

    @Override
    public void register(ActorRef receiver, ActorSystemEvent event, Object message) throws IOException {
        if(!(receiver instanceof ActorShardRef)) {
            throw new IllegalArgumentException("ActorRef must be referencing a Persistent Actor (i.e. annotated with @Actor)");
        }
        // get the underlying KafkaActorShard
        KafkaActorShard actorShard = (KafkaActorShard) ((ActorShardRef) receiver).getActorContainer();
        // store the reference
        MessageSerializer serializer = getSerializer(message.getClass());
        ByteBuffer serializedMessage = serializer.serialize(message);
        byte[] serializedBytes = new byte[serializedMessage.remaining()];
        serializedMessage.get(serializedBytes);
        actorShard.getActorThread().register(actorShard.getKey(), event,
                new ActorSystemEventListenerImpl(receiver.getActorId(),message.getClass(),serializedBytes));
    }

    @Override
    public void deregister(ActorRef receiver, ActorSystemEvent event) {
        if(!(receiver instanceof ActorShardRef)) {
            throw new IllegalArgumentException("ActorRef must be referencing a Persistent Actor (i.e. annotated with @Actor)");
        }
        // get the underlying KafkaActorShard
        KafkaActorShard actorShard = (KafkaActorShard) ((ActorShardRef) receiver).getActorContainer();
        actorShard.getActorThread().deregister(actorShard.getKey(), event, receiver);
    }
}
