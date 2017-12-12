package org.elasticsoftware.elasticactors.kafka;

import com.google.common.base.Charsets;
import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsoftware.elasticactors.*;
import org.elasticsoftware.elasticactors.cache.NodeActorCacheManager;
import org.elasticsoftware.elasticactors.cache.ShardActorCacheManager;
import org.elasticsoftware.elasticactors.cluster.*;
import org.elasticsoftware.elasticactors.cluster.scheduler.InternalScheduler;
import org.elasticsoftware.elasticactors.cluster.scheduler.SchedulerService;
import org.elasticsoftware.elasticactors.kafka.scheduler.KafkaTopicScheduler;
import org.elasticsoftware.elasticactors.messaging.internal.ActivateActorMessage;
import org.elasticsoftware.elasticactors.messaging.internal.ActorType;
import org.elasticsoftware.elasticactors.messaging.internal.CreateActorMessage;
import org.elasticsoftware.elasticactors.runtime.ElasticActorsNode;
import org.elasticsoftware.elasticactors.scheduler.Scheduler;
import org.elasticsoftware.elasticactors.serialization.*;
import org.elasticsoftware.elasticactors.state.PersistentActor;

import javax.annotation.Nullable;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;
import java.util.stream.Collectors;

import static java.lang.String.format;
import static java.util.stream.Collectors.counting;
import static java.util.stream.Collectors.groupingBy;

public final class KafkaActorSystemInstance implements InternalActorSystem, ShardDistributor {
    private static final Logger logger = LogManager.getLogger(KafkaActorSystemInstance.class);
    private final InternalActorSystemConfiguration configuration;
    private final NodeSelectorFactory nodeSelectorFactory;
    private final InternalActorSystems cluster;
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

    public KafkaActorSystemInstance(ElasticActorsNode node,
                                    InternalActorSystemConfiguration configuration,
                                    NodeSelectorFactory nodeSelectorFactory,
                                    Integer numberOfShardThreads,
                                    String bootstrapServers,
                                    ShardActorCacheManager shardActorCacheManager,
                                    NodeActorCacheManager nodeActorCacheManager,
                                    Serializer<PersistentActor<ShardKey>,byte[]> stateSerializer,
                                    Deserializer<byte[],PersistentActor<ShardKey>> stareDeserializer) {
        this.schedulerService = new KafkaTopicScheduler(this);
        this.localNode = node;
        this.cluster = node;
        this.configuration = configuration;
        this.nodeSelectorFactory = nodeSelectorFactory;
        this.actorRefFactory = node;
        this.actorShards = new KafkaActorShard[configuration.getNumberOfShards()];
        this.shardThreads = new KafkaActorThread[numberOfShardThreads];
        for(int i = 0 ; i < numberOfShardThreads ; i++) {
            this.shardThreads[i] = new KafkaActorThread(cluster.getClusterName(), bootstrapServers, this, actorRefFactory, shardActorCacheManager, nodeActorCacheManager, stateSerializer, stareDeserializer);
        }
        for(int i = 0 ; i < configuration.getNumberOfShards() ; i++) {
            this.actorShards[i] = new KafkaActorShard(new ShardKey(configuration.getName(), i), this.shardThreads[i % numberOfShardThreads], this);
        }
        // add the local node
        this.localActorNode = new KafkaActorNode(localNode, this.shardThreads[0], this);
        this.activeNodes.add(localActorNode);
    }

    @Override
    public InternalActorSystemConfiguration getConfiguration() {
        return configuration;
    }

    @Override
    public ActorSystemEventListenerRegistry getEventListenerRegistry() {
        return null;
    }

    @Override
    public ActorRefGroup groupOf(Collection<ActorRef> members) throws IllegalArgumentException {
        return null;
    }

    @Override
    public ActorRef tempActorFor(String actorId) {
        return null;
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
        return schedulerService;
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
        return actorOf(actorId, actorClass.getName(), initialState);
    }

    @Override
    public ActorRef actorOf(String actorId, String actorClassName, ActorState initialState) throws Exception {
        // determine shard
        final KafkaActorShard shard = shardFor(actorId);
        // send CreateActorMessage to shard
        CreateActorMessage createActorMessage = new CreateActorMessage(getName(), actorClassName, actorId, initialState);
        ActorRef creator = ActorContextHolder.getSelf();
        shard.sendMessage(creator, shard.getActorRef(), createActorMessage);
        // create actor ref
        return cluster.createPersistentActorRef(shard, actorId);
    }

    private KafkaActorShard shardFor(String actorId) {
        return this.actorShards[Math.abs(hashFunction.hashString(actorId, Charsets.UTF_8).asInt()) % this.actorShards.length];
    }


    @Override
    public <T> ActorRef tempActorOf(Class<T> actorClass, @Nullable ActorState initialState) throws Exception {
        if(actorClass.getAnnotation(TempActor.class) == null) {
            throw new IllegalArgumentException("actorClass has to be annotated with @TempActor");
        }
        String actorId = UUID.randomUUID().toString();
        // see if we are being called in the context of another actor (and set the affinity key)
        // @todo: the affinitykey approach will not work like this, it means this should be executed in another
        // @todo: shardThread than the one that is polling the node topic
        String affinityKey = ActorContextHolder.hasActorContext() ? ActorContextHolder.getSelf().getActorId() : null;
        CreateActorMessage createActorMessage = new CreateActorMessage(getName(),
                actorClass.getName(),
                actorId,
                initialState,
                ActorType.TEMP,
                affinityKey);
        this.localActorNode.createTempActor(createActorMessage);
        return cluster.createTempActorRef(localActorNode, actorId);
    }

    @Override
    public ActorRef actorFor(String actorId) {
        return null;
    }

    @Override
    public ActorRef serviceActorFor(String actorId) {
        return null;
    }

    @Override
    public ActorRef serviceActorFor(String nodeId, String actorId) {
        return null;
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

    }

    @Override
    public List<ActorLifecycleListener<?>> getActorLifecycleListeners(Class<? extends ElasticActor> actorClass) {
        return null;
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
            logger.info(format("Initializing ActorSystem [%s]", getName()));
        }

        NodeSelector nodeSelector = nodeSelectorFactory.create(nodes);

        Map<PhysicalNode, ShardKey> shardDistribution = new HashMap<>();

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
        if(!strategy.waitForReleasedShards(10, TimeUnit.SECONDS)) {
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
            Set<String> serviceActors = configuration.getServices();
            if (serviceActors != null && !serviceActors.isEmpty()) {
                // initialize the service actors in the context
                KafkaActorNode localNode = activeNodes.get(0);
                for (String elasticActorEntry : serviceActors) {
                    localNode.sendMessage(null, localNode.getActorRef(),
                            new ActivateActorMessage(getName(), elasticActorEntry, ActorType.SERVICE));
                }
            }
        }
        // print out the shard distribution here
        Map<String, Long> collect = shardDistribution.entrySet().stream().map(entry -> entry.getKey().getId()).collect(groupingBy(Function.identity(), counting()));
        SortedMap<String, Long> sortedNodes = new TreeMap<>(collect);
        logger.info("Cluster shard mapping summary:");
        for (Map.Entry<String, Long> entry : sortedNodes.entrySet()) {
            logger.info(format("\t%s has %d shards assigned", entry.getKey(), entry.getValue()));
        }
        // now we need to generate the events for the new local shards (if any)
        // @todo: this needs to be done in the ActorThread
        // logger.info(format("Generating ACTOR_SHARD_INITIALIZED events for %d new shards",newLocalShards));
        // for (Integer newLocalShard : newLocalShards) {
        //     this.actorSystemEventListenerService.generateEvents(shardAdapters[newLocalShard], ACTOR_SHARD_INITIALIZED);
        // }
    }
}
