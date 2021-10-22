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

package org.elasticsoftware.elasticactors.runtime;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.protobuf.InvalidProtocolBufferException;
import org.elasticsoftware.elasticactors.ActorNode;
import org.elasticsoftware.elasticactors.ActorRef;
import org.elasticsoftware.elasticactors.ActorShard;
import org.elasticsoftware.elasticactors.ActorSystem;
import org.elasticsoftware.elasticactors.ElasticActor;
import org.elasticsoftware.elasticactors.PhysicalNode;
import org.elasticsoftware.elasticactors.cluster.ActorRefFactory;
import org.elasticsoftware.elasticactors.cluster.ActorRefTools;
import org.elasticsoftware.elasticactors.cluster.ActorShardRef;
import org.elasticsoftware.elasticactors.cluster.BaseDisconnectedActorRef;
import org.elasticsoftware.elasticactors.cluster.ClusterEventListener;
import org.elasticsoftware.elasticactors.cluster.ClusterMessageHandler;
import org.elasticsoftware.elasticactors.cluster.ClusterService;
import org.elasticsoftware.elasticactors.cluster.InternalActorSystem;
import org.elasticsoftware.elasticactors.cluster.InternalActorSystems;
import org.elasticsoftware.elasticactors.cluster.LocalClusterActorNodeRef;
import org.elasticsoftware.elasticactors.cluster.RebalancingEventListener;
import org.elasticsoftware.elasticactors.cluster.RemoteActorSystems;
import org.elasticsoftware.elasticactors.cluster.ServiceActorRef;
import org.elasticsoftware.elasticactors.cluster.ShardDistributionStrategy;
import org.elasticsoftware.elasticactors.cluster.ShardDistributor;
import org.elasticsoftware.elasticactors.cluster.messaging.ShardReleasedMessage;
import org.elasticsoftware.elasticactors.cluster.protobuf.Clustering;
import org.elasticsoftware.elasticactors.cluster.strategies.RunningNodeScaleDownStrategy;
import org.elasticsoftware.elasticactors.cluster.strategies.RunningNodeScaleUpStrategy;
import org.elasticsoftware.elasticactors.cluster.strategies.SingleNodeScaleUpStrategy;
import org.elasticsoftware.elasticactors.cluster.strategies.StartingNodeScaleUpStrategy;
import org.elasticsoftware.elasticactors.serialization.MessageDeserializer;
import org.elasticsoftware.elasticactors.serialization.MessageSerializer;
import org.elasticsoftware.elasticactors.serialization.MessagingSystemDeserializers;
import org.elasticsoftware.elasticactors.serialization.MessagingSystemSerializers;
import org.elasticsoftware.elasticactors.serialization.SerializationFramework;
import org.elasticsoftware.elasticactors.serialization.SystemDeserializers;
import org.elasticsoftware.elasticactors.serialization.SystemSerializers;
import org.elasticsoftware.elasticactors.util.ManifestTools;
import org.elasticsoftware.elasticactors.util.concurrent.DaemonThreadFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import java.net.InetAddress;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

/**
 * @author Joost van de Wijgerd
 */
public final class ElasticActorsNode extends PhysicalNode implements
    InternalActorSystems,
    ActorRefFactory,
    ClusterEventListener,
    ClusterMessageHandler,
    ApplicationContextAware
{
    private static final Logger logger = LoggerFactory.getLogger(ElasticActorsNode.class);
    private final String clusterName;
    private final SystemSerializers systemSerializers = new MessagingSystemSerializers(this);
    private final SystemDeserializers systemDeserializers;
    private final CountDownLatch waitLatch = new CountDownLatch(1);
    private final AtomicBoolean initialized = new AtomicBoolean(false);
    private final Cache<Class<? extends ElasticActor>,String> actorStateVersionCache = CacheBuilder.newBuilder().maximumSize(1024).build();
    private final Cache<String,ActorRef> actorRefCache;
    private final Map<Class<? extends SerializationFramework>,SerializationFramework> serializationFrameworks = new ConcurrentHashMap<>();
    private ApplicationContext applicationContext;
    private ClusterService clusterService;
    private final LinkedBlockingQueue<ShardReleasedMessage> shardReleasedMessages = new LinkedBlockingQueue<>();
    private final AtomicReference<List<PhysicalNode>> currentTopology = new AtomicReference<>(null);
    private final ScheduledExecutorService scheduledExecutorService = Executors.newSingleThreadScheduledExecutor(new DaemonThreadFactory("CLUSTER_SCHEDULER"));
    private final List<RebalancingEventListener> rebalancingEventListeners = new CopyOnWriteArrayList<>();
    private final ActorRefTools actorRefTools;
    private InternalActorSystem internalActorSystem;
    private RemoteActorSystems remoteActorSystems;

    public ElasticActorsNode(String clusterName,
                             String nodeId,
                             InetAddress nodeAddress,
                             Cache<String,ActorRef> actorRefCache) {
        super(nodeId, nodeAddress, true);
        this.clusterName = clusterName;
        this.systemDeserializers = new MessagingSystemDeserializers(this,this);
        this.actorRefCache = actorRefCache;
        this.actorRefTools = new ActorRefTools(this);
    }

    public ElasticActorsNode(String clusterName,
                             String nodeId,
                             InetAddress nodeAddress,
                             Cache<String,ActorRef> actorRefCache,
                             ActorRefFactory actorRefFactory) {
        super(nodeId, nodeAddress, true);
        this.clusterName = clusterName;
        this.systemDeserializers = new MessagingSystemDeserializers(this, actorRefFactory);
        this.actorRefCache = actorRefCache;
        this.actorRefTools = new ActorRefTools(this);
    }

    @Autowired
    public void setClusterService(ClusterService clusterService) {
        this.clusterService = clusterService;
        clusterService.addEventListener(this);
        clusterService.setClusterMessageHandler(this);
    }

    @PostConstruct
    public void init() throws Exception {

    }

    @PreDestroy
    public void destroy() {
        clusterService.reportPlannedShutdown();
        waitLatch.countDown();
    }

    @Override
    public void onTopologyChanged(final List<PhysicalNode> topology) throws Exception {
        // see if we have a scale up or a scale down event
        // store the new topology as the current one
        List<PhysicalNode> previousTopology = currentTopology.getAndSet(topology);
        ShardDistributionStrategy shardDistributionStrategy;
        if(previousTopology == null) {
            // scale out, I'm the one that's starting up.. will receive Local Shards..
            if(topology.size() == 1) {
                // if there is only one node in the list, then I'm the only server
                shardDistributionStrategy = new SingleNodeScaleUpStrategy();
            } else {
                // there are multiple nodes, I will receive shard releases messages
                shardDistributionStrategy = new StartingNodeScaleUpStrategy(shardReleasedMessages);
            }
        } else {
            // we are already running, see if this is a scale up or a scale down
            if(previousTopology.size() < topology.size()) {
                // scale up
                shardDistributionStrategy = new RunningNodeScaleUpStrategy(shardReleasedMessages,clusterService);
            } else if(previousTopology.size() > topology.size()) {
                // scale down
                shardDistributionStrategy = new RunningNodeScaleDownStrategy();
            } else {
                // topology changed, but same size.. node added at the same time node was removed
                // new node will use StartingNodeScaleUpStrategy and wait for ShardReleasedMessages
                // which may never arrive since the node that was removed won't be sending them
                // what to do?
                // let MasterNode take over the role of sending ShardReleased messages?
                // let MasterNode decide on the strategy?
                // for now treat it as scale down and let the starting node time out
                shardDistributionStrategy = new RunningNodeScaleDownStrategy();
            }
        }
        scheduledExecutorService.submit(new RebalancingRunnable(shardDistributionStrategy, topology));
    }

    @Override
    public void  onMasterElected(PhysicalNode masterNode) throws Exception {
        // ignore
    }

    @Override
    public void handleMessage(byte[] message, String senderToken) {
        // @todo: abstract this more
        try {
            Clustering.ClusterMessage clusterMessage = Clustering.ClusterMessage.parseFrom(message);
            if(clusterMessage.hasShardReleased()) {
                // @todo: need to take into account the Shoal viewId here
                ShardReleasedMessage shardReleasedMessage = new ShardReleasedMessage(clusterMessage.getShardReleased().getActorSystem(),clusterMessage.getShardReleased().getShardId());
                //
                shardReleasedMessages.add(shardReleasedMessage);
            }
        } catch(InvalidProtocolBufferException e) {
            logger.error("Exception while deserializing ClusterMessage",e);
        }
    }

    public void join() throws Exception {
        // send the cluster we're ready
        clusterService.reportReady();

        try {
            waitLatch.await();
        } catch (InterruptedException e) {
            //
        }
    }

    @Override
    public ActorRef create(final String refSpec) {
        ActorRef actorRef = actorRefCache.getIfPresent(refSpec);
        if(actorRef == null) {
            actorRef = actorRefTools.parse(refSpec);
            if (!(actorRef instanceof BaseDisconnectedActorRef)) {
                actorRefCache.put(refSpec, actorRef);
            }
        }
        return actorRef;
    }

    @Override
    public String getClusterName() {
        return clusterName;
    }

    @Override
    public InternalActorSystem get(String name) {
        if (internalActorSystem == null) {
            internalActorSystem = applicationContext.getBean(InternalActorSystem.class);
        }
        return internalActorSystem;
    }

    @Override
    public ActorSystem getRemote(String clusterName, String actorSystemName) {
        if (remoteActorSystems == null) {
            remoteActorSystems = applicationContext.getBean(RemoteActorSystems.class);
        }
        return remoteActorSystems.get(clusterName, actorSystemName);
    }

    @Override
    public ActorSystem getRemote(String actorSystemName) {
        if (remoteActorSystems == null) {
            remoteActorSystems = applicationContext.getBean(RemoteActorSystems.class);
        }
        return remoteActorSystems.get(actorSystemName);
    }

    @Override
    public void registerRebalancingEventListener(RebalancingEventListener eventListener) {
        this.rebalancingEventListeners.add(eventListener);
    }

    @Override
    public <T> MessageSerializer<T> getSystemMessageSerializer(Class<T> messageClass) {
        return systemSerializers.get(messageClass);
    }

    @Override
    public <T> MessageDeserializer<T> getSystemMessageDeserializer(Class<T> messageClass) {
        return systemDeserializers.get(messageClass);
    }

    @Override
    public SerializationFramework getSerializationFramework(Class<? extends SerializationFramework> frameworkClass) {
        return serializationFrameworks.computeIfAbsent(frameworkClass, applicationContext::getBean);
    }

    @Override
    public ActorRef createPersistentActorRef(ActorShard shard, String actorId) {
        final String refSpec = ActorShardRef.generateRefSpec(clusterName,shard,actorId);
        ActorRef ref = actorRefCache.getIfPresent(refSpec);
        if(ref == null) {
            ref = new ActorShardRef(clusterName, shard, actorId, get(null));
            actorRefCache.put(refSpec,ref);
        }
        return ref;
    }

    @Override
    public ActorRef createTempActorRef(ActorNode node, String actorId) {
        final String refSpec = LocalClusterActorNodeRef.generateRefSpec(this.clusterName,node,actorId);
        ActorRef ref = actorRefCache.getIfPresent(refSpec);
        if(ref == null) {
            ref = new LocalClusterActorNodeRef(get(null), clusterName, node, actorId);
            actorRefCache.put(refSpec,ref);
        }
        return ref;
    }

    @Override
    public ActorRef createServiceActorRef(ActorNode node, String actorId) {
        final String refSpec = ServiceActorRef.generateRefSpec(this.clusterName,node,actorId);
        ActorRef ref = actorRefCache.getIfPresent(refSpec);
        if(ref == null) {
            ref = new ServiceActorRef(get(null), clusterName, node, actorId);
            actorRefCache.put(refSpec,ref);
        }
        return ref;
    }

    @Override
    public String getActorStateVersion(Class<? extends ElasticActor> actorClass) {
        String version = actorStateVersionCache.getIfPresent(actorClass);
        if(version == null) {
            version = ManifestTools.extractActorStateVersion(actorClass);
            actorStateVersionCache.put(actorClass,version);
        }
        return version;
    }

    @Override
    public void setApplicationContext(ApplicationContext applicationContext) throws BeansException {
        this.applicationContext = applicationContext;
    }

    private final class RebalancingRunnable implements Runnable {
        private final ShardDistributionStrategy shardDistributionStrategy;
        private final List<PhysicalNode> clusterNodes;

        private RebalancingRunnable(ShardDistributionStrategy shardDistributionStrategy, List<PhysicalNode> clusterNodes) {
            this.shardDistributionStrategy = shardDistributionStrategy;
            this.clusterNodes = clusterNodes;
        }

        @Override
        public void run() {
            if(initialized.compareAndSet(false,true)) {
                // load the remote actorsystems (if any)
                applicationContext.getBeansOfType(RemoteActorSystems.class).forEach((s, remoteActorSystems) -> {
                    try {
                        remoteActorSystems.init();
                    } catch (Exception e) {
                        logger.error("IMPORTANT: Initializing Remote ActorSystems failed, ElasticActors cluster is unstable. Please check all nodes",e);
                    }
                });
            }
            // call the pre methods on the RebalancingEventListeners
            for (RebalancingEventListener rebalancingEventListener : rebalancingEventListeners) {
                try {
                    if(shardDistributionStrategy instanceof RunningNodeScaleDownStrategy) {
                        rebalancingEventListener.preScaleDown();
                    } else {
                        rebalancingEventListener.preScaleUp();
                    }
                } catch(Exception e) {
                    logger.warn("Exception while calling RebalancingEventListener preScaleUp/Down [{}]",rebalancingEventListener.getClass().getName(),e);
                }
            }
            ShardDistributor distributor = applicationContext.getBean(ShardDistributor.class);
            InternalActorSystem instance = applicationContext.getBean(InternalActorSystem.class);
            logger.info("Updating {} nodes for ActorSystem[{}]", clusterNodes.size(), instance.getName());
            try {
                distributor.updateNodes(clusterNodes);
            } catch (Exception e) {
                logger.error("IMPORTANT: ActorSystem[{}] failed to update nodes, ElasticActors cluster is unstable. Please check all nodes", instance.getName(), e);
            }
            logger.info("Rebalancing {} shards for ActorSystem[{}] using {}", instance.getNumberOfShards(), instance.getName(), shardDistributionStrategy.getClass().getSimpleName());
            try {
                distributor.distributeShards(clusterNodes,shardDistributionStrategy);
            } catch (Exception e) {
                logger.error("IMPORTANT: ActorSystem[{}] failed to (re-)distribute shards,ElasticActors cluster is unstable. Please check all nodes", instance.getName(), e);
            }
            // call the post methods on the RebalancingEventListeners
            for (RebalancingEventListener rebalancingEventListener : rebalancingEventListeners) {
                try {
                    if(shardDistributionStrategy instanceof RunningNodeScaleDownStrategy) {
                        rebalancingEventListener.postScaleDown();
                    } else {
                        rebalancingEventListener.postScaleUp();
                    }
                } catch(Exception e) {
                    logger.warn("Exception while calling RebalancingEventListener postScaleUp/Down [{}]",rebalancingEventListener.getClass().getName(),e);
                }
            }
        }
    }


}
