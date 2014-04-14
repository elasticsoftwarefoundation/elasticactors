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

package org.elasticsoftware.elasticactors.runtime;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.protobuf.InvalidProtocolBufferException;
import org.apache.log4j.Logger;
import org.elasticsoftware.elasticactors.*;
import org.elasticsoftware.elasticactors.cluster.*;
import org.elasticsoftware.elasticactors.cluster.messaging.ShardReleasedMessage;
import org.elasticsoftware.elasticactors.cluster.protobuf.Clustering;
import org.elasticsoftware.elasticactors.cluster.strategies.RunningNodeScaleDownStrategy;
import org.elasticsoftware.elasticactors.cluster.strategies.RunningNodeScaleUpStrategy;
import org.elasticsoftware.elasticactors.cluster.strategies.SingleNodeScaleUpStrategy;
import org.elasticsoftware.elasticactors.cluster.strategies.StartingNodeScaleUpStrategy;
import org.elasticsoftware.elasticactors.serialization.MessageDeserializer;
import org.elasticsoftware.elasticactors.serialization.MessageSerializer;
import org.elasticsoftware.elasticactors.serialization.SerializationFramework;
import org.elasticsoftware.elasticactors.serialization.internal.SystemDeserializers;
import org.elasticsoftware.elasticactors.serialization.internal.SystemSerializers;
import org.elasticsoftware.elasticactors.util.ManifestTools;
import org.elasticsoftware.elasticactors.util.concurrent.DaemonThreadFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import java.net.InetAddress;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static java.lang.String.format;

/**
 * @author Joost van de Wijgerd
 */
public final class ElasticActorsNode implements PhysicalNode, InternalActorSystems, ActorRefFactory, ClusterEventListener, ClusterMessageHandler {
    private static final Logger logger = Logger.getLogger(ElasticActorsNode.class);
    private final String clusterName;
    private final String nodeId;
    private final InetAddress nodeAddress;
    private final SystemSerializers systemSerializers = new SystemSerializers(this);
    private final SystemDeserializers systemDeserializers = new SystemDeserializers(this,this);
    private final InternalActorSystemConfiguration configuration;
    private final CountDownLatch waitLatch = new CountDownLatch(1);
    private final AtomicBoolean initialized = new AtomicBoolean(false);
    private final Cache<Class<? extends ElasticActor>,String> actorStateVersionCache = CacheBuilder.newBuilder().maximumSize(1024).build();
    private Cache<String,ActorRef> actorRefCache;
    @Autowired
    private ApplicationContext applicationContext;
    private ClusterService clusterService;
    private final LinkedBlockingQueue<ShardReleasedMessage> shardReleasedMessages = new LinkedBlockingQueue<>();
    private final AtomicReference<List<PhysicalNode>> currentTopology = new AtomicReference<>(null);
    private final ScheduledExecutorService scheduledExecutorService = Executors.newSingleThreadScheduledExecutor(new DaemonThreadFactory("CLUSTER_SCHEDULER"));

    public ElasticActorsNode(String clusterName, String nodeId, InetAddress nodeAddress, InternalActorSystemConfiguration configuration) {
        this.clusterName = clusterName;
        this.nodeId = nodeId;
        this.nodeAddress = nodeAddress;
        this.configuration = configuration;
    }

    @Autowired
    public void setClusterService(ClusterService clusterService) {
        this.clusterService = clusterService;
        clusterService.addEventListener(this);
        clusterService.setClusterMessageHandler(this);
    }

    @PostConstruct
    public void init() throws Exception {
        //@todo: take this value from the configuration file
        actorRefCache = CacheBuilder.newBuilder().maximumSize(10240).build();

    }

    @PreDestroy
    public void destroy() {
        clusterService.reportPlannedShutdown();
        waitLatch.countDown();
    }

    @Override
    public void onTopologyChanged(final List<PhysicalNode> topology) throws Exception {
        // see if we have a scale up or a scale down event
        List<PhysicalNode> previousTopology = currentTopology.get();
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
        // store the new topology as the current one
        this.currentTopology.set(topology);
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
            actorRef = ActorRefTools.parse(refSpec, this);
            actorRefCache.put(refSpec,actorRef);
        }
        return actorRef;
    }

    @Override
    public String getClusterName() {
        return clusterName;
    }

    @Override
    public InternalActorSystem get(String name) {
        return applicationContext.getBean(InternalActorSystem.class);
    }

    @Override
    public ActorSystem getRemote(String clusterName, String actorSystemName) {
        RemoteActorSystems remoteActorSystems = applicationContext.getBean(RemoteActorSystems.class);
        return remoteActorSystems != null ? remoteActorSystems.get(clusterName,actorSystemName) : null;
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
        return applicationContext.getBean(frameworkClass);
    }

    @Override
    public ActorRef createPersistentActorRef(ActorShard shard, String actorId) {
        final String refSpec = ActorShardRef.generateRefSpec(clusterName,shard,actorId);
        ActorRef ref = actorRefCache.getIfPresent(refSpec);
        if(ref == null) {
            ref = new ActorShardRef(clusterName,shard,actorId);
            actorRefCache.put(refSpec,ref);
        }
        return ref;
    }

    @Override
    public ActorRef createTempActorRef(ActorNode node, String actorId) {
        final String refSpec = LocalClusterActorNodeRef.generateRefSpec(this.clusterName,node,actorId);
        ActorRef ref = actorRefCache.getIfPresent(refSpec);
        if(ref == null) {
            ref = new LocalClusterActorNodeRef(clusterName,node,actorId);
            actorRefCache.put(refSpec,ref);
        }
        return ref;
    }

    @Override
    public ActorRef createServiceActorRef(ActorNode node, String actorId) {
        final String refSpec = ServiceActorRef.generateRefSpec(this.clusterName,node,actorId);
        ActorRef ref = actorRefCache.getIfPresent(refSpec);
        if(ref == null) {
            ref = new ServiceActorRef(clusterName,node,actorId);
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
    public boolean isLocal() {
        return true;
    }

    @Override
    public String getId() {
        return nodeId;
    }

    @Override
    public InetAddress getAddress() {
        return nodeAddress;
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
                RemoteActorSystems remoteActorSystems = applicationContext.getBean(RemoteActorSystems.class);
                try {
                    remoteActorSystems.init();
                } catch (Exception e) {
                    logger.error("Initializing Remote ActorSystems failed",e);
                }
                // apparently new nodes seem to catch on to the changed ClusterView faster than existing nodes
                // need to solve this in a more elegant way but for now we just pause for a bit to let the others
                // catch up
            }
            LocalActorSystemInstance instance = applicationContext.getBean(LocalActorSystemInstance.class);
            logger.info(format("Updating %d nodes for ActorSystem[%s]", clusterNodes.size(), instance.getName()));
            try {
                instance.updateNodes(clusterNodes);
            } catch (Exception e) {
                logger.error(format("ActorSystem[%s] failed to update nodes", instance.getName()), e);
            }
            logger.info(format("Rebalancing %d shards for ActorSystem[%s] using %s", instance.getNumberOfShards(), instance.getName(), shardDistributionStrategy.getClass().getSimpleName()));
            try {
                instance.distributeShards(clusterNodes,shardDistributionStrategy);
            } catch (Exception e) {
                logger.error(format("ActorSystem[%s] failed to (re-)distribute shards", instance.getName()), e);
            }
        }
    }


}
