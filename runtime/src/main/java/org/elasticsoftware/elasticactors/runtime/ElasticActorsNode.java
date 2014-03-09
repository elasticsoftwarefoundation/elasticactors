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
import org.apache.log4j.Logger;
import org.elasticsoftware.elasticactors.*;
import org.elasticsoftware.elasticactors.cluster.*;
import org.elasticsoftware.elasticactors.serialization.MessageDeserializer;
import org.elasticsoftware.elasticactors.serialization.MessageSerializer;
import org.elasticsoftware.elasticactors.serialization.SerializationFramework;
import org.elasticsoftware.elasticactors.serialization.internal.SystemDeserializers;
import org.elasticsoftware.elasticactors.serialization.internal.SystemSerializers;
import org.elasticsoftware.elasticactors.util.concurrent.DaemonThreadFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import java.net.InetAddress;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

/**
 * @author Joost van de Wijgerd
 */
public final class ElasticActorsNode implements PhysicalNode, InternalActorSystems, ActorRefFactory, ClusterEventListener, ClusterMessageHandler {
    private static final Logger logger = Logger.getLogger(ElasticActorsNode.class);
    private final String clusterName;
    private final String nodeId;
    private final InetAddress nodeAddress;
    private final SystemSerializers systemSerializers = new SystemSerializers(this);
    private final SystemDeserializers systemDeserializers = new SystemDeserializers(this);
    private final CountDownLatch waitLatch = new CountDownLatch(1);
    private Cache<String,ActorRef> actorRefCache;
    @Autowired
    private ApplicationContext applicationContext;
    private ClusterService clusterService;

    private final ScheduledExecutorService scheduledExecutorService = Executors.newSingleThreadScheduledExecutor(new DaemonThreadFactory("CLUSTER_SCHEDULER"));

    public ElasticActorsNode(String clusterName, String nodeId, InetAddress nodeAddress) {
        this.clusterName = clusterName;
        this.nodeId = nodeId;
        this.nodeAddress = nodeAddress;
    }

    @Autowired
    public void setClusterService(ClusterService clusterService) {
        this.clusterService = clusterService;
        clusterService.addEventListener(this);
        clusterService.setClusterMessageHandler(this);
    }

    @PostConstruct
    public void init() {
        //@todo: take this value from the configuration file
        actorRefCache = CacheBuilder.newBuilder().maximumSize(10240).build();
    }

    @PreDestroy
    public void destroy() {
        clusterService.reportPlannedShutdown();
        waitLatch.countDown();
    }

    @Override
    public void onTopologyChanged(List<PhysicalNode> topology) throws Exception {
        // @todo: keep track of the previous schedule and cancel it if possible
        scheduledExecutorService.submit(new RebalancingRunnable(topology));
    }

    @Override
    public void onMasterElected(PhysicalNode masterNode) throws Exception {
        // ignore
    }

    @Override
    public void handleMessage(byte[] message, String senderToken) {
        //To change body of implemented methods use File | Settings | File Templates.
    }

    public void join() {
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
        Map<String,RemoteActorSystemInstance> remoteActorSystems = applicationContext.getBeansOfType(RemoteActorSystemInstance.class);
        if(remoteActorSystems != null) {
            return remoteActorSystems.get(clusterName);
        }
        return null;
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
            ref = new LocalClusterActorNodeRef(clusterName,node,actorId);
            actorRefCache.put(refSpec,ref);
        }
        return ref;
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
        private final List<PhysicalNode> clusterNodes;

        private RebalancingRunnable(List<PhysicalNode> clusterNodes) {
            this.clusterNodes = clusterNodes;
        }

        @Override
        public void run() {
            LocalActorSystemInstance instance = applicationContext.getBean(LocalActorSystemInstance.class);
            logger.info(String.format("Updating %d nodes for ActorSystem[%s]", clusterNodes.size(), instance.getName()));
            try {
                instance.updateNodes(clusterNodes);
            } catch (Exception e) {
                logger.error(String.format("ActorSystem[%s] failed to update nodes", instance.getName()), e);
            }
            logger.info(String.format("Rebalancing %d shards for ActorSystem[%s]", instance.getNumberOfShards(), instance.getName()));
            try {
                instance.distributeShards(clusterNodes);
            } catch (Exception e) {
                logger.error(String.format("ActorSystem[%s] failed to (re-)distribute shards", instance.getName()), e);
            }
        }
    }


}
