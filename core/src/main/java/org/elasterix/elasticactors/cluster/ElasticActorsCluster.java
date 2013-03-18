/*
 * Copyright (c) 2013 Joost van de Wijgerd <jwijgerd@gmail.com>
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *  	http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.elasterix.elasticactors.cluster;

import me.prettyprint.cassandra.serializers.StringSerializer;
import org.apache.log4j.Logger;
import org.elasterix.elasticactors.*;
import org.elasterix.elasticactors.cassandra.ClusterEventListener;
import org.elasterix.elasticactors.messaging.internal.CreateActorMessage;
import org.elasterix.elasticactors.serialization.MessageDeserializer;
import org.elasterix.elasticactors.serialization.MessageSerializer;
import org.elasterix.elasticactors.serialization.internal.*;
import org.elasterix.elasticactors.util.concurrent.ThreadBoundExecutor;
import org.elasterix.elasticactors.util.concurrent.ThreadBoundRunnable;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Configurable;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;

import java.net.InetAddress;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

/**
 * @author Joost van de Wijgerd
 */
@Configurable
public final class ElasticActorsCluster implements ActorRefFactory, ApplicationContextAware, ClusterEventListener, InternalActorSystems {
    private static final Logger logger = Logger.getLogger(ElasticActorsCluster.class);
    private static final AtomicReference<ElasticActorsCluster> INSTANCE = new AtomicReference<ElasticActorsCluster>(null);
    private final ConcurrentMap<String,LocalActorSystemInstance> managedActorSystems = new ConcurrentHashMap<String,LocalActorSystemInstance>();
    private final AtomicBoolean clusterStarted = new AtomicBoolean(false);
    private String clusterName;
    private PhysicalNode localNode;
    private NodeSelectorFactory nodeSelectorFactory;
    private ThreadBoundExecutor<String> executor;
    private final SystemSerializers systemSerializers = new SystemSerializers(this);
    private final SystemDeserializers systemDeserializers = new SystemDeserializers(this);


    public static ElasticActorsCluster getInstance() {
        return INSTANCE.get();
    }

    @Override
    public void setApplicationContext(ApplicationContext applicationContext) throws BeansException {
        INSTANCE.set(applicationContext.getBean(ElasticActorsCluster.class));
    }

    @Override
    public void onJoined(String hostId, InetAddress hostAddress) throws Exception {
        this.localNode = new PhysicalNodeImpl(hostId,hostAddress,true);
        logger.info(String.format("%s running, starting ElasticActors Runtime",localNode.toString()));
        // start NodeSelectorFactory
        nodeSelectorFactory.start();
        // load all actor systems (but not start them yet since we are not officially part of the cluster)
    }

    @Override
    public void onTopologyChanged(Map<InetAddress,String> topology) {
        logger.info("Cluster topology changed");
        List<PhysicalNode> clusterNodes = new LinkedList<PhysicalNode>();
        for (Map.Entry<InetAddress, String> hostEntry : topology.entrySet()) {
            if(localNode.getId().equals(hostEntry.getValue())) {
                clusterNodes.add(localNode);
            } else {
                clusterNodes.add(new PhysicalNodeImpl(hostEntry.getValue(),hostEntry.getKey(),false));
            }
        }
        logger.info("New Cluster view: "+clusterNodes.toString());
        // see if it's the first time
        if(clusterStarted.compareAndSet(false,true)) {
            // do some initialization
        }

        for (LocalActorSystemInstance actorSystemInstance : managedActorSystems.values()) {
            executor.execute(new RebalancingRunnable(actorSystemInstance,clusterNodes));
        }
    }

    private final class RebalancingRunnable implements ThreadBoundRunnable<String> {
        private final LocalActorSystemInstance actorSystemInstance;
        private final List<PhysicalNode> clusterNodes;

        private RebalancingRunnable(LocalActorSystemInstance actorSystemInstance, List<PhysicalNode> clusterNodes) {
            this.actorSystemInstance = actorSystemInstance;
            this.clusterNodes = clusterNodes;
        }

        @Override
        public String getKey() {
            return actorSystemInstance.getName();
        }

        @Override
        public void run() {
            logger.info(String.format("Rebalancing %d shards for ActorSystem[%s]",actorSystemInstance.getNumberOfShards(),actorSystemInstance.getName()));
            try {
                actorSystemInstance.distributeShards(clusterNodes);
            } catch (Exception e) {
                logger.error(String.format("ActorSystem[%s] failed to (re-)distribute shards",actorSystemInstance.getName()),e);
            }
        }
    }

    @Override
    public void onLeft() {
        // shutdown all actor systems
    }

    @Override
    public String getClusterName() {
        return clusterName;
    }

    @Override
    public ActorRefFactory getActorRefFactory() {
        return this;
    }

    @Override
    public InternalActorSystem get(String actorSystemName) {
        return managedActorSystems.get(actorSystemName);
    }

    @Override
    public <T> MessageSerializer<T> getSystemMessageSerializer(Class<T> messageClass) {
        return systemSerializers.get(messageClass);
    }

    @Override
    public <T> MessageDeserializer<T> getSystemMessageDeserializer(Class<T> messageClass) {
        return systemDeserializers.get(messageClass);
    }

    @Value("${elasticactors.cluster.name}")
    public void setClusterName(String clusterName) {
        this.clusterName = clusterName;
    }

    @Override
    public ActorRef create(String refSpec) throws IllegalArgumentException {
        return ActorRefTools.parse(refSpec,this);
    }

    @Autowired
    public void setNodeSelectorFactory(NodeSelectorFactory nodeSelectorFactory) {
        this.nodeSelectorFactory = nodeSelectorFactory;
    }

    @Autowired
    public void setExecutor(@Qualifier("clusterExecutor") ThreadBoundExecutor<String> executor) {
        this.executor = executor;
    }
}
