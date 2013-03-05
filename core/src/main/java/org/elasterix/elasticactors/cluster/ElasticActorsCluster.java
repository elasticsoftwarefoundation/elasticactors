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

import org.apache.log4j.Logger;
import org.elasterix.elasticactors.*;
import org.elasterix.elasticactors.cassandra.ClusterEventListener;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.annotation.Configurable;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;

import java.net.InetAddress;
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
public class ElasticActorsCluster implements ActorRefFactory, ApplicationContextAware, ClusterEventListener, ActorSystems {
    private static final Logger logger = Logger.getLogger(ElasticActorsCluster.class);
    private static final AtomicReference<ElasticActorsCluster> INSTANCE = new AtomicReference<ElasticActorsCluster>(null);
    private final ConcurrentMap<String,LocalActorSystemInstance> managedActorSystems = new ConcurrentHashMap<String,LocalActorSystemInstance>();
    private final AtomicBoolean clusterStarted = new AtomicBoolean(false);
    private String clusterName;
    private PhysicalNode localNode;

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
    public ActorSystem get(String actorSystemName) {
        return managedActorSystems.get(actorSystemName);
    }

    @Value("${elasticactors.cluster.name}")
    public void setClusterName(String clusterName) {
        this.clusterName = clusterName;
    }

    @Override
    public ActorRef create(String refSpec) throws IllegalArgumentException {
        return ActorRefTools.parse(refSpec,this);
    }
}
