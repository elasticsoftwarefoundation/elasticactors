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

package org.elasticsoftware.elasticactors.cassandra;

import org.apache.cassandra.service.IEndpointLifecycleSubscriber;
import org.apache.cassandra.tools.NodeProbe;
import org.apache.log4j.Logger;
import org.elasticsoftware.elasticactors.cluster.ClusterEventListener;
import org.springframework.beans.factory.annotation.Autowired;

import java.net.InetAddress;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 *
 */
public class ClusterView implements IEndpointLifecycleSubscriber {
    private static final Logger log = Logger.getLogger(ClusterView.class);
    private AtomicBoolean initialStartup = new AtomicBoolean(true);
    private volatile NodeProbe nodeProbe;
    private ClusterEventListener eventListener;
    private Set<InetAddress> liveNodes = new HashSet<InetAddress>();
    private final ScheduledExecutorService scheduledExecutorService = Executors.newSingleThreadScheduledExecutor();
    private volatile ScheduledFuture onTopologyChangedFuture = null;
    public static final int DEFAULT_WAIT_SECONDS = 10;

    @Override
    public void onJoinCluster(InetAddress endpoint) {
        log.info(String.format("%s joined the cluster", endpoint.getHostName()));
    }

    @Override
    public void onLeaveCluster(InetAddress endpoint) {
        log.info(String.format("%s left the cluster", endpoint.getHostName()));
    }

    @Override
    public void onUp(InetAddress endpoint) {
        //log.info(String.format("%s is now UP", endpoint.getHostAddress()));
        try {
            initializeNodeProbe();
            String localHostId = nodeProbe.getLocalHostId();
            Map<InetAddress, String> clusterMembers = getClusterMembers();
            if (localHostId.equals(clusterMembers.get(endpoint))) {
                if (initialStartup.compareAndSet(true, false)) {
                    log.info("************************** ElasticActors Node Marked UP ****************************");
                    log.info("Own host marked as UP, Initiating ElasticActors Cluster");
                    eventListener.onJoined(localHostId, endpoint);
                }
            }
            if(onTopologyChangedFuture == null) {
                onTopologyChangedFuture = scheduledExecutorService.schedule(new OnTopologyChanged(),DEFAULT_WAIT_SECONDS, TimeUnit.SECONDS);
            }
        } catch (Exception e) {
            // @todo: we probably want to shut down the whole system now
            log.error("Exception initializing ElasticActors Cluster", e);
        }

        /*
        log.info(String.format("localNode id = %s", nodeProbe.getLocalHostId()));
        List<InetAddress> naturalEndpoints = nodeProbe.getEndpoints("ElasticActors", "ActorSystems", "testKey");
        log.info(String.format("Primary endpoint for key 'testKey' is %s ", naturalEndpoints.get(0).getHostName()));
        */

    }

    private Map<InetAddress, String> getClusterMembers() {
        Map<String, String> hostMap = nodeProbe.getHostIdMap();
        List<String> liveNodes = nodeProbe.getLiveNodes();
        Map<InetAddress, String> clusterMembers = new HashMap<InetAddress, String>();
        try {
            for (Map.Entry<String, String> hostEntry : hostMap.entrySet()) {
                if (liveNodes.contains(hostEntry.getKey())) {
                    clusterMembers.put(InetAddress.getByName(hostEntry.getKey()), hostEntry.getValue());
                }
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        return clusterMembers;
    }

    private synchronized void initializeNodeProbe() {
        if (nodeProbe == null) {
            try {
                nodeProbe = new NodeProbe("localhost");
            } catch (Exception e) {
                log.error("Exception starting NodeProbe on localhost", e);
                nodeProbe = null;
            }
        }
    }

    @Override
    public void onDown(InetAddress endpoint) {
        log.info(String.format("%s is now DOWN", endpoint.getHostName()));
        if(onTopologyChangedFuture == null) {
            onTopologyChangedFuture = scheduledExecutorService.schedule(new OnTopologyChanged(),DEFAULT_WAIT_SECONDS, TimeUnit.SECONDS);
        }
    }

    @Override
    public void onMove(InetAddress endpoint) {
        //To change body of implemented methods use File | Settings | File Templates.
    }

    @Autowired
    public void setEventListener(ClusterEventListener eventListener) {
        this.eventListener = eventListener;
    }

    private final class OnTopologyChanged implements Callable<Void> {

        private OnTopologyChanged() {
        }

        @Override
        public Void call() throws Exception {
            eventListener.onTopologyChanged(getClusterMembers());
            onTopologyChangedFuture = null;
            return null;
        }
    }

}
