/*
 *   Copyright 2013 - 2022 The Original Authors
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

package org.elasticsoftware.elasticactors.test.cluster;

import org.elasticsoftware.elasticactors.PhysicalNode;
import org.elasticsoftware.elasticactors.cluster.ClusterEventListener;
import org.elasticsoftware.elasticactors.cluster.ClusterMessageHandler;
import org.elasticsoftware.elasticactors.cluster.ClusterService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;

/**
 * @author Joost van de Wijgerd
 */
public class SingleNodeClusterService implements ClusterService {
    private static Logger logger = LoggerFactory.getLogger(SingleNodeClusterService.class);
    private final Queue<ClusterEventListener> eventListeners = new ConcurrentLinkedQueue<>();
    private ClusterMessageHandler clusterMessageHandler;
    private final PhysicalNode localNode;

    public SingleNodeClusterService(PhysicalNode localNode) {
        this.localNode = localNode;
    }

    @Override
    public void reportReady() {
        for (ClusterEventListener eventListener : eventListeners) {
            try {
                eventListener.onTopologyChanged(Collections.singletonList(localNode));
            } catch (Exception e) {
                logger.error("Exception in onTopologyChanged", e);
            }
            try {
                eventListener.onMasterElected(localNode);
            } catch (Exception e) {
                logger.error("Exception in onMasterElected",e);
            }
        }
    }

    @Override
    public void reportPlannedShutdown() {
        //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public void addEventListener(ClusterEventListener eventListener) {
        eventListeners.add(eventListener);
    }

    @Override
    public void removeEventListener(ClusterEventListener eventListener) {
        this.eventListeners.remove(eventListener);
    }

    @Override
    public void sendMessage(String memberToken, byte[] message) throws Exception {
        //@todo: send to local?
    }

    @Override
    public void setClusterMessageHandler(ClusterMessageHandler clusterMessageHandler) {
        this.clusterMessageHandler = clusterMessageHandler;
    }
}
