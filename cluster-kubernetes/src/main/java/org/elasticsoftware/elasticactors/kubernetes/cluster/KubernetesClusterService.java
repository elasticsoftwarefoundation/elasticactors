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

package org.elasticsoftware.elasticactors.kubernetes.cluster;

import io.fabric8.kubernetes.api.model.apps.StatefulSet;
import io.fabric8.kubernetes.client.DefaultKubernetesClient;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.KubernetesClientException;
import io.fabric8.kubernetes.client.Watcher;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsoftware.elasticactors.PhysicalNode;
import org.elasticsoftware.elasticactors.cluster.ClusterEventListener;
import org.elasticsoftware.elasticactors.cluster.ClusterMessageHandler;
import org.elasticsoftware.elasticactors.cluster.ClusterService;
import org.elasticsoftware.elasticactors.kubernetes.cluster.statemachine.KubernetesStateMachine;
import org.elasticsoftware.elasticactors.kubernetes.cluster.statemachine.KubernetesStateMachineListener;
import org.elasticsoftware.elasticactors.kubernetes.cluster.statemachine.impl.SingleThreadKubernetesStateMachine;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import java.util.ArrayList;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ScheduledExecutorService;

import static java.lang.String.format;
import static java.util.concurrent.Executors.newSingleThreadScheduledExecutor;

public final class KubernetesClusterService implements ClusterService, KubernetesStateMachineListener {
    private static final Logger logger = LogManager.getLogger(KubernetesClusterService.class);
    private KubernetesClient client;
    private final String namespace;
    private final String name;
    private final String nodeId;
    private final String masterNodeId;
    private final Queue<ClusterEventListener> eventListeners = new ConcurrentLinkedQueue<>();
    private final KubernetesStateMachine kubernetesStateMachine;

    public KubernetesClusterService(String namespace, String name, String nodeId, Integer timeoutSeconds) {
        this.namespace = namespace;
        this.name = name;
        this.nodeId = nodeId;
        this.masterNodeId = format("%s-0", name);

        ScheduledExecutorService scheduledExecutorService =
                newSingleThreadScheduledExecutor(new DaemonThreadFactory("KUBERNETES_CLUSTERSERVICE_SCHEDULER"));
        this.kubernetesStateMachine = new SingleThreadKubernetesStateMachine(new TaskScheduler(scheduledExecutorService, timeoutSeconds));
        this.kubernetesStateMachine.addListener(this);
    }

    @PostConstruct
    public void init() {
        kubernetesStateMachine.addListener(this);
        try {
            client = new DefaultKubernetesClient();
        } catch(KubernetesClientException e) {
            // we cannot connect to a k8s cluster
            logger.error("Exception creation DefaultKubernetesClient", e);
            throw new IllegalStateException("Unable to create DefaultKubernetesClient", e);
        }
    }

    @PreDestroy
    public void destroy() {

    }

    @Override
    public void reportReady() throws Exception {
        StatefulSet statefulSet = client.apps().statefulSets().inNamespace(namespace).withName(name).get();

        if (statefulSet == null) {
            throw new IllegalStateException(format("StatefulSet %s not found in namespace %s", name, namespace));
        }

        // send out the first update
        kubernetesStateMachine.handleStateUpdate(statefulSet);

        // subscribe to updates
        client.apps().statefulSets().inNamespace(namespace).withName(name).watch(watcher);

        // and send out the master to be the 0 ordinal pod (i.e. <name>-0)
        PhysicalNodeImpl masterNode = new PhysicalNodeImpl(masterNodeId, null, nodeId.equals(masterNodeId));
        this.eventListeners.forEach(clusterEventListener -> {
            try {
                clusterEventListener.onMasterElected(masterNode);
            } catch (Exception e) {
                logger.error("Unexpected exception while calling clusterEventListener.onMasterElected", e);
            }
        });
    }

    @Override
    public void onTopologyChange(int totalReplicas) {
        logger.info(format("Signalling Cluster Topology change to %d nodes", totalReplicas));
        List<PhysicalNode> nodeList = new ArrayList<>(totalReplicas);
        for (int i = 0; i < totalReplicas; i++) {
            String id = format("%s-%d", name, i);
            nodeList.add(new PhysicalNodeImpl(id, null, id.equals(nodeId)));
        }

        this.eventListeners.forEach(clusterEventListener -> {
            try {
                clusterEventListener.onTopologyChanged(nodeList);
            } catch (Exception e) {
                logger.error("Unexpected exception while calling clusterEventListener.onTopologyChanged", e);
            }
        });

    }

    @Override
    public void reportPlannedShutdown() {
        // don't do anything.. scale down changes should come from the k8s api
    }

    @Override
    public void addEventListener(ClusterEventListener eventListener) {
        this.eventListeners.add(eventListener);
    }

    @Override
    public void removeEventListener(ClusterEventListener eventListener) {
        this.eventListeners.remove(eventListener);
    }

    @Override
    public void sendMessage(String memberToken, byte[] message) throws Exception {
        // do nothing
    }

    @Override
    public void setClusterMessageHandler(ClusterMessageHandler clusterMessageHandler) {
        // do nothing
    }

    private final Watcher<StatefulSet> watcher = new Watcher<StatefulSet>() {
        @Override
        public void eventReceived(Action action, StatefulSet resource) {
            if (Action.MODIFIED.equals(action)) {
                kubernetesStateMachine.handleStateUpdate(resource);
            }
        }

        @Override
        public void onClose(KubernetesClientException cause) {
            logger.error(format("Watcher on statefulset %s was closed", name));
            // try to re-add it
            client.apps().statefulSets().inNamespace(namespace).withName(name).watch(watcher);
        }
    };
}
