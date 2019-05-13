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
import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import java.util.ArrayList;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicReference;

import static java.lang.String.format;
import static java.util.concurrent.Executors.newSingleThreadScheduledExecutor;

public final class KubernetesClusterService implements ClusterService {
    private static final Logger logger = LogManager.getLogger(KubernetesClusterService.class);
    private KubernetesClient client;
    private final String namespace;
    private final String name;
    private final String nodeId;
    private final String masterNodeId;
    private final Queue<ClusterEventListener> eventListeners = new ConcurrentLinkedQueue<>();
    private final AtomicReference<StatefulSet> currentState = new AtomicReference<>(null);
    private final AtomicReference<Status> currentStatus = new AtomicReference<>(Status.STABLE);
    private final ScheduledExecutorService scheduledExecutorService =
            newSingleThreadScheduledExecutor(new DaemonThreadFactory("KUBERNETES_CLUSTERSERVICE_SCHEDULER"));
    private final AtomicReference<ScheduledFuture> scheduledScaleUpTimeout = new AtomicReference<>();

    public KubernetesClusterService(String namespace, String name, String nodeId) {
        this.namespace = namespace;
        this.name = name;
        this.nodeId = nodeId;
        this.masterNodeId = format("%s-0", name);
    }

    @PostConstruct
    public void init() {
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

        if(statefulSet == null) {
            throw new IllegalStateException(format("StatefulSet %s not found in namespace %s", name, namespace));
        }

        this.currentState.set(statefulSet);

        // subscribe to updates
        client.apps().statefulSets().inNamespace(namespace).withName(name).watch(watcher);

        // send out the first update
        signalTopologyChange(statefulSet);

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

    private void signalTopologyChange(StatefulSet statefulSet) {
        Integer totalReplicas = statefulSet.getSpec().getReplicas();
        List<PhysicalNode> nodeList = new ArrayList<>();

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
        // @todo: how do I get a message to the other server?
    }

    @Override
    public void setClusterMessageHandler(ClusterMessageHandler clusterMessageHandler) {
        // do nothing
    }

    private enum Status {
        STABLE, SCALING_UP, SCALING_DOWN, SCALING_UP_STARTED, UNSTABLE
    }

    private final Watcher<StatefulSet> watcher = new Watcher<StatefulSet>() {
        @Override
        public void eventReceived(Action action, StatefulSet resource) {
            if (Action.MODIFIED.equals(action)) {
                // get the total number of replicas to identify if something changed
                Integer totalReplicas = resource.getSpec().getReplicas();
                logger.info(format("Got MODIFIED Action: spec.replicas=%d, status.replicas=%d, status.currentReplicas=%d, status.readyReplicas=%d",
                        resource.getSpec().getReplicas(), resource.getStatus().getReplicas(),
                        resource.getStatus().getCurrentReplicas(), resource.getStatus().getReadyReplicas()));
                if (currentStatus.get().equals(Status.STABLE)) {
                    Integer currentReplicas = currentState.get().getSpec().getReplicas();
                    if (currentReplicas.equals(totalReplicas)) {
                        logger.info("Cluster topology remains unchanged");
                    } else if (currentReplicas > totalReplicas) {
                        logger.info(format("Cluster topology changed from %d to %d replicas -> setting status to SCALING_DOWN", currentReplicas, totalReplicas));
                        // we are scaling down the cluster
                        currentStatus.set(Status.SCALING_DOWN);
                    } else {
                        logger.info(format("Cluster topology changed from %d to %d replicas -> setting status to SCALING_UP", currentReplicas, totalReplicas));
                        // we are scaling down the cluster
                        currentStatus.set(Status.SCALING_UP);
                    }
                }

                if (currentStatus.get().equals(Status.SCALING_DOWN)) {
                    // spec and status should all be the same
                    if (totalReplicas.equals(resource.getStatus().getReplicas()) &&
                            totalReplicas.equals(resource.getStatus().getCurrentReplicas()) &&
                            totalReplicas.equals(resource.getStatus().getReadyReplicas())) {
                        logger.info(format("Successfully scaled down to %d nodes", totalReplicas));
                        currentStatus.set(Status.STABLE);
                        currentState.set(resource);
                        signalTopologyChange(resource);
                    }
                } else if (currentStatus.get().equals(Status.SCALING_UP)) {
                    // with scaling up it can happen that there are no more resources and the cluster will scale down
                    // again without anything actually happening so we need to check here with the current state again
                    Integer currentReplicas = currentState.get().getSpec().getReplicas();
                    if (currentReplicas.equals(totalReplicas)) {
                        logger.info("Scaling up cancelled, reverting back to STABLE status");
                        currentStatus.set(Status.STABLE);
                    } else if (currentReplicas > totalReplicas) {  // it could also be smaller, in that case we are actually scaling down
                        logger.info("Scaling up cancelled, Scale down detected. Switching to SCALING_DOWN status");
                        currentStatus.set(Status.SCALING_DOWN);
                    }
                    // don't wait for the other node to be ready but shed partitions immediately
                    else if (totalReplicas.equals(resource.getStatus().getReplicas()) &&
                            (totalReplicas.equals(resource.getStatus().getCurrentReplicas()) ||
                            resource.getStatus().getReadyReplicas() == null)) {
                        logger.info(format("Starting scale up to %d nodes", totalReplicas));
                        currentStatus.set(Status.SCALING_UP_STARTED);
                        signalTopologyChange(resource);
                        // we need to set a timeout to receive the ready message
                        scheduledScaleUpTimeout.set(scheduledExecutorService.schedule(() -> {
                            logger.error(format("Scaling up to %d nodes failed. reverting to previous state", totalReplicas));
                            currentStatus.set(Status.UNSTABLE);
                            // change the topology back to the old state
                            signalTopologyChange(currentState.get());
                        }, 1, TimeUnit.MINUTES)); // set it to 60 seconds
                    }
                } else if(currentStatus.get().equals(Status.SCALING_UP_STARTED)) {
                    if(totalReplicas.equals(resource.getStatus().getReadyReplicas())) {
                        logger.info(format("Successfully scaled up to %d nodes", totalReplicas));
                        scheduledScaleUpTimeout.getAndSet(null).cancel(false);
                        currentStatus.set(Status.STABLE);
                        currentState.set(resource);
                    }
                } else if(currentStatus.get().equals(Status.UNSTABLE)) {
                    // we need to somehow get out of this UNSTABLE state
                    // spec and status should all be the same
                    if (totalReplicas.equals(currentState.get().getSpec().getReplicas()) &&
                            totalReplicas.equals(resource.getStatus().getReplicas()) &&
                            totalReplicas.equals(resource.getStatus().getCurrentReplicas()) &&
                            totalReplicas.equals(resource.getStatus().getReadyReplicas())) {
                        logger.info("Switching back to STABLE state");
                        currentStatus.set(Status.STABLE);
                        currentState.set(resource);
                    }
                }
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
