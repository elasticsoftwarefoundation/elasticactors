/*
 * Copyright 2013 - 2024 The Original Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 *     you may not use this file except in compliance with the License.
 *     You may obtain a copy of the License at
 *
 *           http://www.apache.org/licenses/LICENSE-2.0
 *
 *     Unless required by applicable law or agreed to in writing, software
 *     distributed under the License is distributed on an "AS IS" BASIS,
 *     WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *     See the License for the specific language governing permissions and
 *     limitations under the License.
 *
 */

package org.elasticsoftware.elasticactors.kubernetes.cluster;

import io.fabric8.kubernetes.api.model.apps.StatefulSet;
import io.fabric8.kubernetes.client.DefaultKubernetesClient;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.KubernetesClientException;
import io.fabric8.kubernetes.client.Watch;
import io.fabric8.kubernetes.client.Watcher;
import org.elasticsoftware.elasticactors.PhysicalNode;
import org.elasticsoftware.elasticactors.cluster.ClusterEventListener;
import org.elasticsoftware.elasticactors.cluster.ClusterMessageHandler;
import org.elasticsoftware.elasticactors.cluster.ClusterService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import java.util.ArrayList;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static java.lang.String.format;
import static java.net.HttpURLConnection.HTTP_GONE;

public final class KubernetesClusterService implements ClusterService {
    private static final Logger logger = LoggerFactory.getLogger(KubernetesClusterService.class);
    private KubernetesClient client;
    private final String namespace;
    private final String name;
    private final String nodeId;
    private final String masterNodeId;
    private final Queue<ClusterEventListener> eventListeners = new ConcurrentLinkedQueue<>();
    private final ExecutorService clusterServiceExecutor =
        Executors.newSingleThreadExecutor(new DaemonThreadFactory("KUBERNETES-CLUSTER-SERVICE"));
    private final AtomicInteger currentTopology = new AtomicInteger();
    private final AtomicReference<Watch> currentWatch = new AtomicReference<>();
    private final Boolean useDesiredReplicas;

    public KubernetesClusterService(
            String namespace,
            String name,
            String nodeId,
            Boolean useDesiredReplicas) {
        this.namespace = namespace;
        this.name = name;
        this.nodeId = nodeId;
        this.useDesiredReplicas = useDesiredReplicas;
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
        Watch watch = currentWatch.get();
        if (watch != null) {
            watch.close();
        }
        clusterServiceExecutor.shutdownNow();
    }

    @Override
    public void reportReady() throws Exception {
        StatefulSet resource = client.apps().statefulSets().inNamespace(namespace).withName(name).get();

        if (resource == null) {
            throw new IllegalStateException(format("StatefulSet %s not found in namespace %s", name, namespace));
        }

        // send out the first update
        handleStatefulSetUpdate(resource);

        // subscribe to updates
        watchStatefulSet();

        // and send out the master to be the 0 ordinal pod (i.e. <name>-0)
        PhysicalNode masterNode = new PhysicalNode(masterNodeId, null, nodeId.equals(masterNodeId));
        this.eventListeners.forEach(clusterEventListener -> {
            try {
                clusterEventListener.onMasterElected(masterNode);
            } catch (Exception e) {
                logger.error("Unexpected exception while calling clusterEventListener.onMasterElected", e);
            }
        });
    }

    private void watchStatefulSet() {
        String resourceVersion = client.apps()
                .statefulSets()
                .inNamespace(namespace)
                .list()
                .getMetadata()
                .getResourceVersion();
        currentWatch.set(client.apps()
                .statefulSets()
                .inNamespace(namespace)
                .withName(name)
                .withResourceVersion(resourceVersion)
                .watch(watcher));
        logger.info(
                "Watching StatefulSet {} on namespace {} for resource version {}",
                name,
                namespace,
                resourceVersion);
    }

    private void handleStatefulSetUpdate(StatefulSet resource) {
        clusterServiceExecutor.submit(() -> reportTopologyChangeIfNeeded(resource));
    }

    private void reportTopologyChangeIfNeeded(StatefulSet resource) {
        logger.info(
                "Received Cluster State Update: spec.replicas={}, status.replicas={}, status.readyReplicas={}",
                resource.getSpec().getReplicas(),
                resource.getStatus().getReplicas(),
                resource.getStatus().getReadyReplicas());
        int totalReplicas = Boolean.TRUE.equals(useDesiredReplicas)
                ? Math.max(resource.getSpec().getReplicas(), resource.getStatus().getReplicas())
                : resource.getStatus().getReplicas();
        if (currentTopology.getAndSet(totalReplicas) != totalReplicas) {
            logger.info("Signalling Cluster Topology change to {} nodes", totalReplicas);
            List<PhysicalNode> nodeList = new ArrayList<>(totalReplicas);
            for (int i = 0; i < totalReplicas; i++) {
                String id = format("%s-%d", name, i);
                nodeList.add(new PhysicalNode(id, null, id.equals(nodeId)));
            }

            this.eventListeners.forEach(clusterEventListener -> {
                try {
                    clusterEventListener.onTopologyChanged(nodeList);
                } catch (Exception e) {
                    logger.error("Unexpected exception while calling clusterEventListener.onTopologyChanged", e);
                }
            });
        }
    }

    @Override
    public void reportPlannedShutdown() {
        // do nothing
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
            if (action == Action.MODIFIED) {
                handleStatefulSetUpdate(resource);
            }
        }

        @Override
        public void onClose(KubernetesClientException cause) {
            // try to re-add it if it's an abnormal close
            if (cause != null) {
                if (cause.getCode() == HTTP_GONE) {
                    logger.info(
                            "Watcher on StatefulSet {} was closed. Reason: {} ",
                            name,
                            cause.getMessage());
                } else {
                    logger.error("Watcher on StatefulSet {} was closed", name, cause);
                }
                watchStatefulSet();
            }
        }
    };

}
