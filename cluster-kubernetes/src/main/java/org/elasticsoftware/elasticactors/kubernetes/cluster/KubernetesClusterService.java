package org.elasticsoftware.elasticactors.kubernetes.cluster;

import io.fabric8.kubernetes.api.model.extensions.StatefulSet;
import io.fabric8.kubernetes.api.model.extensions.StatefulSetList;
import io.fabric8.kubernetes.client.DefaultKubernetesClient;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.KubernetesClientException;
import io.fabric8.kubernetes.client.Watcher;
import org.elasticsoftware.elasticactors.cluster.ClusterEventListener;
import org.elasticsoftware.elasticactors.cluster.ClusterMessageHandler;
import org.elasticsoftware.elasticactors.cluster.ClusterService;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;

public class KubernetesClusterService implements ClusterService {
    private KubernetesClient client;
    private final String appLabel;
    private final Queue<ClusterEventListener> eventListeners = new ConcurrentLinkedQueue<>();
    private ClusterMessageHandler clusterMessageHandler;

    public KubernetesClusterService(String appLabel) {
        this.appLabel = appLabel;
    }

    @PostConstruct
    public void init() {
        try {
            client = new DefaultKubernetesClient();
        } catch(KubernetesClientException e) {
            // we cannot connect to a k8s cluster
        }
    }

    @PreDestroy
    public void destroy() {

    }


    @Override
    public void reportReady() throws Exception {
        StatefulSetList statefulSetList = client.apps().statefulSets().inNamespace("default").withLabel("app", appLabel).list();
        // should have only one (or none) item
        if(statefulSetList.getItems().isEmpty()) {
            // is this an error?
        }

        // subscribe to updates
        client.apps().statefulSets().inNamespace("default").withLabel("app",appLabel).watch(new Watcher<StatefulSet>() {
            @Override
            public void eventReceived(Action action, StatefulSet resource) {

            }

            @Override
            public void onClose(KubernetesClientException cause) {

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
        this.clusterMessageHandler = clusterMessageHandler;
    }
}
