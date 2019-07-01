package org.elasticsoftware.elasticactors.kubernetes.cluster.statemachine.data;

import io.fabric8.kubernetes.api.model.apps.StatefulSet;
import org.elasticsoftware.elasticactors.kubernetes.cluster.statemachine.KubernetesStateMachineListener;

import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

public class KubernetesStateMachineData {

    private final AtomicReference<KubernetesClusterState> currentState;
    private final AtomicReference<StatefulSet> latestStableState;
    private final AtomicInteger currentTopology;
    private final Queue<KubernetesStateMachineListener> stateMachineListeners;

    public KubernetesStateMachineData() {
        this.currentState = new AtomicReference<>();
        this.latestStableState = new AtomicReference<>();
        this.currentTopology = new AtomicInteger();
        this.stateMachineListeners = new ConcurrentLinkedQueue<>();
    }

    public AtomicReference<KubernetesClusterState> getCurrentState() {
        return currentState;
    }

    public AtomicReference<StatefulSet> getLatestStableState() {
        return latestStableState;
    }

    public AtomicInteger getCurrentTopology() {
        return currentTopology;
    }

    public Queue<KubernetesStateMachineListener> getStateMachineListeners() {
        return stateMachineListeners;
    }

}
