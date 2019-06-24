package org.elasticsoftware.elasticactors.kubernetes.cluster.statemachine;

import io.fabric8.kubernetes.api.model.apps.StatefulSet;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

class ClusterStateData {

    private final AtomicReference<ClusterState> currentState;
    private final AtomicReference<StatefulSet> latestStableState;
    private final AtomicInteger currentTopology;
    private final Consumer<Integer> onTopologyChange;
    private final BiConsumer<Runnable, Integer> scaleUpTaskScheduler;

    ClusterStateData(Consumer<Integer> onTopologyChange, BiConsumer<Runnable, Integer> scaleUpTaskScheduler) {
        this.currentState = new AtomicReference<>();
        this.latestStableState = new AtomicReference<>();
        this.currentTopology = new AtomicInteger();
        this.onTopologyChange = onTopologyChange;
        this.scaleUpTaskScheduler = scaleUpTaskScheduler;
    }

    AtomicReference<ClusterState> getCurrentState() {
        return currentState;
    }

    AtomicReference<StatefulSet> getLatestStableState() {
        return latestStableState;
    }

    AtomicInteger getCurrentTopology() {
        return currentTopology;
    }

    Consumer<Integer> getOnTopologyChange() {
        return onTopologyChange;
    }

    BiConsumer<Runnable, Integer> getScaleUpTaskScheduler() {
        return scaleUpTaskScheduler;
    }
}
