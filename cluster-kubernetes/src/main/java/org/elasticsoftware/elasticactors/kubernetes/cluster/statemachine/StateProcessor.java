package org.elasticsoftware.elasticactors.kubernetes.cluster.statemachine;

import io.fabric8.kubernetes.api.model.apps.StatefulSet;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import static java.lang.String.format;

abstract class StateProcessor {

    protected static final Logger logger = LogManager.getLogger(KubernetesStateMachine.class);

    protected final ClusterStateData clusterStateData;

    /**
     * Processes the current resource
     *
     * @param resource The current state of the Stateful Set
     * @return true if the resource should be processed by another processor after this one is done
     */
    abstract boolean process(StatefulSet resource);

    protected static int getInt(Integer value) {
        return value != null ? value : 0;
    }

    protected void switchToStableState(StatefulSet resource) {
        clusterStateData.getCurrentState().set(ClusterState.STABLE);
        clusterStateData.getLatestStableState().set(resource);
        clusterStateData.getScaleUpTaskScheduler().accept(null, null);
        clusterStateData.getOnTopologyChange().accept(resource.getSpec().getReplicas());
    }

    protected void scheduleTimeoutTask(int desiredReplicas, int baseReplicas) {
        clusterStateData.getScaleUpTaskScheduler().accept(() -> {
            final int stableReplicas = clusterStateData.getLatestStableState().get().getSpec().getReplicas();
            logger.error(format("Scaling up to %d nodes failed. Reverting to previous stable state with %d nodes -> setting status to UNSTABLE", desiredReplicas, stableReplicas));
            clusterStateData.getCurrentState().set(ClusterState.UNSTABLE);
            // change the topology back to the old state
            clusterStateData.getOnTopologyChange().accept(stableReplicas);
        }, Math.max(desiredReplicas - baseReplicas, 1));
    }

    StateProcessor(ClusterStateData clusterStateData) {
        this.clusterStateData = clusterStateData;
    }

}
