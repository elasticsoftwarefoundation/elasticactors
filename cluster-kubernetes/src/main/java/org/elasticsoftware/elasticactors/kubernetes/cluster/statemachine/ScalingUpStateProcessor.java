package org.elasticsoftware.elasticactors.kubernetes.cluster.statemachine;

import io.fabric8.kubernetes.api.model.apps.StatefulSet;

import static java.lang.String.format;

class ScalingUpStateProcessor extends StateProcessor {

    ScalingUpStateProcessor(ClusterStateData clusterStateData) {
        super(clusterStateData);
    }

    @Override
    boolean process(StatefulSet resource) {

        int desiredReplicas = resource.getSpec().getReplicas();
        int actualReplicas = resource.getStatus().getReplicas();
        int currentDesiredReplicas = clusterStateData.getLatestStableState().get().getSpec().getReplicas();

        // with scaling up it can happen that there are no more resources and the cluster will scale down
        // again without anything actually happening so we need to check here with the current state again
        if (desiredReplicas == currentDesiredReplicas) {
            logger.info("Scaling up cancelled. Reverting back to STABLE status");
            switchToStableState(resource);
        } else if (desiredReplicas < currentDesiredReplicas) {  // it could also be smaller, in that case we are actually scaling down
            logger.info("Scaling up cancelled. Scale down detected. Switching to SCALING_DOWN status");
            clusterStateData.getCurrentState().set(ClusterState.SCALING_DOWN);
        } else if (actualReplicas > clusterStateData.getCurrentTopology().get()) { // don't wait for the other node to be ready but shed partitions immediately
            logger.info(format("Starting scale up to %d nodes -> setting status to SCALING_UP_STARTED", desiredReplicas));
            clusterStateData.getCurrentState().set(ClusterState.SCALING_UP_STARTED);
            clusterStateData.getOnTopologyChange().accept(desiredReplicas);
            // we need to set a timeout to receive the ready message
            scheduleTimeoutTask(desiredReplicas, currentDesiredReplicas);
        }

        return false;
    }

}
