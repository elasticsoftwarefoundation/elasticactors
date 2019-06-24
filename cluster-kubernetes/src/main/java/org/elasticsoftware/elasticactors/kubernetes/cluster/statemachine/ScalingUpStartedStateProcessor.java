package org.elasticsoftware.elasticactors.kubernetes.cluster.statemachine;

import io.fabric8.kubernetes.api.model.apps.StatefulSet;

import static java.lang.String.format;

class ScalingUpStartedStateProcessor extends StateProcessor {

    ScalingUpStartedStateProcessor(ClusterStateData clusterStateData) {
        super(clusterStateData);
    }

    @Override
    boolean process(StatefulSet resource) {

        int desiredReplicas = resource.getSpec().getReplicas();
        int actualReplicas = resource.getStatus().getReplicas();
        int readyReplicas = getInt(resource.getStatus().getReadyReplicas());
        int currentDesiredReplicas = clusterStateData.getLatestStableState().get().getSpec().getReplicas();

        if(desiredReplicas == actualReplicas && desiredReplicas == readyReplicas) {
            logger.info(format("Successfully scaled up to %d nodes -> setting status to STABLE", desiredReplicas));
            switchToStableState(resource);
        } else if (desiredReplicas < currentDesiredReplicas) {
            logger.info("Scaling up cancelled. Scale down detected. Switching to SCALING_DOWN status");
            clusterStateData.getCurrentState().set(ClusterState.SCALING_DOWN);
        } else if(desiredReplicas > clusterStateData.getCurrentTopology().get()) {
            logger.info(format("New scale up to %d nodes detected -> signalling new topology change", desiredReplicas));
            clusterStateData.getOnTopologyChange().accept(desiredReplicas);
            // we need to set a timeout to receive the ready message
            scheduleTimeoutTask(desiredReplicas, readyReplicas);
        }

        return false;
    }
}
