package org.elasticsoftware.elasticactors.kubernetes.cluster.statemachine;

import io.fabric8.kubernetes.api.model.apps.StatefulSet;

import static java.lang.String.format;

class StableStateProcessor extends StateProcessor {

    StableStateProcessor(ClusterStateData clusterStateData) {
        super(clusterStateData);
    }

    @Override
    boolean process(StatefulSet resource) {

        int desiredReplicas = resource.getSpec().getReplicas();
        int currentDesiredReplicas = clusterStateData.getLatestStableState().get().getSpec().getReplicas();

        if (desiredReplicas == currentDesiredReplicas) {
            logger.info("Cluster topology remains unchanged");
        } else if (desiredReplicas < currentDesiredReplicas) {
            logger.info(format("Cluster topology changed from %d to %d replicas -> setting status to SCALING_DOWN", currentDesiredReplicas, desiredReplicas));
            // we are scaling down the cluster
            clusterStateData.getCurrentState().set(ClusterState.SCALING_DOWN);
            return true;
        } else {
            logger.info(format("Cluster topology changed from %d to %d replicas -> setting status to SCALING_UP", currentDesiredReplicas, desiredReplicas));
            // we are scaling down the cluster
            clusterStateData.getCurrentState().set(ClusterState.SCALING_UP);
            return true;
        }

        return false;
    }

}
