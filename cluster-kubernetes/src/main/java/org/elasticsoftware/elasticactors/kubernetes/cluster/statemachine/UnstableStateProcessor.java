package org.elasticsoftware.elasticactors.kubernetes.cluster.statemachine;

import io.fabric8.kubernetes.api.model.apps.StatefulSet;

class UnstableStateProcessor extends StateProcessor {

    UnstableStateProcessor(ClusterStateData clusterStateData) {
        super(clusterStateData);
    }

    @Override
    boolean process(StatefulSet resource) {

        int desiredReplicas = resource.getSpec().getReplicas();
        int actualReplicas = resource.getStatus().getReplicas();
        int readyReplicas = getInt(resource.getStatus().getReadyReplicas());
        int currentDesiredReplicas = clusterStateData.getLatestStableState().get().getSpec().getReplicas();

        // we need to somehow get out of this UNSTABLE state
        // spec and status should all be the same
        if (desiredReplicas == currentDesiredReplicas
                && desiredReplicas == actualReplicas
                && desiredReplicas == readyReplicas) {
            logger.info("Switching back to STABLE state");
            switchToStableState(resource);
        }

        return false;
    }

}
