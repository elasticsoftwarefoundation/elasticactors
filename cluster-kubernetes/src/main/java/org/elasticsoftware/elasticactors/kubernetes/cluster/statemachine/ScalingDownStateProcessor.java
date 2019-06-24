package org.elasticsoftware.elasticactors.kubernetes.cluster.statemachine;

import io.fabric8.kubernetes.api.model.apps.StatefulSet;

import static java.lang.String.format;

class ScalingDownStateProcessor extends StateProcessor {

    ScalingDownStateProcessor(ClusterStateData clusterStateData) {
        super(clusterStateData);
    }

    @Override
    boolean process(StatefulSet resource) {

        int desiredReplicas = resource.getSpec().getReplicas();
        int actualReplicas = resource.getStatus().getReplicas();
        int readyReplicas = getInt(resource.getStatus().getReadyReplicas());

        // spec and status should all be the same
        if (desiredReplicas == actualReplicas && desiredReplicas == readyReplicas) {
            logger.info(format("Successfully scaled down to %d nodes -> setting status to STABLE", desiredReplicas));
            switchToStableState(resource);
        }

        return false;
    }

}
