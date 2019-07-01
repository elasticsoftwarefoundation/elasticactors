package org.elasticsoftware.elasticactors.kubernetes.cluster.statemachine.processor.impl;

import io.fabric8.kubernetes.api.model.apps.StatefulSet;
import org.elasticsoftware.elasticactors.kubernetes.cluster.statemachine.data.KubernetesClusterState;
import org.elasticsoftware.elasticactors.kubernetes.cluster.statemachine.data.KubernetesStateMachineData;
import org.elasticsoftware.elasticactors.kubernetes.cluster.statemachine.processor.AbstractStateProcessor;

import static java.lang.String.format;

public class StableStateProcessor extends AbstractStateProcessor {

    public StableStateProcessor(KubernetesStateMachineData kubernetesStateMachineData) {
        super(kubernetesStateMachineData);
    }

    @Override
    public boolean process(StatefulSet resource) {

        int desiredReplicas = getDesiredReplicas(resource);
        int actualReplicas = getActualReplicas(resource);
        int readyReplicas = getReadyReplicas(resource);
        int currentDesiredReplicas = getDesiredReplicas(kubernetesStateMachineData.getLatestStableState().get());

        if (desiredReplicas == currentDesiredReplicas && desiredReplicas == actualReplicas) {
            logger.info("Cluster topology remains unchanged");
        } else if (desiredReplicas == actualReplicas && desiredReplicas == readyReplicas) {
            logger.info(format("Cluster topology changed from %d to %d replicas. We probably missed a few updates. Signalling topology change", currentDesiredReplicas, desiredReplicas));
            switchToStableState(resource);
        } else if (desiredReplicas < currentDesiredReplicas) {
            logger.info(format("Cluster topology changed from %d to %d replicas. Switching to SCALING_DOWN", currentDesiredReplicas, desiredReplicas));
            // we are scaling down the cluster
            kubernetesStateMachineData.getCurrentState().set(KubernetesClusterState.SCALING_DOWN);
            return true;
        } else if (desiredReplicas > currentDesiredReplicas || desiredReplicas > actualReplicas) {
            logger.info(format("Cluster topology changed from %d to %d replicas. Switching to SCALING_UP", currentDesiredReplicas, desiredReplicas));
            // we are scaling up the cluster
            kubernetesStateMachineData.getCurrentState().set(KubernetesClusterState.SCALING_UP);
            return true;
        }

        return false;
    }

}
