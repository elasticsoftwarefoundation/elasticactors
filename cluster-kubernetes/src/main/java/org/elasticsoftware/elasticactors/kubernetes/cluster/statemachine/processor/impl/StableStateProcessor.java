package org.elasticsoftware.elasticactors.kubernetes.cluster.statemachine.processor.impl;

import io.fabric8.kubernetes.api.model.apps.StatefulSet;
import org.elasticsoftware.elasticactors.kubernetes.cluster.statemachine.KubernetesClusterState;
import org.elasticsoftware.elasticactors.kubernetes.cluster.statemachine.KubernetesStateMachineData;
import org.elasticsoftware.elasticactors.kubernetes.cluster.statemachine.processor.AbstractStateProcessor;

import static java.lang.String.format;

public class StableStateProcessor extends AbstractStateProcessor {

    public StableStateProcessor(KubernetesStateMachineData kubernetesStateMachineData) {
        super(kubernetesStateMachineData);
    }

    @Override
    public boolean process(StatefulSet resource) {

        int desiredReplicas = resource.getSpec().getReplicas();
        int currentDesiredReplicas = kubernetesStateMachineData.getLatestStableState().get().getSpec().getReplicas();

        if (desiredReplicas == currentDesiredReplicas) {
            logger.info("Cluster topology remains unchanged");
        } else if (desiredReplicas < currentDesiredReplicas) {
            logger.info(format("Cluster topology changed from %d to %d replicas -> setting status to SCALING_DOWN", currentDesiredReplicas, desiredReplicas));
            // we are scaling down the cluster
            kubernetesStateMachineData.getCurrentState().set(KubernetesClusterState.SCALING_DOWN);
            return true;
        } else {
            logger.info(format("Cluster topology changed from %d to %d replicas -> setting status to SCALING_UP", currentDesiredReplicas, desiredReplicas));
            // we are scaling down the cluster
            kubernetesStateMachineData.getCurrentState().set(KubernetesClusterState.SCALING_UP);
            return true;
        }

        return false;
    }

}
