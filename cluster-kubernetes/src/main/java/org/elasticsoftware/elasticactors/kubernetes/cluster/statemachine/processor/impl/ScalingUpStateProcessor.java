package org.elasticsoftware.elasticactors.kubernetes.cluster.statemachine.processor.impl;

import io.fabric8.kubernetes.api.model.apps.StatefulSet;
import org.elasticsoftware.elasticactors.kubernetes.cluster.TaskScheduler;
import org.elasticsoftware.elasticactors.kubernetes.cluster.statemachine.KubernetesClusterState;
import org.elasticsoftware.elasticactors.kubernetes.cluster.statemachine.KubernetesStateMachineData;
import org.elasticsoftware.elasticactors.kubernetes.cluster.statemachine.processor.AbstractTaskSchedulingStateProcessor;

import static java.lang.String.format;

public class ScalingUpStateProcessor extends AbstractTaskSchedulingStateProcessor {

    public ScalingUpStateProcessor(KubernetesStateMachineData kubernetesStateMachineData, TaskScheduler taskScheduler) {
        super(kubernetesStateMachineData, taskScheduler);
    }

    @Override
    public boolean process(StatefulSet resource) {

        int desiredReplicas = resource.getSpec().getReplicas();
        int actualReplicas = resource.getStatus().getReplicas();
        int currentDesiredReplicas = kubernetesStateMachineData.getLatestStableState().get().getSpec().getReplicas();

        // with scaling up it can happen that there are no more resources and the cluster will scale down
        // again without anything actually happening so we need to check here with the current state again
        if (desiredReplicas == currentDesiredReplicas) {
            logger.info("Scaling up cancelled. Reverting back to STABLE status");
            switchToStableState(resource);
        }
        // it could also be smaller, in that case we are actually scaling down
        else if (desiredReplicas < currentDesiredReplicas) {
            logger.info("Scaling up cancelled. Scale down detected. Switching to SCALING_DOWN status");
            kubernetesStateMachineData.getCurrentState().set(KubernetesClusterState.SCALING_DOWN);
        }
        // don't wait for the other node to be ready but shed partitions immediately
        else if (actualReplicas > kubernetesStateMachineData.getCurrentTopology().get()) {
            logger.info(format("Starting scale up to %d nodes -> setting status to SCALING_UP_STARTED", desiredReplicas));
            kubernetesStateMachineData.getCurrentState().set(KubernetesClusterState.SCALING_UP_STARTED);
            onTopologyChange(desiredReplicas);
            // we need to set a timeout to receive the ready message
            scheduleTimeoutTask(desiredReplicas, currentDesiredReplicas);
        }

        return false;
    }

}
