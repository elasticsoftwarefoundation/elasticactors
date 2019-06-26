package org.elasticsoftware.elasticactors.kubernetes.cluster.statemachine.processor.impl;

import io.fabric8.kubernetes.api.model.apps.StatefulSet;
import org.elasticsoftware.elasticactors.kubernetes.cluster.TaskScheduler;
import org.elasticsoftware.elasticactors.kubernetes.cluster.statemachine.KubernetesClusterState;
import org.elasticsoftware.elasticactors.kubernetes.cluster.statemachine.KubernetesStateMachineData;
import org.elasticsoftware.elasticactors.kubernetes.cluster.statemachine.processor.AbstractTaskSchedulingStateProcessor;

import static java.lang.String.format;

public class ScalingUpStartedStateProcessor extends AbstractTaskSchedulingStateProcessor {

    public ScalingUpStartedStateProcessor(KubernetesStateMachineData kubernetesStateMachineData, TaskScheduler taskScheduler) {
        super(kubernetesStateMachineData, taskScheduler);
    }

    @Override
    public boolean process(StatefulSet resource) {

        int desiredReplicas = getDesiredReplicas(resource);
        int actualReplicas = getActualReplicas(resource);
        int readyReplicas = getReadyReplicas(resource);
        int currentDesiredReplicas = getDesiredReplicas(kubernetesStateMachineData.getLatestStableState().get());

        if(desiredReplicas == actualReplicas && desiredReplicas == readyReplicas) {
            logger.info(format("Successfully scaled up to %d nodes -> setting status to STABLE", desiredReplicas));
            switchToStableState(resource);
        } else if (desiredReplicas < currentDesiredReplicas) {
            logger.info("Scaling up cancelled. Scale down detected. Switching to SCALING_DOWN status");
            kubernetesStateMachineData.getCurrentState().set(KubernetesClusterState.SCALING_DOWN);
            cancelScheduledTimeoutTask();
            return true;
        } else if(desiredReplicas > kubernetesStateMachineData.getCurrentTopology().get()) {
            logger.info(format("New scale up to %d nodes detected. Switching to SCALING_UP status", desiredReplicas));
            kubernetesStateMachineData.getCurrentState().set(KubernetesClusterState.SCALING_UP);
            return true;
        }

        return false;
    }
}
