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

        int desiredReplicas = resource.getSpec().getReplicas();
        int actualReplicas = resource.getStatus().getReplicas();
        int readyReplicas = getInt(resource.getStatus().getReadyReplicas());
        int currentDesiredReplicas = kubernetesStateMachineData.getLatestStableState().get().getSpec().getReplicas();

        if(desiredReplicas == actualReplicas && desiredReplicas == readyReplicas) {
            logger.info(format("Successfully scaled up to %d nodes -> setting status to STABLE", desiredReplicas));
            switchToStableState(resource);
        } else if (desiredReplicas < currentDesiredReplicas) {
            logger.info("Scaling up cancelled. Scale down detected. Switching to SCALING_DOWN status");
            kubernetesStateMachineData.getCurrentState().set(KubernetesClusterState.SCALING_DOWN);
        } else if(desiredReplicas > kubernetesStateMachineData.getCurrentTopology().get()) {
            logger.info(format("New scale up to %d nodes detected -> signalling new topology change", desiredReplicas));
            onTopologyChange(desiredReplicas);
            // we need to set a timeout to receive the ready message
            scheduleTimeoutTask(desiredReplicas, readyReplicas);
        }

        return false;
    }
}
