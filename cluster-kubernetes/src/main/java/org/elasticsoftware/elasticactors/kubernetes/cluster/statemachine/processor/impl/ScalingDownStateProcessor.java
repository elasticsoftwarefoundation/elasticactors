package org.elasticsoftware.elasticactors.kubernetes.cluster.statemachine.processor.impl;

import io.fabric8.kubernetes.api.model.apps.StatefulSet;
import org.elasticsoftware.elasticactors.kubernetes.cluster.TaskScheduler;
import org.elasticsoftware.elasticactors.kubernetes.cluster.statemachine.data.KubernetesClusterState;
import org.elasticsoftware.elasticactors.kubernetes.cluster.statemachine.data.KubernetesStateMachineData;
import org.elasticsoftware.elasticactors.kubernetes.cluster.statemachine.processor.AbstractTaskSchedulingStateProcessor;

import static java.lang.String.format;

public class ScalingDownStateProcessor extends AbstractTaskSchedulingStateProcessor {

    public ScalingDownStateProcessor(KubernetesStateMachineData kubernetesStateMachineData, TaskScheduler taskScheduler) {
        super(kubernetesStateMachineData, taskScheduler);
    }

    @Override
    public boolean process(StatefulSet resource) {

        int desiredReplicas = getDesiredReplicas(resource);
        int actualReplicas = getActualReplicas(resource);
        int readyReplicas = getReadyReplicas(resource);
        int currentDesiredReplicas = getDesiredReplicas(kubernetesStateMachineData.getLatestStableState().get());

        // spec and status should all be the same
        if (desiredReplicas == actualReplicas && desiredReplicas == readyReplicas) {
            logger.info(format("Successfully scaled down to %d nodes. Switching to STABLE", desiredReplicas));
            switchToStableState(resource);
        } else if (desiredReplicas > currentDesiredReplicas) {
            logger.info("Scaling down cancelled. Scale up detected. Switching to SCALING_UP");
            kubernetesStateMachineData.getCurrentState().set(KubernetesClusterState.SCALING_UP);
            return true;
        }

        return false;
    }

}
