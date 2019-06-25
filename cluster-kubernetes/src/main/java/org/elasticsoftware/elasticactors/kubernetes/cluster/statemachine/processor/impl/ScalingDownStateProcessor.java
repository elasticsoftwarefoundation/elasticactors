package org.elasticsoftware.elasticactors.kubernetes.cluster.statemachine.processor.impl;

import io.fabric8.kubernetes.api.model.apps.StatefulSet;
import org.elasticsoftware.elasticactors.kubernetes.cluster.TaskScheduler;
import org.elasticsoftware.elasticactors.kubernetes.cluster.statemachine.KubernetesStateMachineData;
import org.elasticsoftware.elasticactors.kubernetes.cluster.statemachine.processor.AbstractTaskSchedulingStateProcessor;

import static java.lang.String.format;

public class ScalingDownStateProcessor extends AbstractTaskSchedulingStateProcessor {

    public ScalingDownStateProcessor(KubernetesStateMachineData kubernetesStateMachineData, TaskScheduler taskScheduler) {
        super(kubernetesStateMachineData, taskScheduler);
    }

    @Override
    public boolean process(StatefulSet resource) {

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
