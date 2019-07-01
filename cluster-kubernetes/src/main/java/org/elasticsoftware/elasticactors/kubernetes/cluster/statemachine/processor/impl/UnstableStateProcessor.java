package org.elasticsoftware.elasticactors.kubernetes.cluster.statemachine.processor.impl;

import io.fabric8.kubernetes.api.model.apps.StatefulSet;
import org.elasticsoftware.elasticactors.kubernetes.cluster.TaskScheduler;
import org.elasticsoftware.elasticactors.kubernetes.cluster.statemachine.data.KubernetesStateMachineData;
import org.elasticsoftware.elasticactors.kubernetes.cluster.statemachine.processor.AbstractTaskSchedulingStateProcessor;

import static java.lang.String.format;

public class UnstableStateProcessor extends AbstractTaskSchedulingStateProcessor {

    public UnstableStateProcessor(KubernetesStateMachineData kubernetesStateMachineData, TaskScheduler taskScheduler) {
        super(kubernetesStateMachineData, taskScheduler);
    }

    @Override
    public boolean process(StatefulSet resource) {

        int desiredReplicas = getDesiredReplicas(resource);
        int actualReplicas = getActualReplicas(resource);
        int readyReplicas = getReadyReplicas(resource);

        // we need to somehow get out of this UNSTABLE state
        // spec and status should all be the same
        if (desiredReplicas == actualReplicas && desiredReplicas == readyReplicas) {
            logger.info(format("Switching back to STABLE state with %d replicas", desiredReplicas));
            switchToStableState(resource);
        }

        return false;
    }

}
