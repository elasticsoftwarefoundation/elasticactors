package org.elasticsoftware.elasticactors.kubernetes.cluster.statemachine.processor;

import io.fabric8.kubernetes.api.model.apps.StatefulSet;
import org.elasticsoftware.elasticactors.kubernetes.cluster.TaskScheduler;
import org.elasticsoftware.elasticactors.kubernetes.cluster.statemachine.data.KubernetesClusterState;
import org.elasticsoftware.elasticactors.kubernetes.cluster.statemachine.data.KubernetesStateMachineData;

import static java.lang.String.format;

public abstract class AbstractTaskSchedulingStateProcessor extends AbstractStateProcessor {

    private final static String TASK_NAME = "SCALE_UP_TIMEOUT_TASK";

    private final TaskScheduler taskScheduler;

    protected AbstractTaskSchedulingStateProcessor(KubernetesStateMachineData kubernetesStateMachineData, TaskScheduler taskScheduler) {
        super(kubernetesStateMachineData);
        this.taskScheduler = taskScheduler;
    }

    @Override
    protected void switchToStableState(StatefulSet resource) {
        cancelScheduledTimeoutTask();
        super.switchToStableState(resource);
    }

    protected void cancelScheduledTimeoutTask() {
        taskScheduler.cancelScheduledTask();
    }

    protected void scheduleTimeoutTask(int desiredReplicas, int baseReplicas) {
        taskScheduler.scheduleTask(() -> {
            final int stableReplicas = getDesiredReplicas(kubernetesStateMachineData.getLatestStableState().get());
            logger.error(format("Scaling up to %d nodes failed. Reverting to previous stable state with %d nodes -> setting status to UNSTABLE", desiredReplicas, stableReplicas));
            kubernetesStateMachineData.getCurrentState().set(KubernetesClusterState.UNSTABLE);
            // change the topology back to the old state
            onTopologyChange(stableReplicas);
        }, Math.max(desiredReplicas - baseReplicas, 1), TASK_NAME);
    }

}
