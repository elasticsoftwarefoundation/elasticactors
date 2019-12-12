/*
 *   Copyright 2013 - 2019 The Original Authors
 *
 *   Licensed under the Apache License, Version 2.0 (the "License");
 *   you may not use this file except in compliance with the License.
 *   You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 */

package org.elasticsoftware.elasticactors.kubernetes.cluster.statemachine.processor;

import io.fabric8.kubernetes.api.model.apps.StatefulSet;
import org.elasticsoftware.elasticactors.kubernetes.cluster.TaskScheduler;
import org.elasticsoftware.elasticactors.kubernetes.cluster.statemachine.data.KubernetesClusterState;
import org.elasticsoftware.elasticactors.kubernetes.cluster.statemachine.data.KubernetesStateMachineData;

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
            StatefulSet latestState = kubernetesStateMachineData.getLatestState().get();
            final int currentReplicas = Math.max(getDesiredReplicas(latestState), getActualReplicas(latestState));
            logger.error(
                    "Scaling up to {} nodes failed. "
                            + "Setting topology to {} nodes -> setting status to UNSTABLE",
                    desiredReplicas,
                    currentReplicas);
            kubernetesStateMachineData.getCurrentState().set(KubernetesClusterState.UNSTABLE);
            // change the topology back to the old state
            onTopologyChange(currentReplicas);
        }, Math.max(desiredReplicas - baseReplicas, 1), TASK_NAME);
    }

}
