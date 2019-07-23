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

package org.elasticsoftware.elasticactors.kubernetes.cluster.statemachine.processor.impl;

import io.fabric8.kubernetes.api.model.apps.StatefulSet;
import org.elasticsoftware.elasticactors.kubernetes.cluster.TaskScheduler;
import org.elasticsoftware.elasticactors.kubernetes.cluster.statemachine.data.KubernetesClusterState;
import org.elasticsoftware.elasticactors.kubernetes.cluster.statemachine.data.KubernetesStateMachineData;
import org.elasticsoftware.elasticactors.kubernetes.cluster.statemachine.processor.AbstractTaskSchedulingStateProcessor;

import static java.lang.String.format;

public class ScalingUpStateProcessor extends AbstractTaskSchedulingStateProcessor {

    public ScalingUpStateProcessor(KubernetesStateMachineData kubernetesStateMachineData, TaskScheduler taskScheduler) {
        super(kubernetesStateMachineData, taskScheduler);
    }

    @Override
    public boolean process(StatefulSet resource) {

        int desiredReplicas = getDesiredReplicas(resource);
        int actualReplicas = getActualReplicas(resource);
        int readyReplicas = getReadyReplicas(resource);
        int currentDesiredReplicas = getDesiredReplicas(kubernetesStateMachineData.getLatestStableState().get());
        int currentActualReplicas = getActualReplicas(kubernetesStateMachineData.getLatestStableState().get());

        // with scaling up it can happen that there are no more resources and the cluster will scale down
        // again without anything actually happening so we need to check here with the current state again
        if (desiredReplicas == actualReplicas && desiredReplicas == readyReplicas) {
            logger.info("Scaling up cancelled. Switching to STABLE");
            switchToStableState(resource);
        }
        // it could also be smaller, in that case we are actually scaling down
        else if (desiredReplicas < currentDesiredReplicas && desiredReplicas < actualReplicas) {
            logger.info("Scaling up cancelled. Scale down detected. Switching to SCALING_DOWN");
            kubernetesStateMachineData.getCurrentState().set(KubernetesClusterState.SCALING_DOWN);
            return true;
        }
        // don't wait for the other node to be ready but shed partitions immediately
        else if (actualReplicas > currentDesiredReplicas || actualReplicas > currentActualReplicas) {
            logger.info(format("Starting scale up to %d nodes. Switching to SCALING_UP_STARTED", desiredReplicas));
            kubernetesStateMachineData.getCurrentState().set(KubernetesClusterState.SCALING_UP_STARTED);
            onTopologyChange(desiredReplicas);
            // we need to set a timeout to receive the ready message
            scheduleTimeoutTask(desiredReplicas, readyReplicas);
        }

        return false;
    }

}
