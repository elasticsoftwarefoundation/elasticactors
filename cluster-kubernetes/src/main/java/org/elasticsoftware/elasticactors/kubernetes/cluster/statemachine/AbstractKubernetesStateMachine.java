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

package org.elasticsoftware.elasticactors.kubernetes.cluster.statemachine;

import io.fabric8.kubernetes.api.model.apps.StatefulSet;
import org.elasticsoftware.elasticactors.kubernetes.cluster.TaskScheduler;
import org.elasticsoftware.elasticactors.kubernetes.cluster.statemachine.data.KubernetesClusterState;
import org.elasticsoftware.elasticactors.kubernetes.cluster.statemachine.data.KubernetesStateMachineData;
import org.elasticsoftware.elasticactors.kubernetes.cluster.statemachine.processor.StateProcessor;
import org.elasticsoftware.elasticactors.kubernetes.cluster.statemachine.processor.impl.ScalingDownStateProcessor;
import org.elasticsoftware.elasticactors.kubernetes.cluster.statemachine.processor.impl.ScalingUpStartedStateProcessor;
import org.elasticsoftware.elasticactors.kubernetes.cluster.statemachine.processor.impl.ScalingUpStateProcessor;
import org.elasticsoftware.elasticactors.kubernetes.cluster.statemachine.processor.impl.StableStateProcessor;
import org.elasticsoftware.elasticactors.kubernetes.cluster.statemachine.processor.impl.UninitializedStateProcessor;
import org.elasticsoftware.elasticactors.kubernetes.cluster.statemachine.processor.impl.UnstableStateProcessor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.EnumMap;
import java.util.Map;

public abstract class AbstractKubernetesStateMachine implements KubernetesStateMachine {

    protected final Logger logger = LoggerFactory.getLogger(getClass());

    private final Map<KubernetesClusterState, StateProcessor> stateProcessorMap = new EnumMap<>(KubernetesClusterState.class);

    private final KubernetesStateMachineData kubernetesStateMachineData = new KubernetesStateMachineData();
    private final UninitializedStateProcessor uninitializedStateProcessor = new UninitializedStateProcessor(kubernetesStateMachineData);

    protected void processStateUpdate(StatefulSet resource) {
        logger.info("Received Cluster State Update: spec.replicas={}, status.replicas={}, status.readyReplicas={}",
                resource.getSpec().getReplicas(), resource.getStatus().getReplicas(), resource.getStatus().getReadyReplicas());
        boolean reprocess;
        do {
            KubernetesClusterState clusterState = kubernetesStateMachineData.getCurrentState().get();
            reprocess = stateProcessorMap.getOrDefault(clusterState, uninitializedStateProcessor).process(resource);
        } while (reprocess);
    }

    protected AbstractKubernetesStateMachine(TaskScheduler taskScheduler) {
        stateProcessorMap.put(KubernetesClusterState.SCALING_DOWN, new ScalingDownStateProcessor(kubernetesStateMachineData, taskScheduler));
        stateProcessorMap.put(KubernetesClusterState.SCALING_UP, new ScalingUpStateProcessor(kubernetesStateMachineData, taskScheduler));
        stateProcessorMap.put(KubernetesClusterState.SCALING_UP_STARTED, new ScalingUpStartedStateProcessor(kubernetesStateMachineData, taskScheduler));
        stateProcessorMap.put(KubernetesClusterState.UNSTABLE, new UnstableStateProcessor(kubernetesStateMachineData, taskScheduler));
        stateProcessorMap.put(KubernetesClusterState.STABLE, new StableStateProcessor(kubernetesStateMachineData));
    }

    @Override
    public void addListener(KubernetesStateMachineListener listener) {
        kubernetesStateMachineData.getStateMachineListeners().add(listener);
    }
}
