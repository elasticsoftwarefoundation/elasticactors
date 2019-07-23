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
import org.elasticsoftware.elasticactors.kubernetes.cluster.statemachine.data.KubernetesClusterState;
import org.elasticsoftware.elasticactors.kubernetes.cluster.statemachine.data.KubernetesStateMachineData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class AbstractStateProcessor implements StateProcessor {

    protected final Logger logger = LoggerFactory.getLogger(getClass());

    protected final KubernetesStateMachineData kubernetesStateMachineData;

    protected AbstractStateProcessor(KubernetesStateMachineData kubernetesStateMachineData) {
        this.kubernetesStateMachineData = kubernetesStateMachineData;
    }

    private static int getInt(Integer value) {
        return value != null ? value : 0;
    }

    protected void switchToStableState(StatefulSet resource) {
        kubernetesStateMachineData.getCurrentState().set(KubernetesClusterState.STABLE);
        kubernetesStateMachineData.getLatestStableState().set(resource);
        onTopologyChange(getDesiredReplicas(resource));
    }

    protected void onTopologyChange(int totalReplicas) {
        if (kubernetesStateMachineData.getCurrentTopology().getAndSet(totalReplicas) != totalReplicas) {
            kubernetesStateMachineData.getStateMachineListeners().forEach(l -> l.onTopologyChange(totalReplicas));
        }
    }

    protected int getReadyReplicas(StatefulSet resource) {
        return getInt(resource.getStatus().getReadyReplicas());
    }

    protected int getActualReplicas(StatefulSet resource) {
        return resource.getStatus().getReplicas();
    }

    protected int getDesiredReplicas(StatefulSet resource) {
        return resource.getSpec().getReplicas();
    }
}
