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
import org.elasticsoftware.elasticactors.kubernetes.cluster.statemachine.data.KubernetesStateMachineData;
import org.elasticsoftware.elasticactors.kubernetes.cluster.statemachine.processor.AbstractStateProcessor;

import static java.lang.String.format;

public class UninitializedStateProcessor extends AbstractStateProcessor {

    public UninitializedStateProcessor(KubernetesStateMachineData kubernetesStateMachineData) {
        super(kubernetesStateMachineData);
    }

    @Override
    public boolean process(StatefulSet resource) {
        logger.info(format("Initialized Cluster State: spec.replicas=%d, status.replicas=%d, status.readyReplicas=%d",
                getDesiredReplicas(resource), getActualReplicas(resource), getReadyReplicas(resource)));
        switchToStableState(resource);
        return true;
    }

}
