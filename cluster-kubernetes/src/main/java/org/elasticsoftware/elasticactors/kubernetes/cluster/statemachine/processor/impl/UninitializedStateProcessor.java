package org.elasticsoftware.elasticactors.kubernetes.cluster.statemachine.processor.impl;

import io.fabric8.kubernetes.api.model.apps.StatefulSet;
import org.elasticsoftware.elasticactors.kubernetes.cluster.statemachine.KubernetesStateMachineData;
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
