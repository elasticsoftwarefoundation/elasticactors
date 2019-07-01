package org.elasticsoftware.elasticactors.kubernetes.cluster.statemachine.processor;

import io.fabric8.kubernetes.api.model.apps.StatefulSet;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsoftware.elasticactors.kubernetes.cluster.statemachine.data.KubernetesClusterState;
import org.elasticsoftware.elasticactors.kubernetes.cluster.statemachine.data.KubernetesStateMachineData;

public abstract class AbstractStateProcessor implements StateProcessor {

    protected final Logger logger = LogManager.getLogger(getClass());

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
        onTopologyChange(resource.getSpec().getReplicas());
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
