package org.elasticsoftware.elasticactors.kubernetes.cluster.statemachine.processor;

import io.fabric8.kubernetes.api.model.apps.StatefulSet;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsoftware.elasticactors.kubernetes.cluster.statemachine.KubernetesClusterState;
import org.elasticsoftware.elasticactors.kubernetes.cluster.statemachine.KubernetesStateMachine;
import org.elasticsoftware.elasticactors.kubernetes.cluster.statemachine.KubernetesStateMachineData;

public abstract class AbstractStateProcessor implements StateProcessor {

    protected static final Logger logger = LogManager.getLogger(KubernetesStateMachine.class);

    protected final KubernetesStateMachineData kubernetesStateMachineData;

    public AbstractStateProcessor(KubernetesStateMachineData kubernetesStateMachineData) {
        this.kubernetesStateMachineData = kubernetesStateMachineData;
    }

    protected static int getInt(Integer value) {
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
}
