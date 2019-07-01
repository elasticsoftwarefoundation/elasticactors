package org.elasticsoftware.elasticactors.kubernetes.cluster.statemachine;

import io.fabric8.kubernetes.api.model.apps.StatefulSet;

public interface KubernetesStateMachine {

    void handleStateUpdate(StatefulSet resource);

    void addListener(KubernetesStateMachineListener listener);
}
