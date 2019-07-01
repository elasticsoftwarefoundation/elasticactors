package org.elasticsoftware.elasticactors.kubernetes.cluster.statemachine;

public interface KubernetesStateMachineListener {

    void onTopologyChange(int totalReplicas);

}
