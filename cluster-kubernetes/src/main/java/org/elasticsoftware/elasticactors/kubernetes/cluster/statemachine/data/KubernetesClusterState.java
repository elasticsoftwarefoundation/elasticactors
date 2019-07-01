package org.elasticsoftware.elasticactors.kubernetes.cluster.statemachine.data;

public enum KubernetesClusterState {
    STABLE, SCALING_UP, SCALING_UP_STARTED, SCALING_DOWN, UNSTABLE
}
