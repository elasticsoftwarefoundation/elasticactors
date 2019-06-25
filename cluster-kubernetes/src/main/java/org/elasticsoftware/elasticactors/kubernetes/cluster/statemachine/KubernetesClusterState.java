package org.elasticsoftware.elasticactors.kubernetes.cluster.statemachine;

public enum KubernetesClusterState {
    STABLE, SCALING_UP, SCALING_UP_STARTED, SCALING_DOWN, UNSTABLE
}
