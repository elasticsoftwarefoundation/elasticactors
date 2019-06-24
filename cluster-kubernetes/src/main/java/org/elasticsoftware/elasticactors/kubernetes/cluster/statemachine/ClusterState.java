package org.elasticsoftware.elasticactors.kubernetes.cluster.statemachine;

enum ClusterState {
    STABLE, SCALING_UP, SCALING_UP_STARTED, SCALING_DOWN, UNSTABLE
}
