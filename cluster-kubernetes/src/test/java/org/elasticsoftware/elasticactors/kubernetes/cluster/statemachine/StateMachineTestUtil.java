package org.elasticsoftware.elasticactors.kubernetes.cluster.statemachine;

import io.fabric8.kubernetes.api.model.apps.StatefulSet;
import io.fabric8.kubernetes.api.model.apps.StatefulSetSpec;
import io.fabric8.kubernetes.api.model.apps.StatefulSetStatus;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import static java.lang.String.format;

public final class StateMachineTestUtil {

    private StateMachineTestUtil() {
    }

    public static StatefulSet resourceWith(Integer desiredReplicas, Integer actualReplicas, Integer readyReplicas) {
        StatefulSet statefulSet = mock(StatefulSet.class);
        StatefulSetSpec spec = mock(StatefulSetSpec.class);
        StatefulSetStatus status = mock(StatefulSetStatus.class);
        when(statefulSet.getSpec()).thenReturn(spec);
        when(statefulSet.getStatus()).thenReturn(status);
        when(spec.getReplicas()).thenReturn(desiredReplicas);
        when(status.getReplicas()).thenReturn(actualReplicas);
        when(status.getReadyReplicas()).thenReturn(readyReplicas);
        when(statefulSet.toString()).thenReturn(
                format("StatefulSet(spec.replicas=%d, status.replicas=%d, status.readyReplicas=%d)",
                        desiredReplicas, actualReplicas, readyReplicas));
        return statefulSet;
    }

    public static void initialize(KubernetesStateMachineData data, StatefulSet resource, KubernetesStateMachineListener... listener) {
        initialize(data, resource, null, listener);
    }

    public static void initialize(KubernetesStateMachineData data, StatefulSet resource, KubernetesClusterState state, KubernetesStateMachineListener... listener) {
        data.getCurrentState().set(state);
        data.getCurrentTopology().set(resource.getSpec().getReplicas());
        data.getLatestStableState().set(resource);
        if (listener != null) {
            for (KubernetesStateMachineListener l : listener) {
                data.getStateMachineListeners().add(l);
            }
        }
    }

}
