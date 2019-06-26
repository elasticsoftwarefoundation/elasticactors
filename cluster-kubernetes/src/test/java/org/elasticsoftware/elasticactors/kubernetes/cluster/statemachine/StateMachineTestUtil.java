package org.elasticsoftware.elasticactors.kubernetes.cluster.statemachine;

import io.fabric8.kubernetes.api.model.apps.StatefulSet;
import io.fabric8.kubernetes.api.model.apps.StatefulSetSpec;
import io.fabric8.kubernetes.api.model.apps.StatefulSetStatus;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

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

    public static List<StatefulSet> scale(int from, int to) {
        if(to < from) {
            return scaleDown(from, to);
        }
        return scaleUp(from, to);
    }

    private static List<StatefulSet> scaleUp(int currentDesiredReplicas, int desiredReplicas) {
        List<StatefulSet> steps = new ArrayList<>();
        for (int i = currentDesiredReplicas; i <= desiredReplicas; i++) {
            for (int j = i; j <= Math.min(i + 1, desiredReplicas); j++) {
                steps.add(resourceWith(desiredReplicas, j, i));
            }
        }
        return steps;
    }

    private static List<StatefulSet> scaleDown(int currentDesiredReplicas, int desiredReplicas) {
        List<StatefulSet> steps = new ArrayList<>();
        for (int i = currentDesiredReplicas; i >= desiredReplicas; i--) {
            for (int j = i; j >= Math.max(i - 1, desiredReplicas); j--) {
                steps.add(resourceWith(desiredReplicas, i, j));
            }
        }
        return steps;
    }

}
