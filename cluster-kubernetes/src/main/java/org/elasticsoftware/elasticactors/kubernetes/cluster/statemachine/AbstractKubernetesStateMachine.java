package org.elasticsoftware.elasticactors.kubernetes.cluster.statemachine;

import io.fabric8.kubernetes.api.model.apps.StatefulSet;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsoftware.elasticactors.kubernetes.cluster.TaskScheduler;
import org.elasticsoftware.elasticactors.kubernetes.cluster.statemachine.data.KubernetesClusterState;
import org.elasticsoftware.elasticactors.kubernetes.cluster.statemachine.data.KubernetesStateMachineData;
import org.elasticsoftware.elasticactors.kubernetes.cluster.statemachine.processor.StateProcessor;
import org.elasticsoftware.elasticactors.kubernetes.cluster.statemachine.processor.impl.ScalingDownStateProcessor;
import org.elasticsoftware.elasticactors.kubernetes.cluster.statemachine.processor.impl.ScalingUpStartedStateProcessor;
import org.elasticsoftware.elasticactors.kubernetes.cluster.statemachine.processor.impl.ScalingUpStateProcessor;
import org.elasticsoftware.elasticactors.kubernetes.cluster.statemachine.processor.impl.StableStateProcessor;
import org.elasticsoftware.elasticactors.kubernetes.cluster.statemachine.processor.impl.UninitializedStateProcessor;
import org.elasticsoftware.elasticactors.kubernetes.cluster.statemachine.processor.impl.UnstableStateProcessor;

import java.util.EnumMap;
import java.util.Map;

import static java.lang.String.format;

public abstract class AbstractKubernetesStateMachine implements KubernetesStateMachine {

    protected final Logger logger = LogManager.getLogger(getClass());

    private final Map<KubernetesClusterState, StateProcessor> stateProcessorMap = new EnumMap<>(KubernetesClusterState.class);

    private final KubernetesStateMachineData kubernetesStateMachineData = new KubernetesStateMachineData();
    private final UninitializedStateProcessor uninitializedStateProcessor = new UninitializedStateProcessor(kubernetesStateMachineData);

    protected void processStateUpdate(StatefulSet resource) {
        logger.info(format("Received Cluster State Update: spec.replicas=%d, status.replicas=%d, status.readyReplicas=%d",
                resource.getSpec().getReplicas(), resource.getStatus().getReplicas(), resource.getStatus().getReadyReplicas()));
        boolean reprocess;
        do {
            KubernetesClusterState clusterState = kubernetesStateMachineData.getCurrentState().get();
            reprocess = stateProcessorMap.getOrDefault(clusterState, uninitializedStateProcessor).process(resource);
        } while (reprocess);
    }

    protected AbstractKubernetesStateMachine(TaskScheduler taskScheduler) {
        stateProcessorMap.put(KubernetesClusterState.SCALING_DOWN, new ScalingDownStateProcessor(kubernetesStateMachineData, taskScheduler));
        stateProcessorMap.put(KubernetesClusterState.SCALING_UP, new ScalingUpStateProcessor(kubernetesStateMachineData, taskScheduler));
        stateProcessorMap.put(KubernetesClusterState.SCALING_UP_STARTED, new ScalingUpStartedStateProcessor(kubernetesStateMachineData, taskScheduler));
        stateProcessorMap.put(KubernetesClusterState.UNSTABLE, new UnstableStateProcessor(kubernetesStateMachineData, taskScheduler));
        stateProcessorMap.put(KubernetesClusterState.STABLE, new StableStateProcessor(kubernetesStateMachineData));
    }

    @Override
    public void addListener(KubernetesStateMachineListener listener) {
        kubernetesStateMachineData.getStateMachineListeners().add(listener);
    }
}
