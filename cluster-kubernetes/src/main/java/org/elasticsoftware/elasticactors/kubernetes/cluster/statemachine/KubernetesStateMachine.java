package org.elasticsoftware.elasticactors.kubernetes.cluster.statemachine;

import io.fabric8.kubernetes.api.model.apps.StatefulSet;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsoftware.elasticactors.kubernetes.cluster.TaskScheduler;
import org.elasticsoftware.elasticactors.kubernetes.cluster.statemachine.processor.StateProcessor;
import org.elasticsoftware.elasticactors.kubernetes.cluster.statemachine.processor.impl.ScalingDownStateProcessor;
import org.elasticsoftware.elasticactors.kubernetes.cluster.statemachine.processor.impl.ScalingUpStartedStateProcessor;
import org.elasticsoftware.elasticactors.kubernetes.cluster.statemachine.processor.impl.ScalingUpStateProcessor;
import org.elasticsoftware.elasticactors.kubernetes.cluster.statemachine.processor.impl.StableStateProcessor;
import org.elasticsoftware.elasticactors.kubernetes.cluster.statemachine.processor.impl.UninitializedStateProcessor;
import org.elasticsoftware.elasticactors.kubernetes.cluster.statemachine.processor.impl.UnstableStateProcessor;

import java.util.EnumMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

import static java.lang.String.format;

public class KubernetesStateMachine {

    private static final Logger logger = LogManager.getLogger(KubernetesStateMachine.class);

    private final Map<KubernetesClusterState, StateProcessor> stateProcessorMap = new EnumMap<>(KubernetesClusterState.class);
    private final AtomicBoolean initialized = new AtomicBoolean();

    private final KubernetesStateMachineData kubernetesStateMachineData = new KubernetesStateMachineData();

    public void processStateUpdate(StatefulSet resource) {
        logger.info(format("Received Cluster State Update: spec.replicas=%d, status.replicas=%d, status.readyReplicas=%d",
                resource.getSpec().getReplicas(), resource.getStatus().getReplicas(), resource.getStatus().getReadyReplicas()));
        if(!initialized.getAndSet(true)) {
            new UninitializedStateProcessor(kubernetesStateMachineData).process(resource);
        } else {
            boolean reprocess;
            do {
                reprocess = stateProcessorMap.get(kubernetesStateMachineData.getCurrentState().get()).process(resource);
            } while (reprocess);
        }
    }

    public KubernetesStateMachine(TaskScheduler taskScheduler) {
        stateProcessorMap.put(KubernetesClusterState.SCALING_DOWN, new ScalingDownStateProcessor(kubernetesStateMachineData, taskScheduler));
        stateProcessorMap.put(KubernetesClusterState.SCALING_UP, new ScalingUpStateProcessor(kubernetesStateMachineData, taskScheduler));
        stateProcessorMap.put(KubernetesClusterState.SCALING_UP_STARTED, new ScalingUpStartedStateProcessor(kubernetesStateMachineData, taskScheduler));
        stateProcessorMap.put(KubernetesClusterState.UNSTABLE, new UnstableStateProcessor(kubernetesStateMachineData, taskScheduler));
        stateProcessorMap.put(KubernetesClusterState.STABLE, new StableStateProcessor(kubernetesStateMachineData));
    }

    public void addListener(KubernetesStateMachineListener listener) {
        kubernetesStateMachineData.getStateMachineListeners().add(listener);
    }
}
