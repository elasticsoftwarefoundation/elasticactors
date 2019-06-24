package org.elasticsoftware.elasticactors.kubernetes.cluster.statemachine;

import io.fabric8.kubernetes.api.model.apps.StatefulSet;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsoftware.elasticactors.kubernetes.cluster.DaemonThreadFactory;

import java.util.EnumMap;
import java.util.Map;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

import static java.lang.String.format;
import static java.util.concurrent.Executors.newSingleThreadScheduledExecutor;

public class KubernetesStateMachine {

    private static final Logger logger = LogManager.getLogger(KubernetesStateMachine.class);

    private final Map<ClusterState, StateProcessor> stateProcessorMap = new EnumMap<>(ClusterState.class);

    private final ClusterStateData clusterStateData;

    private final AtomicReference<ScheduledFuture> scheduledScaleUpTimeout = new AtomicReference<>();
    private final AtomicBoolean initialized = new AtomicBoolean();

    private final ScheduledExecutorService scheduledExecutorService =
            newSingleThreadScheduledExecutor(new DaemonThreadFactory("KUBERNETES_CLUSTERSERVICE_SCHEDULER"));

    public void processStateUpdate(StatefulSet resource){
        logger.info(format("Received Cluster State Update: spec.replicas=%d, status.replicas=%d, status.readyReplicas=%d",
                resource.getSpec().getReplicas(), resource.getStatus().getReplicas(), resource.getStatus().getReadyReplicas()));
        if(!initialized.getAndSet(true)) {
            clusterStateData.getCurrentState().set(ClusterState.STABLE);
            clusterStateData.getLatestStableState().set(resource);
        } else {
            boolean reprocess;
            do {
                reprocess = stateProcessorMap.get(clusterStateData.getCurrentState().get()).process(resource);
            } while (reprocess);
        }
    }

    public KubernetesStateMachine(Integer timeoutSeconds, Consumer<Integer> topologyChangeConsumer) {
        clusterStateData = new ClusterStateData(getOnTopologyChange(topologyChangeConsumer), getScaleUpTaskScheduler(timeoutSeconds));
        stateProcessorMap.put(ClusterState.SCALING_DOWN, new ScalingDownStateProcessor(clusterStateData));
        stateProcessorMap.put(ClusterState.SCALING_UP, new ScalingUpStateProcessor(clusterStateData));
        stateProcessorMap.put(ClusterState.SCALING_UP_STARTED, new ScalingUpStartedStateProcessor(clusterStateData));
        stateProcessorMap.put(ClusterState.STABLE, new StableStateProcessor(clusterStateData));
        stateProcessorMap.put(ClusterState.UNSTABLE, new UnstableStateProcessor(clusterStateData));
    }

    private Consumer<Integer> getOnTopologyChange(Consumer<Integer> topologyChangeConsumer) {
        return replicas -> {
            if (clusterStateData.getCurrentTopology().getAndSet(replicas) != replicas) {
                topologyChangeConsumer.accept(replicas);
            }
        };
    }

    private BiConsumer<Runnable, Integer> getScaleUpTaskScheduler(Integer timeoutSeconds) {
        return (runnable, multiplier) -> {
            ScheduledFuture<?> previous;
            if (runnable == null) {
                previous = scheduledScaleUpTimeout.getAndSet(null);
            } else {
                int delay = multiplier * timeoutSeconds;
                logger.info(format("Scheduling new scale up timeout task with a delay of %d seconds", delay));
                previous = scheduledScaleUpTimeout.getAndSet(scheduledExecutorService.schedule(runnable, delay, TimeUnit.SECONDS));
            }
            if(previous != null) {
                logger.info("Cancelling previously scheduled scale up timeout task");
                previous.cancel(false);
            }
        };
    }
}
