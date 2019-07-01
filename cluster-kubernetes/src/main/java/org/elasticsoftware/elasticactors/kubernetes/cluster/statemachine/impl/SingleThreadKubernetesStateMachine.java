package org.elasticsoftware.elasticactors.kubernetes.cluster.statemachine.impl;

import io.fabric8.kubernetes.api.model.apps.StatefulSet;
import org.elasticsoftware.elasticactors.kubernetes.cluster.TaskScheduler;
import org.elasticsoftware.elasticactors.kubernetes.cluster.statemachine.AbstractKubernetesStateMachine;

import java.util.concurrent.ExecutorService;

import static java.util.concurrent.Executors.newSingleThreadExecutor;

public class SingleThreadKubernetesStateMachine extends AbstractKubernetesStateMachine {

    private final ExecutorService executorService = newSingleThreadExecutor();

    public SingleThreadKubernetesStateMachine(TaskScheduler taskScheduler) {
        super(taskScheduler);
    }

    @Override
    public void handleStateUpdate(StatefulSet resource) {
        logger.info("Received state update. Submitting processing task to the Single Thread Executor Service");
        executorService.submit(() -> processStateUpdate(resource));
    }
}
