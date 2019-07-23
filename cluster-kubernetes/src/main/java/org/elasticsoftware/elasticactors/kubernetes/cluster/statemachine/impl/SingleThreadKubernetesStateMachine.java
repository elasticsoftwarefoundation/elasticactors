/*
 *   Copyright 2013 - 2019 The Original Authors
 *
 *   Licensed under the Apache License, Version 2.0 (the "License");
 *   you may not use this file except in compliance with the License.
 *   You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 */

package org.elasticsoftware.elasticactors.kubernetes.cluster.statemachine.impl;

import io.fabric8.kubernetes.api.model.apps.StatefulSet;
import org.elasticsoftware.elasticactors.kubernetes.cluster.DaemonThreadFactory;
import org.elasticsoftware.elasticactors.kubernetes.cluster.TaskScheduler;
import org.elasticsoftware.elasticactors.kubernetes.cluster.statemachine.AbstractKubernetesStateMachine;

import java.util.concurrent.ExecutorService;

import static java.util.concurrent.Executors.newSingleThreadExecutor;

public class SingleThreadKubernetesStateMachine extends AbstractKubernetesStateMachine {

    private final ExecutorService executorService = newSingleThreadExecutor(new DaemonThreadFactory("KUBERNETES_CLUSTERSERVICE_STATEMACHINE"));

    public SingleThreadKubernetesStateMachine(TaskScheduler taskScheduler) {
        super(taskScheduler);
    }

    @Override
    public void handleStateUpdate(StatefulSet resource) {
        executorService.submit(() -> processStateUpdate(resource));
    }
}
