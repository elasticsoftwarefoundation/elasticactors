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

package org.elasticsoftware.elasticactors.kubernetes.cluster.statemachine.data;

import io.fabric8.kubernetes.api.model.apps.StatefulSet;
import org.elasticsoftware.elasticactors.kubernetes.cluster.statemachine.KubernetesStateMachineListener;

import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

public class KubernetesStateMachineData {

    private final AtomicReference<KubernetesClusterState> currentState;
    private final AtomicReference<StatefulSet> latestStableState;
    private final AtomicInteger currentTopology;
    private final Queue<KubernetesStateMachineListener> stateMachineListeners;

    public KubernetesStateMachineData() {
        this.currentState = new AtomicReference<>();
        this.latestStableState = new AtomicReference<>();
        this.currentTopology = new AtomicInteger();
        this.stateMachineListeners = new ConcurrentLinkedQueue<>();
    }

    public AtomicReference<KubernetesClusterState> getCurrentState() {
        return currentState;
    }

    public AtomicReference<StatefulSet> getLatestStableState() {
        return latestStableState;
    }

    public AtomicInteger getCurrentTopology() {
        return currentTopology;
    }

    public Queue<KubernetesStateMachineListener> getStateMachineListeners() {
        return stateMachineListeners;
    }

}
