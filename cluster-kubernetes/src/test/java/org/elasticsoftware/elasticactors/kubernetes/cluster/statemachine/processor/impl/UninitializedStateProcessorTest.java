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

package org.elasticsoftware.elasticactors.kubernetes.cluster.statemachine.processor.impl;

import io.fabric8.kubernetes.api.model.apps.StatefulSet;
import org.elasticsoftware.elasticactors.kubernetes.cluster.statemachine.KubernetesStateMachineListener;
import org.elasticsoftware.elasticactors.kubernetes.cluster.statemachine.data.KubernetesStateMachineData;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.elasticsoftware.elasticactors.kubernetes.cluster.statemachine.StateMachineTestUtil.resourceWith;
import static org.elasticsoftware.elasticactors.kubernetes.cluster.statemachine.data.KubernetesClusterState.STABLE;
import static org.mockito.BDDMockito.then;
import static org.mockito.Mockito.mock;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

public class UninitializedStateProcessorTest {

    private UninitializedStateProcessor processor;
    private KubernetesStateMachineData data;
    private KubernetesStateMachineListener listener;

    @BeforeMethod
    public void setUp() {
        listener = mock(KubernetesStateMachineListener.class);
        data = new KubernetesStateMachineData();
        processor = new UninitializedStateProcessor(data);
        data.getStateMachineListeners().add(listener);
    }

    @Test
    public void testProcess() {
        StatefulSet newStableState = resourceWith(2, 2, 2);
        assertTrue(processor.process(newStableState));

        assertEquals(data.getCurrentState().get(), STABLE);
        assertEquals(data.getCurrentTopology().get(), 2);
        assertEquals(data.getLatestStableState().get(), newStableState);
        then(listener).should().onTopologyChange(2);
    }

}