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
import org.elasticsoftware.elasticactors.kubernetes.cluster.TaskScheduler;
import org.elasticsoftware.elasticactors.kubernetes.cluster.statemachine.KubernetesStateMachineListener;
import org.elasticsoftware.elasticactors.kubernetes.cluster.statemachine.data.KubernetesStateMachineData;
import org.mockito.Mockito;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import static org.elasticsoftware.elasticactors.kubernetes.cluster.statemachine.StateMachineTestUtil.initialize;
import static org.elasticsoftware.elasticactors.kubernetes.cluster.statemachine.StateMachineTestUtil.resourceWith;
import static org.elasticsoftware.elasticactors.kubernetes.cluster.statemachine.data.KubernetesClusterState.SCALING_DOWN;
import static org.elasticsoftware.elasticactors.kubernetes.cluster.statemachine.data.KubernetesClusterState.SCALING_UP;
import static org.elasticsoftware.elasticactors.kubernetes.cluster.statemachine.data.KubernetesClusterState.SCALING_UP_STARTED;
import static org.elasticsoftware.elasticactors.kubernetes.cluster.statemachine.data.KubernetesClusterState.STABLE;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.BDDMockito.then;
import static org.mockito.Mockito.never;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

public class ScalingUpStartedStateProcessorTest {

    private ScalingUpStartedStateProcessor processor;
    private KubernetesStateMachineData data;
    private TaskScheduler taskScheduler;
    private KubernetesStateMachineListener listener;
    private StatefulSet originalStableState;

    @BeforeMethod
    public void setUp() {
        taskScheduler = Mockito.mock(TaskScheduler.class);
        listener = Mockito.mock(KubernetesStateMachineListener.class);
        data = new KubernetesStateMachineData();
        processor = new ScalingUpStartedStateProcessor(data, taskScheduler);
        originalStableState = resourceWith(2, 2, 2);
        initialize(data, originalStableState, SCALING_UP_STARTED, listener);
        data.getCurrentTopology().set(4);
    }

    @Test
    public void testProcess_shouldSwitchToStable() {
        StatefulSet newStableState = resourceWith(4, 4, 4);

        assertFalse(processor.process(newStableState));

        assertEquals(data.getCurrentState().get(), STABLE);
        assertEquals(data.getCurrentTopology().get(), 4);
        assertEquals(data.getLatestStableState().get(), newStableState);
        then(listener).should(never()).onTopologyChange(anyInt());
        then(taskScheduler).should().cancelScheduledTask();
        then(taskScheduler).should(never()).scheduleTask(any(), any(), any());
    }

    @Test
    public void testProcess_shouldSwitchToStable_cancelledScaleUp() {
        StatefulSet newStableState = resourceWith(3, 3, 3);

        assertFalse(processor.process(newStableState));

        assertEquals(data.getCurrentState().get(), STABLE);
        assertEquals(data.getCurrentTopology().get(), 3);
        assertEquals(data.getLatestStableState().get(), newStableState);
        then(listener).should().onTopologyChange(3);
        then(taskScheduler).should().cancelScheduledTask();
        then(taskScheduler).should(never()).scheduleTask(any(), any(), any());
    }

    @Test
    public void testProcess_shouldSwitchToScaleUp() {
        assertTrue(processor.process(resourceWith(5, 4, 3)));

        assertEquals(data.getCurrentState().get(), SCALING_UP);
        assertEquals(data.getCurrentTopology().get(), 4);
        assertEquals(data.getLatestStableState().get(), originalStableState);
        then(listener).should(never()).onTopologyChange(anyInt());
        then(taskScheduler).should(never()).cancelScheduledTask();
        then(taskScheduler).should(never()).scheduleTask(any(), any(), any());
    }

    @DataProvider(name = "unstableStates")
    public Object[][] getUnstableStates() {
        return new Object[][]{
                {resourceWith(3, 2, 2)},
                {resourceWith(3, 3, 2)}
        };
    }

    @Test(dataProvider = "unstableStates")
    public void testProcess_shouldNotSwitchToStable_scaleUpFailed(StatefulSet resource) {
        assertFalse(processor.process(resource));

        assertEquals(data.getCurrentState().get(), SCALING_UP_STARTED);
        assertEquals(data.getCurrentTopology().get(), 4);
        assertEquals(data.getLatestStableState().get(), originalStableState);
        then(listener).should(never()).onTopologyChange(anyInt());
        then(taskScheduler).should(never()).cancelScheduledTask();
        then(taskScheduler).should(never()).scheduleTask(any(), any(), any());
    }

    @Test
    public void testProcess_shouldSwitchToScaleDown() {
        assertTrue(processor.process(resourceWith(1, 2, 2)));

        assertEquals(data.getCurrentState().get(), SCALING_DOWN);
        assertEquals(data.getCurrentTopology().get(), 4);
        assertEquals(data.getLatestStableState().get(), originalStableState);
        then(listener).should(never()).onTopologyChange(anyInt());
        then(taskScheduler).should().cancelScheduledTask();
        then(taskScheduler).should(never()).scheduleTask(any(), any(), any());
    }

}